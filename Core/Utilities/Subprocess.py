# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Subprocess.py,v 1.5 2007/11/16 16:14:17 acasajus Exp $
__RCSID__ = "$Id: Subprocess.py,v 1.5 2007/11/16 16:14:17 acasajus Exp $"
"""
   DIRAC Wrapper to execute python and system commands with a wrapper, that might
   set a timeout.
   3 FUNCTIONS are provided:
     - shellCall( iTimeOut, cmdSeq, callbackFunction = None, env = None ):
       it uses subprocess.Popen class with "shell = True".
       If cmdSeq is a string, it specifies the command string to execute through
       the shell.  If cmdSeq is a sequence, the first item specifies the command
       string, and any additional items will be treated as additional shell arguments.

     - systemCall( iTimeOut, cmdSeq, callbackFunction = None, env = None ):
       it uses subprocess.Popen class with "shell = False".
       cmdSeq should be a string, or a sequence of program arguments.

       stderr and stdout are piped. callbackFunction( pipeId, line ) can be
       defined to process the stdout (pipeId = 0) and stderr (pipeId = 1) as
       they are produced

       They return a DIRAC.ReturnValue dictionary with a tuple in Value
       ( returncode, stdout, stderr ) the tuple will also be available upon
       timeout error or buffer overflow error.

     - pythonCall( iTimeOut, function, *stArgs, **stKeyArgs )
       calls function with given arguments within a timeout Wrapper
       should be used to wrap third party python functions
"""

# Very Important:
#  Here we can not import directly from DIRAC, since this file it is imported
#  at initialization time therefore the full path is necesary
# from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
# from DIRAC import gLogger
from DIRAC.LoggingSystem.Client.Logger import gLogger

import time
import select
import os
import subprocess

gLogger = gLogger.getSubLogger( 'Subprocess' )

class Subprocess:

  def __init__( self, timeout = False, bufferLimit = 5242880 ):
    try:
      self.changeTimeout( timeout )
      self.bufferLimit = int( bufferLimit) # 5MB limit for data
    except Exception, v:
      gLogger.exception( 'Failed initialisation of Subprocess object' )
      raise v

  def changeTimeout( self, timeout ):
    self.timeout = int( timeout )
    if self.timeout == 0:
      self.timeout = False
    gLogger.debug( 'Timeout set to', timeout )

  def __readFromFD( self, fd, baseLength = 0 ):
    dataString = ''
    redBuf = " "

    while len( redBuf ) > 0:
      redBuf = os.read( fd, 8192 )
      lastSliceLength = len( redBuf )
      dataString += redBuf
      if len( dataString ) + baseLength > self.bufferLimit:
        gLogger.error( 'Maximum output buffer length reached' )
        retDict = S_ERROR( 'Reached maximum allowed length (%d bytes) '
                           'for called function return value' % self.bufferLimit )
        retDict[ 'Value' ] = dataString
        return retDict

    return S_OK( dataString )

  def __executePythonFunction( self, function, writePipe, *stArgs, **stKeyArgs ):
    try:
      os.write( writePipe, "%s\n" % str( S_OK( function( *stArgs, **stKeyArgs ) ) ) )
    except OSError, v:
      if str(v) == '[Errno 32] Broken pipe':
        # the parent has died
        pass
    except Exception, v:
      gLogger.exception( 'Exception while executing', function.__name__ )
      os.write( writePipe, "%s\n" % str( S_ERROR( str( v ) ) ) )
    try:
      os.close( writePipe )
    finally:
      os._exit(0)

  def __selectFD( self, readSeq, timeout = False ):
    if self.timeout and not timeout:
      timeout = self.timeout
    if not timeout:
      return select.select( readSeq , [], [] )[0]
    else:
      return select.select( readSeq , [], [], timeout )[0]

  def __killPid( self, pid, signal = 9 ):
    try:
      os.kill( pid, signal )
    except Exception, v:
      if not str(v) == '[Errno 3] No such process':
        gLogger.exeption( 'Exception while killing timed out process' )
        raise v

  def __killChild( self ):
    self.__killPid( self.child.pid )

    #HACK to avoid python bug
    # self.child.wait()
    exitStatus = self.child.poll()
    while exitStatus == None:
      time.sleep( 0.000001 )
      exitStatus = self.child.poll()
    return exitStatus

  def pythonCall( self, function, *stArgs, **stKeyArgs ):
    readFD, writeFD = os.pipe()
    pid = os.fork()
    if pid == 0:
      os.close( readFD )
      self.__executePythonFunction( function, writeFD, *stArgs, **stKeyArgs )
      # FIXME: the close it is done at __executePythonFunction, do we need it here?
      os.close( writeFD )
    else:
      os.close( writeFD )
      readSeq = self.__selectFD( [ readFD ] )
      if len( readSeq ) == 0:
        gLogger.debug( 'Timeout limit reached for pythonCall', function.__name__)
        self.__killPid( pid )

        #HACK to avoid python bug
        # self.wait()
        while os.waitpid( pid, 0 ) == -1:
          time.sleep( 0.000001 )

        os.close( readFD )
        return S_ERROR( '%d seconds timeout for "%s" call' % ( self.timeout, function.__name__ ) )
      elif readSeq[0] == readFD:
        retDict = self.__readFromFD( readFD )
        os.close( readFD )
        os.waitpid( pid, 0 )
        if retDict[ 'OK' ]:
          return eval( retDict[ 'Value' ] )
        return retDict

  def __generateSystemCommandError( self, exitStatus, message ):
    retDict = S_ERROR( message )
    retDict[ 'Value' ] = ( exitStatus,
                           self.bufferList[0][0],
                           self.bufferList[1][0] )
    return retDict

  def __readFromFile( self, file, baseLength, doAll ):
    try:
      if doAll:
        dataString = "".join( file.readlines() )
      else:
        dataString = file.readline()
    except Exception, v:
      pass
    if len( dataString ) + baseLength > self.bufferLimit:
      gLogger.error( 'Maximum output buffer length reached' )
      retDict = S_ERROR( 'Reached maximum allowed length (%d bytes) for called '
                         'function return value' % self.bufferLimit )
      retDict[ 'Value' ] = dataString
      return retDict

    return S_OK( dataString )

  def __readFromSystemCommandOutput( self, file, bufferIndex, doAll = False ):
    retDict = self.__readFromFile( file,
                                   len( self.bufferList[ bufferIndex ][0] ),
                                   doAll )
    if retDict[ 'OK' ]:
      self.bufferList[ bufferIndex ][0] += retDict[ 'Value' ]
      if not self.callback == None:
        while self.__callLineCallback( bufferIndex ):
          pass
      return S_OK()
    else: # buffer size limit reached killing process (see comment on __readFromFile)
      self.bufferList[ bufferIndex ][0] += retDict[ 'Value' ]
      exitStatus = self.__killChild( self.child.pid )

      return self.__generateSystemCommandError(
                  exitStatus,
                  "Exceeded maximum buffer size ( %d bytes ) for '%s' call" %
                  ( self.bufferLimit, self.cmdSeq ) )

  def systemCall( self, cmdSeq, callbackFunction = None, shell = False, env = None ):
    self.cmdSeq = cmdSeq
    self.callback = callbackFunction
    try:
      self.child = subprocess.Popen( self.cmdSeq,
                                      shell = shell,
                                      stdout = subprocess.PIPE,
                                      stderr = subprocess.PIPE,
                                      close_fds = True,
                                      env=env )
    except OSError, v:
      retDict = S_ERROR( v )
      retDict['Value'] = ( -1, '' , str(v) )
      return retDict
    except Exception, v:
      retDict = S_ERROR( v )
      retDict['Value'] = ( -1, '' , str(v) )
      return retDict

    self.bufferList = [ [ "", 0 ], [ "", 0 ] ]
    initialTime = time.time()
    exitStatus = self.child.poll()

    while exitStatus == None:
      retDict = self.__readFromCommand()
      if not retDict[ 'OK' ]:
        return retDict

      if self.timeout and time.time() - initialTime > self.timeout:
        exitStatus = self.__killChild()
        self.__readFromCommand( True )
        return self.__generateSystemCommandError(
                    exitStatus,
                    "Timeout (%d seconds) for '%s' call" %
                    ( self.timeout, cmdSeq ) )

      exitStatus = self.child.poll()

    self.__readFromCommand(True )

    if exitStatus >= 256:
      exitStatus /= 256
    return S_OK( ( exitStatus, self.bufferList[0][0], self.bufferList[1][0] ) )

  def __readFromCommand( self, isLast = False ):
    if isLast:
      retDict = self.__readFromSystemCommandOutput( self.child.stdout, 0, True )
      if retDict[ 'OK' ]:
        retDict = self.__readFromSystemCommandOutput( self.child.stderr, 1, True )
      try:
        self.child.stdout.close()
        self.child.stderr.close()
      except Exception, v:
        gLogger.debug( 'Exception while closing pipes to child', str(v) )
      return retDict
    else:
      readSeq = self.__selectFD( [ self.child.stdout, self.child.stderr ], True )
      if self.child.stdout in readSeq:
        retDict = self.__readFromSystemCommandOutput( self.child.stdout, 0 )
        if not retDict[ 'OK' ]:
          return retDict
      if self.child.stderr in readSeq:
        retDict = self.__readFromSystemCommandOutput( self.child.stderr, 1 )
        if not retDict[ 'OK' ]:
          return retDict
      return S_OK()


  def __callLineCallback( self, bufferIndex ):
    nextLineIndex = self.bufferList[ bufferIndex ][0][ self.bufferList[ bufferIndex ][1]: ].find( "\n" )
    if nextLineIndex > -1:
      try:
        self.callback( bufferIndex, self.bufferList[ bufferIndex ][0][
                        self.bufferList[ bufferIndex ][1]:
                        self.bufferList[ bufferIndex ][1] + nextLineIndex ] )
      except Exception, v:
        gLogger.exception( 'Exception while calling callback function',
                           '%s: %s' % ( self.callback.__name__, str(v) ) )
        gLogger.showStack()

      self.bufferList[ bufferIndex ][1] += nextLineIndex + 1
      return True
    return False

def systemCall( timeout, cmdSeq, callbackFunction = None, env = None ):
  """
     Use SubprocessExecutor class to execute cmdSeq (it can be a string or a sequence)
     with a timeout wrapper, it is executed directly without calling a shell
  """
  spObject = Subprocess( timeout )
  return spObject.systemCall( cmdSeq,
                              callbackFunction = callbackFunction,
                              env = env,
                              shell = False )

def shellCall( timeout, cmdSeq, callbackFunction = None, env = None ):
  """
     Use SubprocessExecutor class to execute cmdSeq (it can be a string or a sequence)
     with a timeout wrapper, cmdSeq it is invoque by /bin/sh
  """
  spObject = Subprocess( timeout )
  return spObject.systemCall( cmdSeq,
                              callbackFunction = callbackFunction,
                              env = env,
                              shell = True )

def pythonCall( timeout, function, *stArgs, **stKeyArgs ):
  """
     Use SubprocessExecutor class to execute function with provided arguments,
     with a timeout wrapper.
  """
  spObject = Subprocess( timeout )
  return spObject.pythonCall( function, *stArgs, **stKeyArgs )
