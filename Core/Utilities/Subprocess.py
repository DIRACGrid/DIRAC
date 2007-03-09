# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Subprocess.py,v 1.1 2007/03/09 15:33:19 rgracian Exp $
__RCSID__ = "$Id: Subprocess.py,v 1.1 2007/03/09 15:33:19 rgracian Exp $"
"""
   DIRAC Wrapper to execute python and system commands with a wrapper, that might 
   set a timeout.
   3 FUNCTIONS are provided:
     - shellCall( iTimeOut, sCmd, oCallbackFunction = None, env = None ): 
       it uses subprocess.Popen class with "shell = True". 
       If sCmd is a string, it specifies the command string to execute through 
       the shell.  If sCmd is a sequence, the first item specifies the command 
       string, and any additional items will be treated as additional shell arguments.
       
     - systemCall( iTimeOut, sCmd, oCallbackFunction = None, env = None ):
       it uses subprocess.Popen class with "shell = False". 
       sCmd should be a string, or a sequence of program arguments. 

       stderr and stdout are piped. oCallbackFunction( iPipe, sLine ) can be 
       defined to process the stdout and stderr as they are produced

       They return a DIRAC.ReturnValue dictionary with a tuple in Value 
       ( returncode, stdout, stderr ) the tuple will also be available upon 
       timeout error or buffer overflow error.
     
     - pythonCall( )
"""

from DIRAC.Core.Utilities.ReturnValues import S_ERROR, S_OK

import time
import select
import os
import sys
import popen2
import subprocess

class SubprocessExecuter:

    def __init__( self, iTimeout = False ):
        self.changeTimeout( iTimeout )
        self.iBufferLimit = 5242880 # 5MB limit for data

    def changeTimeout( self, iTimeout ):
        self.iTimeout = iTimeout
        if self.iTimeout == 0:
            self.iTimeout = False
        
    def __readFromPipe( self, oPipe, iBaseLength = 0 ):
        sData = ""
        iMaxSliceLength = 8192
        iLastSliceLength = 8192
        
        while iLastSliceLength == iMaxSliceLength:
            sReadBuffer = os.read( oPipe, iMaxSliceLength )
            iLastSliceLength = len( sReadBuffer )
            sData += sReadBuffer
            if len( sData ) + iBaseLength > self.iBufferLimit:
                dRetVal = S_ERROR( "Reached maximum allowed length (%d bytes) for called function return value" % self.iBufferLimit )
                dRetVal[ 'ReadData' ] = sData
                return dRetVal
            
        return S_OK( sData )
                    
    def __executePythonFunction( self, oFunc, oWritePipe, *stArgs, **stKeyArgs ):
        try:
            os.write( oWritePipe, "%s\n" % str( S_OK( oFunc( *stArgs, **stKeyArgs ) ) ) )
        except OSError, v:
          if str(v) == '[Errno 32] Broken pipe':
            # the parent has died
            pass
        except Exception, v:
            os.write( oWritePipe, "%s\n" % str( S_ERROR( str( v ) ) ) )
        try:
            os.close( oWritePipe )
        finally:
            os._exit(0)
    
    def __selectFD( self, lR, iTimeout = False ):
        if self.iTimeout and not iTimeout:
            iTimeout = self.iTimeout
        if not iTimeout: 
            return select.select( lR , [], [] )[0]
        else:
            return select.select( lR , [], [], iTimeout )[0]
    
    def pythonCall( self, oFunction, *stArgs, **stKeyArgs ):
        oReadPipe, oWritePipe = os.pipe()
        iPid = os.fork()
        if iPid == 0:
            os.close( oReadPipe )
            self.__executePythonFunction( oFunction, oWritePipe, *stArgs, **stKeyArgs )
            os.close( oWritePipe )
        else:
            os.close( oWritePipe )
            lReadable = self.__selectFD( [ oReadPipe ] )
            if len( lReadable ) == 0:
                try:
                  os.kill( iPid, 9 )
                except OSError, v:
                  if not str(v) == '[Errno 3] No such process':
                    raise v
                
                #HACK to avoid python bug
                # self.oChild.wait()
                while os.waitpid( iPid, 0 ) == -1:
                  time.sleep( 0.000001 )
                # FIXME: stdout and stderr, should be read? 
                os.close( oReadPipe )
                return S_ERROR( "%d seconds timeout for '%s' call" % ( self.iTimeout, oFunction.__name__ ) )
            elif lReadable[0] == oReadPipe:
                dData = self.__readFromPipe( oReadPipe )
                os.close( oReadPipe )
                os.waitpid( iPid, 0 )
                if dData[ 'OK' ]:
                    return eval( dData[ 'Value' ] )
                return dData
            
    def __generateSystemCommandError( self, iExitStatus, sMessage ):
        retVal = S_ERROR( sMessage )
        retVal[ 'Value' ] = ( iExitStatus, self.lBuffers[0][0], self.lBuffers[1][0] )
        return retVal
        
    def __readFromFile( self, oFile, iBaseLength, bAll ):
        try:
            if bAll:
                sData = "".join( oFile.readlines() )
            else:
                sData = oFile.readline()
        except Exception, v:
            pass 
        if sData == "":
            #self.checkAlive()
            self.bAlive = False
        if len( sData ) + iBaseLength > self.iBufferLimit:
            dRetVal = S_ERROR( "Reached maximum allowed length (%d bytes) for called function return value" % self.iBufferLimit )
            dRetVal[ 'ReadData' ] = sData
            return dRetVal
            
        return S_OK( sData )

    def __readFromSystemCommandOutput( self, oFile, iDataIndex, bAll = False ):
        retVal = self.__readFromFile( oFile, len( self.lBuffers[ iDataIndex ][0] ), bAll )
        if retVal[ 'OK' ]:
            self.lBuffers[ iDataIndex ][0] += retVal[ 'Value' ]
            if not self.oCallback == None:
                while self.__callLineCallback( iDataIndex ):
                    pass
            return S_OK()
        else:
            self.lBuffers[ iDataIndex ][0] += retVal[ 'ReadData' ]
            try:
              os.kill( self.oChild.pid, 9 )
            except:
              if not str(v) == '[Errno 3] No such process':
                raise v
            
            #HACK to avoid python bug
            # self.oChild.wait()
            iExitStatus = self.oChild.poll()
            while iExitStatus == None:
              time.sleep( 0.000001 )
              iExitStatus = self.oChild.poll()
            return self.__generateSystemCommandError( 
          iExitStatus, 
          "Exceeded maximum buffer size ( %d bytes ) timeout for '%s' call" % ( self.iBufferLimit, self.sCmd ) )

    def systemCall( self, sCmd, oCallbackFunction = None, shell = False, env = None ):
        self.sCmd = sCmd
        self.oCallback = oCallbackFunction
        try:
          self.oChild = subprocess.Popen( self.sCmd, 
                                          shell = shell,
                                          stdout = subprocess.PIPE,
                                          stderr = subprocess.PIPE,
                                          close_fds = True,
                                          env=env,
                                        )
        except OSError, v:
          retVal = S_ERROR( v )
          retVal['Value': ( -1, '' , str(v) ) ]
          return retVal
        except Exception, v:
          retVal = S_ERROR( v )
          retVal['Value'] = ( -1, '' , str(v) )
          return retVal
        self.lBuffers = [ [ "", 0 ], [ "", 0 ] ]
        iInitialTime = time.time()
        iExitStatus = self.oChild.poll()
        while iExitStatus == None:
            retVal = self.__readFromCommand()
            if not retVal[ 'OK' ]:
                return retVal
            if self.iTimeout and time.time() - iInitialTime > self.iTimeout:
                try:
                  os.kill( self.oChild.pid, 9 )
                except OSError, v:
                  # FIXME
                  if not str(v) == '[Errno 3] No such process':
                    raise v
                
                #HACK to avoid python bug
                # self.oChild.wait()
                iExitStatus = self.oChild.poll()
                while iExitStatus == None:
                  time.sleep( 0.000001 )
                  iExitStatus = self.oChild.poll()
                self.__readFromCommand( True )
                self.oChild.stdout.close()
                self.oChild.stderr.close()
                return self.__generateSystemCommandError( 
              iExitStatus,
              "Timeout (%d seconds) for '%s' call" % ( self.iTimeout, sCmd ) )
            iExitStatus = self.oChild.poll()
  
        self.__readFromCommand(True )

        self.oChild.stdout.close()
        self.oChild.stderr.close() 
        if iExitStatus >= 256:
          iExitStatus /= 256
        return S_OK( ( iExitStatus, self.lBuffers[0][0], self.lBuffers[1][0] ) )

    def __readFromCommand( self, bLast = False ):
        if bLast:
            # retVal = self.__readFromSystemCommandOutput( self.oChild.fromchild, 0, True )
            retVal = self.__readFromSystemCommandOutput( self.oChild.stdout, 0, True )
            if not retVal[ 'OK' ]:
                return retVal
            # retVal = self.__readFromSystemCommandOutput( self.oChild.childerr, 1, True )
            retVal = self.__readFromSystemCommandOutput( self.oChild.stderr, 1, True )
            if not retVal[ 'OK' ]:
                return retVal
        else:
            # lReadable = self.__selectFD( [ self.oChild.fromchild, self.oChild.childerr ], 1 )
            lReadable = self.__selectFD( [ self.oChild.stdout, self.oChild.stderr ], 1 )
            if self.oChild.stdout in lReadable:
                retVal = self.__readFromSystemCommandOutput( self.oChild.stdout, 0 )
                if not retVal[ 'OK' ]:
                    return retVal
            if self.oChild.stderr in lReadable:
                retVal = self.__readFromSystemCommandOutput( self.oChild.stderr, 1 )
                if not retVal[ 'OK' ]:
                    return retVal
        return S_OK()

    
    def __callLineCallback( self, iIndex ):
        iNextLine = self.lBuffers[ iIndex ][0][ self.lBuffers[ iIndex ][1]: ].find( "\n" )
        if iNextLine > -1:
            self.oCallback( iIndex, self.lBuffers[ iIndex ][0][ self.lBuffers[ iIndex ][1]: self.lBuffers[ iIndex ][1] + iNextLine ] )
            self.lBuffers[ iIndex ][1] += iNextLine + 1 
            return True
        return False

      
def systemCall( iTimeOut, sCmd, oCallbackFunction = None, env = None ):
  """
     Use SubprocessExecutor class to execute sCmd (it can be a string or a sequence)
     with a timeout wrapper, it is executed directly without calling a shell
  """
  subprocessObject = SubprocessExecuter( iTimeOut )
  return subprocessObject.systemCall( sCmd, 
                               oCallbackFunction = oCallbackFunction,
                               env = env,
                               shell = False )
                               
def shellCall( iTimeOut, sCmd, oCallbackFunction = None, env = None ):
  """
     Use SubprocessExecutor class to execute sCmd (it can be a string or a sequence)
     with a timeout wrapper, sCmd it is invoque by /bin/sh
  """
  subprocessObject = SubprocessExecuter( iTimeOut )
  return subprocessObject.systemCall( sCmd, 
                               oCallbackFunction = oCallbackFunction,
                               env = env,
                               shell = True )

def pythonCall( iTimeOut, oFunction, *stArgs, **stKeyArgs ):
  """
     Use SubprocessExecutor class to execute oFunction with provided arguments,
     with a timeout wrapper.
  """
  subprocessObject = SubprocessExecuter( iTimeOut )
  return subprocessObject.pythonCall( oFunction, *stArgs, **stKeyArgs )
  return S_OK()
