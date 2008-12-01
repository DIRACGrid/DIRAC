# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/LoggingSystem/private/Logger.py,v 1.30 2008/12/01 11:47:08 acasajus Exp $
__RCSID__ = "$Id: Logger.py,v 1.30 2008/12/01 11:47:08 acasajus Exp $"
"""
   DIRAC Logger client
"""

import sys
import traceback
import os
import os.path
import re
import inspect
import Queue
from DIRAC.LoggingSystem.private.LogLevels import LogLevels
from DIRAC.LoggingSystem.private.Message import Message
from DIRAC.Core.Utilities import Time, List
from DIRAC.LoggingSystem.private.backends.BackendIndex import gBackendIndex
from DIRAC.Core.Utilities import ExitCallback, ColorCLI

class Logger:

  def __init__( self ):
    self._minLevel = 0
    self._showCallingFrame = False
    self._systemName = False
    self._outputList = []
    self._subLoggersDict = {}
    self._logLevels = LogLevels()
    self.__preinitialize()

  def initialized( self ):
    return not self._systemName == False

  def registerBackends( self, desiredBackends ):
    self._backendsDict = {}
    for backend in desiredBackends:
      backend = backend.lower()
      if not backend in gBackendIndex:
        self.warn( "Unexistant method for showing messages",
                   "Unexistant %s logging method" % backend )
      else:
        self._backendsDict[ backend ] = gBackendIndex[ backend ]( self.backendsOptions )

  def __preinitialize ( self ):
    self._systemName = "Framework"
    self.backendsOptions = {}
    self.registerBackends( [ 'stdout' ] )
    self._minLevel = self._logLevels.getLevelValue( "INFO" )

  def initialize( self, systemName, cfgPath ):
    if self._systemName == "Framework":
      from DIRAC.ConfigurationSystem.Client.Config import gConfig
      from os import getpid
      #Get the options for the different output backends
      retDict = gConfig.getOptionsDict( "%s/BackendsOptions" % cfgPath )
      if not retDict[ 'OK' ]:
        self.backendsOptions = { 'FileName': 'Dirac-log_%s.log' % getpid(),
                                 'Interactive': True, 'SleepTime': 150 }
      else:
        self.backendsOptions = retDict[ 'Value' ]

        if not self.backendsOptions.has_key( 'Filename' ):
          self.backendsOptions[ 'FileName' ] = 'Dirac-log_%s.log' % getpid()

        if self.backendsOptions.has_key( 'SleepTime' ):
          self.backendsOptions[ 'SleepTime' ] = int ( self.backendsOptions[ 'SleepTime' ] )
        else:
          self.backendsOptions[ 'SleepTime' ] = 150

        if self.backendsOptions.has_key( 'Interactive' ) and \
               self.backendsOptions[ 'Interactive' ].lower() \
               in ( "n", "no", "0", "false" ) :
          self.backendsOptions[ 'Interactive' ] = False
        else:
          self.backendsOptions[ 'Interactive' ] = True

      site = gConfig.getValue( "/LocalSite/Site", 'Unknown' )
      self.backendsOptions[ 'Site' ] = site

      #Configure outputs
      desiredBackends = gConfig.getValue( "%s/LogBackends" % cfgPath,
                                          'stdout' )
      self.registerBackends( List.fromChar( desiredBackends ) )
      #Configure verbosity
      self.setLevel( gConfig.getValue( "%s/LogLevel" % cfgPath, "INFO" ) )
      #Configure framing
      retDict = gConfig.getOption( "%s/LogShowLine" % cfgPath )
      if retDict[ 'OK' ] and retDict[ 'Value' ].lower() in ( "y", "yes", "1", "true" ) :
        self._showCallingFrame = True
      self._systemName = str( systemName )
      if not self.backendsOptions['Interactive']:
        ExitCallback.registerExitCallback( self.flushAllMessages )

  def setLevel( self, levelName ):
    levelName = levelName.upper()
    if levelName.upper() in self._logLevels.getLevels():
        self._minLevel = abs( self._logLevels.getLevelValue( levelName ) )
        return True
    return False


  def getName( self ):
    return self._systemName

  def always( self, sMsg, sVarMsg = '' ):
    messageObject = Message( self._systemName,
                             self._logLevels.always,
                             Time.dateTime(),
                             sMsg,
                             sVarMsg,
                             self.__discoverCallingFrame() )
    return self.processMessage( messageObject )

  def info( self, sMsg, sVarMsg = '' ):
    messageObject = Message( self._systemName,
                             self._logLevels.info,
                             Time.dateTime(),
                             sMsg,
                             sVarMsg,
                             self.__discoverCallingFrame() )
    return self.processMessage( messageObject )

  def verbose( self, sMsg, sVarMsg = '' ):
    messageObject = Message( self._systemName,
                             self._logLevels.verbose,
                             Time.dateTime(),
                             sMsg,
                             sVarMsg,
                             self.__discoverCallingFrame() )
    return self.processMessage( messageObject )

  def debug( self, sMsg, sVarMsg = '' ):
    messageObject = Message( self._systemName,
                             self._logLevels.debug,
                             Time.dateTime(),
                             sMsg,
                             sVarMsg,
                             self.__discoverCallingFrame() )
    return self.processMessage( messageObject )

  def warn( self, sMsg, sVarMsg = '' ):
    messageObject = Message( self._systemName,
                             self._logLevels.warn,
                             Time.dateTime(),
                             sMsg,
                             sVarMsg,
                             self.__discoverCallingFrame() )
    return self.processMessage( messageObject )

  def error( self, sMsg, sVarMsg = '' ):
    messageObject = Message( self._systemName,
                             self._logLevels.error,
                             Time.dateTime(),
                             sMsg,
                             sVarMsg,
                             self.__discoverCallingFrame() )
    return self.processMessage( messageObject )

  def exception( self, sMsg = "", sVarMsg = '', lException = False ):
    if sVarMsg:
      sVarMsg += "\n%s" % self.__getExceptionString( lException )
    else:
      sVarMsg = "\n%s" % self.__getExceptionString( lException )
    messageObject = Message( self._systemName,
                             self._logLevels.exception,
                             Time.dateTime(),
                             sMsg,
                             sVarMsg,
                             self.__discoverCallingFrame() )
    return self.processMessage( messageObject )

  def fatal( self, sMsg, sVarMsg = '' ):
    messageObject = Message( self._systemName,
                             self._logLevels.fatal,
                             Time.dateTime(),
                             sMsg,
                             sVarMsg,
                             self.__discoverCallingFrame() )
    return self.processMessage( messageObject )

  def showStack( self ):
    messageObject = Message( self._systemName,
                             self._logLevels.debug,
                             Time.dateTime(),
                             "",
                             self.__getStackString(),
                             self.__discoverCallingFrame() )
    self.processMessage( messageObject )

  def processMessage( self, messageObject ):
    if self.__testLevel( messageObject.getLevel() ):
      if not messageObject.getName():
        messageObject.setName( self._systemName )
      self._processMessage( messageObject )
    return True
  #S_OK()

  def __testLevel( self, sLevel ):
    return abs( self._logLevels.getLevelValue( sLevel ) ) >= self._minLevel

  def _processMessage( self, messageObject ):
    for backend in self._backendsDict:
      self._backendsDict[ backend ].doMessage( messageObject )

  def __getExceptionString( self, lException = False ):
    if lException:
      try:
        args = lException.args
      except:
        return "Passed exception to the logger is not a valid Exception: %s" % str( lException )
      if len( args ) == 0:
        type = "Unknown exception type"
        value = "Unknown exception"
        stack = ""
      elif len( args ) == 1:
        type = "Unknown exception type"
        value = args[0]
        stack = ""
      elif len(args) == 2:
        type = args[0]
        value = args[1]
        stack = ""
      else:
        type = args[0]
        value = args[1]
        stack = "\n".join( args[2] )
    else:
      lExcinfo = sys.exc_info()
      type, value = (lExcinfo[0],lExcinfo[1])
      stack = "\n".join( traceback.format_tb( lExcinfo[2] ) )
    return "== EXCEPTION ==\n%s:%s\n%s===============" % (
                         type,
                         value,
                         stack )


  def __discoverCallingFrame( self ):
    if self.__testLevel( self._logLevels.debug ) and self._showCallingFrame:
      oActualFrame = inspect.currentframe()
      lOuterFrames = inspect.getouterframes( oActualFrame )
      lCallingFrame = lOuterFrames[2]
      return "%s:%s" % ( lCallingFrame[1].replace( sys.path[0], "" )[1:], lCallingFrame[2] )
    else:
      return ""

  def __getExtendedExceptionString( self, lException = None ):
    """
    Print the usual traceback information, followed by a listing of all the
    local variables in each frame.
    """
    if lException:
      tb = lException[2]
    else:
      tb = sys.exc_info()[2]
    if not tb:
      return
    while 1:
      if not tb.tb_next:
        break
      tb = tb.tb_next
    stack = []
    f = tb.tb_frame
    while f:
      stack.append(f)
      f = f.f_back
    stack.reverse()
    #traceback.print_exc()
    sExtendedException = "Locals by frame, innermost last\n"
    for frame in stack:
      sExtendedException += "\n"
      sExtendedException += "Frame %s in %s at line %s\n" % ( frame.f_code.co_name,
                                           frame.f_code.co_filename,
                                           frame.f_lineno)
      for key, value in frame.f_locals.items():
        #We have to be careful not to cause a new error in our error
        #printer! Calling str() on an unknown object could cause an
        #error we don't want.
        try:
          sExtendedException += "\t%20s = %s\n" % ( key, value )
        except:
          sExtendedException += "\t%20s = <ERROR WHILE PRINTING VALUE>\n" % key
    return sExtendedException

  def __getStackString( self ):
    """ This function returns the stack as a string to be printed via
     a debug message, the upper 3 levels are skipped since they correspond
     to gLogger.showStack,  self.__getStackString, traceback.print_stack
	"""
    stack_list = traceback.extract_stack()
    return ''.join( traceback.format_list( stack_list[:-2] ))

  def flushAllMessages( self, exitCode ):
    for backend in self._backendsDict:
      self._backendsDict[ backend ].flush()

  def getSubLogger( self, subName, child = True ):
    from DIRAC.LoggingSystem.private.SubSystemLogger import SubSystemLogger
    if not subName in self._subLoggersDict.keys():
      self._subLoggersDict[ subName ] = SubSystemLogger( subName, self, child )
    return self._subLoggersDict[ subName ]

