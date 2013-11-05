# $HeadURL$
__RCSID__ = "$Id$"
"""
   DIRAC Logger client
"""

import sys
import traceback
import inspect
from DIRAC.FrameworkSystem.private.logging.LogLevels import LogLevels
from DIRAC.FrameworkSystem.private.logging.Message import Message
from DIRAC.Core.Utilities import Time, List
from DIRAC.FrameworkSystem.private.logging.backends.BackendIndex import gBackendIndex
from DIRAC.Core.Utilities import ExitCallback
import DIRAC

DEBUG = 1

class Logger:

  defaultLogLevel = 'NOTICE'

  def __init__( self ):
    self._minLevel = 0
    self._showCallingFrame = False
    self._systemName = False
    self._outputList = []
    self._subLoggersDict = {}
    self._logLevels = LogLevels()
    self.__backendOptions = { 'showHeaders' : True, 'showThreads' : False, 'Color' : True }
    self.__preinitialize()
    self.__initialized = False

  def initialized( self ):
    return self.__initialized

  def showHeaders( self, yesno = True ):
    self.__backendOptions[ 'showHeaders' ] = yesno

  def showThreadIDs( self, yesno = True ):
    self.__backendOptions[ 'showThreads' ] = yesno

  def registerBackends( self, desiredBackends ):
    self._backendsDict = {}
    for backend in desiredBackends:
      backend = backend.lower()
      if not backend in gBackendIndex:
        self.warn( "Unexistant method for showing messages",
                   "Unexistant %s logging method" % backend )
      else:
        self._backendsDict[ backend ] = gBackendIndex[ backend ]( self.__backendOptions )

  def __preinitialize ( self ):
    self._systemName = "Framework"
    self.registerBackends( [ 'stdout' ] )
    self._minLevel = self._logLevels.getLevelValue( "NOTICE" )
    #HACK to take into account dev levels before the command line if fully parsed
    debLevs = 0
    for arg in sys.argv:
      if arg.find( "-d" ) == 0:
        debLevs += arg.count( "d" )
    if debLevs == 1:
      self.setLevel( "VERBOSE" )
    elif debLevs == 2:
      self.setLevel( "VERBOSE" )
      self.showHeaders( True )
    elif debLevs >= 3:
      self.setLevel( "DEBUG" )
      self.showHeaders( True )
      self.showThreadIDs()

  def initialize( self, systemName, cfgPath ):
    if self.__initialized:
      return
    self.__initialized = True

    from DIRAC.ConfigurationSystem.Client.Config import gConfig
    from os import getpid

    #self.__printDebug( "The configuration path is %s" % cfgPath )
    #Get the options for the different output backends
    retDict = gConfig.getOptionsDict( "%s/BackendsOptions" % cfgPath )

    #self.__printDebug( retDict )
    if not retDict[ 'OK' ]:
      cfgBackOptsDict = { 'FileName': 'Dirac-log_%s.log' % getpid(), 'Interactive': True, 'SleepTime': 150 }
    else:
      cfgBackOptsDict = retDict[ 'Value' ]

    self.__backendOptions.update( cfgBackOptsDict )

    if not self.__backendOptions.has_key( 'Filename' ):
      self.__backendOptions[ 'FileName' ] = 'Dirac-log_%s.log' % getpid()

    sleepTime = 150
    try:
      sleepTime = int ( self.__backendOptions[ 'SleepTime' ] )
    except:
      pass
    self.__backendOptions[ 'SleepTime' ] = sleepTime

    self.__backendOptions[ 'Interactive' ] = gConfig.getValue( "%s/BackendsOptions/Interactive" % cfgPath, True )

    self.__backendOptions[ 'Site' ] = DIRAC.siteName()

    self.__backendOptions[ 'Color' ] = gConfig.getValue( "%s/LogColor" % cfgPath, False )

    #Configure outputs
    desiredBackends = gConfig.getValue( "%s/LogBackends" % cfgPath, 'stdout' )
    self.registerBackends( List.fromChar( desiredBackends ) )
    #Configure verbosity
    defaultLevel = Logger.defaultLogLevel
    if "Scripts" in cfgPath:
      defaultLevel = gConfig.getValue( '/Systems/Scripts/LogLevel', Logger.defaultLogLevel )
    self.setLevel( gConfig.getValue( "%s/LogLevel" % cfgPath, defaultLevel ) )
    #Configure framing
    self._showCallingFrame = gConfig.getValue( "%s/LogShowLine" % cfgPath, self._showCallingFrame )
    #Get system name
    self._systemName = str( systemName )

    if not self.__backendOptions['Interactive']:
      ExitCallback.registerExitCallback( self.flushAllMessages )

  def setLevel( self, levelName ):
    levelName = levelName.upper()
    if levelName in self._logLevels.getLevels():
      self._minLevel = abs( self._logLevels.getLevelValue( levelName ) )
      return True
    return False

  def getLevel( self ):
    return self._logLevels.getLevel( self._minLevel )

  def shown( self, levelName ):
    levelName = levelName.upper()
    if levelName in self._logLevels.getLevels():
      return self._logLevels.getLevelValue( levelName ) <= levelName
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

  def notice( self, sMsg, sVarMsg = '' ):
    messageObject = Message( self._systemName,
                             self._logLevels.notice,
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

  def exception( self, sMsg = "", sVarMsg = '', lException = False, lExcInfo = False ):
    if sVarMsg:
      sVarMsg += "\n%s" % self.__getExceptionString( lException, lExcInfo )
    else:
      sVarMsg = "\n%s" % self.__getExceptionString( lException, lExcInfo )
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

  def __getExceptionString( self, lException = False, lExcInfo = False ):
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
      elif len( args ) == 2:
        type = args[0]
        value = args[1]
        stack = ""
      else:
        type = args[0]
        value = args[1]
        stack = "\n".join( args[2] )
    else:
      if not lExcInfo:
        lExcInfo = sys.exc_info()
      type, value = ( lExcInfo[0], lExcInfo[1] )
      stack = "\n".join( traceback.format_tb( lExcInfo[2] ) )
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
      stack.append( f )
      f = f.f_back
    stack.reverse()
    #traceback.print_exc()
    sExtendedException = "Locals by frame, innermost last\n"
    for frame in stack:
      sExtendedException += "\n"
      sExtendedException += "Frame %s in %s at line %s\n" % ( frame.f_code.co_name,
                                           frame.f_code.co_filename,
                                           frame.f_lineno )
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
    return ''.join( traceback.format_list( stack_list[:-2] ) )

  def flushAllMessages( self, exitCode ):
    for backend in self._backendsDict:
      self._backendsDict[ backend ].flush()

  def getSubLogger( self, subName, child = True ):
    from DIRAC.FrameworkSystem.private.logging.SubSystemLogger import SubSystemLogger
    if not subName in self._subLoggersDict.keys():
      self._subLoggersDict[ subName ] = SubSystemLogger( subName, self, child )
    return self._subLoggersDict[ subName ]

  def __printDebug( self, debugString ):
    """ This function is implemented to debug problems with initialization
     of the logger. We have to use it because the Logger is obviously unusable
     during its initialization.
    """
    if DEBUG:
      print debugString

