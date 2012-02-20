########################################################################
# $HeadURL$
# File :   ExecutorReactor.py
# Author : Adria Casajus
########################################################################
__RCSID__ = "$Id$"

import time
import os
import imp
import types
import threading
from DIRAC import S_OK, S_ERROR, gLogger, rootPath, gConfig
from DIRAC.Core.Utilities import ThreadScheduler, List
from DIRAC.Core.DISET.MessageClient import MessageClient
from DIRAC.ConfigurationSystem.Client.Helpers import getInstalledExtensions
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.ExecutorModule import ExecutorModule

class ExecutorReactor( object ):

  class AliveLock( object ):

    def __init__( self ):
      self.__alive = 0
      self.__cond = threading.Condition( threading.Lock() )

    def alive( self ):
      self.__cond.acquire()
      self.__alive += 1
      self.__cond.release()

    def dead( self ):
      self.__cond.acquire()
      self.__alive -= 1
      self.__cond.notify()
      self.__cond.release()

    def lockUntilAllDead( self ):
      self.__cond.acquire()
      while True:
        if self.__alive < 1:
          break
        self.__cond.wait( 1 )
      self.__cond.release()

  class MindCluster( object ):

    def __init__( self, mindName, aliveLock ):
      self.__mindName = mindName
      self.__modules = {}
      self.__maxTasks = 1
      self.__reconnectSleep = 1
      self.__reconnectRetries = 10
      self.__extraArgs = {}
      self.__instances = {}
      self.__instanceLock = threading.Lock()
      self.__aliveLock = aliveLock

    def updateMaxTasks( self, mt ):
      self.__maxTasks = max( self.__maxTasks, mt )

    def addModule( self, name, exeClass ):
      self.__modules[ name ] = exeClass
      self.__maxTasks = max( self.__maxTasks, exeClass.ex_getOption( "MaxTasks" ) )
      self.__reconnectSleep = max( self.__reconnectSleep, exeClass.ex_getOption( "ReconnectSleep" ) )
      self.__reconnectRetries = max( self.__reconnectRetries, exeClass.ex_getOption( "ReconnectRetries" ) )
      self.__extraArgs[ name ] = exeClass.ex_getExtraArguments()

    def connect( self ):
      self.__msgClient = MessageClient( self.__mindName )
      self.__msgClient.subscribeToMessage( 'ProcessTask', self.__processTask )
      self.__msgClient.subscribeToDisconnect( self.__disconnected )
      result = self.__msgClient.connect( executorTypes = list( self.__modules.keys() ),
                                         maxTasks = self.__maxTasks,
                                         extraArgs = self.__extraArgs )
      if result[ 'OK' ]:
        self.__aliveLock.alive()
        gLogger.info( "Connected to %s" % self.__mindName )
      return result

    def __disconnected( self, msgClient ):
      retryCount = 0
      while True:
        gLogger.notice( "Trying to reconnect to %s" % self.__mindName )
        result = self.__msgClient.connect( executorTypes = list( self.__modules.keys() ),
                                           maxTasks = self.__maxTasks,
                                           extraArgs = self.__extraArgs )

        if result[ 'OK' ]:
          if retryCount >= self.__reconnectRetries:
            self.__aliveLock.alive()
          gLogger.notice( "Reconnected to %s" % self.__mindName )
          return S_OK()
        retryCount += 1
        if retryCount == self.__reconnectRetries:
          self.__aliveLock.alive()
        gLogger.info( "Connect error failed: %s" % result[ 'Message' ] )
        gLogger.notice( "Failed to reconnect. Sleeping for %d seconds" % self.__reconnectSleep )
        time.sleep( self.__reconnectSleep )

    def __storeInstance( self, modName, modObj ):
      self.__instanceLock.acquire()
      try:
        self.__instances[ modName ].append( modObj )
      finally:
        self.__instanceLock.release()

    def __getInstance( self, moduleName ):
      self.__instanceLock.acquire()
      try:
        if moduleName not in self.__instances:
          self.__instances[ moduleName ] = []
        try:
          return S_OK( self.__instances[ moduleName ].pop( 0 ) )
        except IndexError:
          pass
      finally:
        self.__instanceLock.release()
      try:
        modObj = self.__modules[ moduleName ]
      except KeyError:
        return S_ERROR( "Unknown %s executor" )
      modInstance = modObj()
      return S_OK( modInstance )

    def __sendExecutorError( self, taskId, errMsg ):
      result = self.__msgClient.createMessage( "ExecutorError" )
      if not result[ 'OK' ]:
        return result
      msgObj = result[ 'Value' ]
      msgObj.taskId = taskId
      msgObj.errorMsg = errMsg
      return self.__msgClient.sendMessage( msgObj )

    def __processTask( self, msgObj ):
      module = msgObj.eType
      taskId = msgObj.taskId
      taskStub = msgObj.taskStub
      eType = msgObj.eType
      result = self.__getInstance( eType )
      if not result[ 'OK' ]:
        return self.__sendExecutorError( taskId, result[ 'Message' ] )
      modInstance = result[ 'Value' ]
      try:
        procResult = modInstance._ex_processTask( taskId, taskStub )
      except Exception, excp:
        gLogger.exception( "Error while processing task %s" % taskId )
        return self.__sendExecutorError( taskId, "Error processing task %s: %s" % ( taskId, excp ) )

      if not procResult[ 'OK' ]:
        msgName = "TaskError"
        gLogger.info( "Task %d has had an error: %s" % ( taskId, procResult[ 'Message' ] ) )
      else:
        taskStub, freezeTime = procResult[ 'Value' ]
        if freezeTime:
          msgName = "TaskFreeze"
        else:
          msgName = "TaskDone"

      result = self.__msgClient.createMessage( msgName )
      if not result[ 'OK' ]:
        return self.__sendExecutorError( taskId, "Can't generate %s message: %s" % ( msgName, result[ 'Message' ] ) )
      self.__storeInstance( eType, modInstance )
      gLogger.verbose( "Task %s: Sending %s" % ( str( taskId ), msgName ) )
      msgObj = result[ 'Value' ]
      msgObj.taskId = taskId
      msgObj.taskStub = taskStub
      if not procResult[ 'OK' ]:
        msgObj.errorMsg = procResult[ 'Message' ]
      elif freezeTime:
        msgObj.freezeTime = freezeTime
      return self.__msgClient.sendMessage( msgObj )


  #####
  # Start of ExecutorReactor
  #####


  def __init__( self ):
    self.__aliveLock = self.AliveLock()
    self.__executorModules = {}
    self.__codeModules = {}
    self.__minds = {}

  def __recurseImport( self, modName, parentModule = False, hideExceptions = False ):
    if type( modName ) in types.StringTypes:
      modName = List.fromChar( modName, "." )
    try:
      if parentModule:
        impData = imp.find_module( modName[0], parentModule.__path__ )
      else:
        impData = imp.find_module( modName[0] )
      impModule = imp.load_module( modName[0], *impData )
      if impData[0]:
        impData[0].close()
    except ImportError, excp:
      if str( excp ).find( "No module named" ) == 0:
        return S_OK()
      errMsg = "Can't load %s" % ".".join( modName )
      if not hideExceptions:
        gLogger.exception( errMsg )
      return S_ERROR( errMsg )
    if len( modName ) == 1:
      return S_OK( impModule )
    return self.__recurseImport( modName[1:], impModule )

  def loadModules( self, modulesList, hideExceptions = False ):
    """
      Load all modules required in moduleList
    """
    modsToLoad = []
    for module in modulesList:
      gLogger.verbose( "Checking %s" % module )
      #if it's a executor module name just load it and be done with it
      if module.find( "/" ) > -1:
        result = self.loadModule( module, hideExceptions = hideExceptions )
        if not result[ 'OK' ]:
          return result
        continue
      #Check if it's a system name
      #Look in the CS
      system = module
      csExePath = "%s/Executors" % PathFinder.getSystemSection ( system, ( system, ) )
      gLogger.verbose( "Exploring %s to discover executors" % csExePath )
      result = gConfig.getSections( csExePath )
      if result[ 'OK' ]:
        #Add all executors in the CS :P
        for executor in result[ 'Value' ]:
          if executor not in modsToLoad:
            result = self.loadModule( "%s/%s" % ( system, executor ), hideExceptions = hideExceptions )
            if not result[ 'OK' ]:
              return result
      #Look what is installed
      parentModule = False
      for rootModule in getInstalledExtensions():
        if system.find( "System" ) != len( system ) - 6:
          modName = "%s.%sSystem.Executor" % ( rootModule, system )
        else:
          modName = "%s.%s.Executor" % ( rootModule, system )
        result = self.__recurseImport( modName )
        if not result[ 'OK' ]:
          return result
        parentModule = result[ 'Value' ]
        if parentModule:
          break
      if not parentModule:
        continue
      parentPath = parentModule.__path__[0]
      gLogger.notice( "Found executors path at %s" % modName )
      for entry in os.listdir( parentPath ):
        if entry[-3:] != ".py" or entry == "__init__.py":
          continue
        if not os.path.isfile( os.path.join( parentPath, entry ) ):
          continue
        execName = "%s/%s" % ( system, entry[:-3] )
        gLogger.verbose( "Trying to import %s" % execName )
        result = self.loadModule( execName,
                                  hideExceptions = hideExceptions,
                                  parentModule = parentModule )

    return S_OK()

  def loadModule( self, execName, hideExceptions = False, parentModule = False ):
    """
      Load module execName.
      execName must take the form [DIRAC System Name]/[DIRAC Agent Name]
      then:
      - calls the am_initialize method of the imported Agent
      - determines the pooling interval: am_getPollingTime,
      - determines the number of executions: am_getMaxCycles
      - creates a periodic Task in the ThreadScheduler: am_go

    """
    if execName in self.__executorModules:
     return S_OK()
    execList = execName.split( "/" )
    if len( execList ) != 2:
      return S_ERROR( "Can't load %s: Invalid executor name" % ( execName ) )
    csSection = PathFinder.getExecutorSection( execName )
    loadGroup = gConfig.getValue( "%s/Load" % csSection, [] )
    #Check if it's a load group
    if loadGroup:
      gLogger.info( "Found load group %s. Will load %s" % ( execName, ", ".join( loadGroup ) ) )
      for executorName in loadGroup:
        if executorName.find( "/" ) == -1:
          executorName = "%s/%s" % ( execList[0], executorName )
        result = self.loadModule( executorName, hideExceptions = hideExceptions, parentModule = parentModule )
        if not result[ 'OK' ]:
          return result
      return S_OK()
    #Normal load
    codeName = gConfig.getValue( "%s/Module" % csSection, "" )
    if not codeName:
      codeName = execName
      gLogger.info( "Loading %s" % ( execName ) )
    else:
      if codeName.find( "/" ) == -1:
        codeName = "%s/%s" % ( execList[0], codeName )
      gLogger.info( "Loading %s (%s)" % ( execName, codeName ) )
    #If already loaded, skip
    codeList = codeName.split( "/" )
    if len( codeList ) != 2:
      return S_ERROR( "Can't load %s: Invalid executor name" % ( codeName ) )
    system, modName = codeList
    if codeName not in self.__codeModules:
      if parentModule:
        #If we've got a parent module, load from there.
        result = self.__recurseImport( modName, parentModule, hideExceptions = hideExceptions )
      else:
        #Check to see if the module exists in any of the root modules
        rootModulesToLook = getInstalledExtensions()
        for rootModule in rootModulesToLook:
          importString = '%s.%sSystem.Executor.%s' % ( rootModule, system, modName )
          gLogger.verbose( "Trying to load %s" % importString )
          result = self.__recurseImport( importString, hideExceptions = hideExceptions )
          #Error while loading
          if not result[ 'OK' ]:
            return result
          #Something has been found! break :)
          if result[ 'Value' ]:
            gLogger.verbose( "Found %s" % importString )
            break
      #Nothing found
      if not result[ 'Value' ]:
        return S_ERROR( "Could not find %s" % codeName )
      exeModule = result[ 'Value' ]
      try:
        #Try to get the class from the module
        exeClass = getattr( exeModule, modName )
      except AttributeError:
        location = ""
        if '__file__' in dir( exeModule ):
          location = exeModule.__file__
        else:
          locateion = exeModule.__path__
        gLogger.exception( "%s module does not have a %s class!" % ( location, modName ) )
        return S_ERROR( "Cannot load %s" % modName )
      #Check if it's subclass
      if not issubclass( exeClass, ExecutorModule ):
        return S_ERROR( "%s has to inherit from ExecutorModule" % codeName )
      self.__codeModules[ codeName ] = { 'classObj' : exeClass, 'moduleObj' : exeModule }
      #End of loading of 'codeName' module

    #A-OK :)
    self.__executorModules[ execName ] = self.__codeModules[ codeName ].copy()
    #keep the name of the real code module
    self.__executorModules[ execName ][ 'codeName' ] = codeName
    gLogger.notice( "Loaded module %s" % execName )

    return S_OK()

  #Go!
  def go( self ):
    for name in self.__executorModules:
      exeClass = self.__executorModules[ name ][ 'classObj' ]
      result = exeClass._ex_initialize( name, self.__executorModules[ name ][ 'codeName' ] )
      if not result[ 'OK' ]:
        return result
      mind = exeClass.ex_getMind()
      if mind not in self.__minds:
        self.__minds[ mind ] = self.MindCluster( mind, self.__aliveLock )
      mc = self.__minds[ mind ]
      mc.addModule( name, exeClass )
    for mindName in self.__minds:
      gLogger.info( "Trying to connect to %s" % mindName )
      result = self.__minds[ mindName ].connect()
      if not result[ 'OK' ]:
        return result
    self.__aliveLock.lockUntilAllDead()
    return S_OK()
