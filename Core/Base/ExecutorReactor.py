########################################################################
# $HeadURL$
# File :   ExecutorReactor.py
# Author : Adria Casajus
########################################################################
__RCSID__ = "$Id$"

import time
import threading
from DIRAC import S_OK, S_ERROR, gLogger, rootPath, gConfig
from DIRAC.Core.DISET.MessageClient import MessageClient
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.private.ModuleLoader import ModuleLoader
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
    self.__loader = ModuleLoader( "Executor",
                                  PathFinder.getExecutorSection,
                                  ExecutorModule )

  def loadModules( self, modulesList, hideExceptions = False ):
    """
      Load all modules required in moduleList
    """
    result = self.__loader.loadModules( modulesList, hideExceptions = hideExceptions )
    if not result[ 'OK' ]:
      return result
    self.__executorModules = self.__loader.getModules()
    return S_OK()

  #Go!
  def go( self ):
    for name in self.__executorModules:
      exeClass = self.__executorModules[ name ][ 'classObj' ]
      result = exeClass._ex_initialize( name, self.__executorModules[ name ][ 'loadName' ] )
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
