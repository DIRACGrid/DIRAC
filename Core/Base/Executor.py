import sys, time, threading
from DIRAC import gLogger
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR, isReturnStructure
from DIRAC.Core.DISET.MessageClient import MessageClient
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC.Core.Utilities.ProcessPool import ProcessPool

class Executor( AgentModule ):

  def connect( self, mindName, maxTasks = 1, useProcesses = False, name = False ):
    self.am_setOption( "PollingTime", 300 )
    self.am_setOption( "AutoReconect", True )
    self.am_setOption( "ReconnectRetries", 10 )
    self.am_setOption( "ReconnectWaitTime", 10 )
    self.__taskData = threading.local()
    self.__taskData.freezeTime = 0
    self.__maxTasks = maxTasks
    self.__useProcesses = useProcesses
    self.__msgClient = MessageClient( mindName )
    self.__msgClient.subscribeToMessage( 'ProcessTask', self.__queueTask )
    self.__msgClient.subscribeToDisconnect( self.__disconnected )
    if not name:
      self.__name = self.am_getModuleParam( 'fullName' )
    else:
      self.__name = name
    if maxTasks > 1:
      if useProcesses:
        self.__pool = ProcessPool( maxTasks )
      else:
        self.__pool = ThreadPool( maxTasks )
      self.__pool.daemonize()
    self.__connectKWArgs = { 'executorName' : self.__name,
                              'maxTasks' : maxTasks }
    return self.__msgClient.connect( **self.__connectKWArgs )

  def am_go( self ):
    result = self._setShifterProxy()
    if not result[ 'OK' ]:
      return result
    cpuStats = self._startReportToMonitoring()
    #Do statistics??
    if cpuStats:
      self._endReportToMonitoring( *cpuStats )
    return S_OK()

  def __queueTask( self, msgObj ):
    if self.__maxTasks > 1:
      if self.__useProcesses:
        return self.__pool.createAndQueueTask( self.__executeTask, ( msgObj.taskId, msgObj.taskStub ) )
      else:
        return self.__pool.generateJobAndQueueIt( self.__executeTask, ( msgObj.taskId, msgObj.taskStub ) )
    return self.__executeTask( msgObj.taskId, msgObj.taskStub )

  def __disconnected( self, msgClient ):
    if not self.am_getOption( "AutoReconnect", True ):
      self.log.notice( "Executor has been disconnected. Exiting..." )
      sys.exit( 1 )
    self.log.notice( "Executor has been disconnected" )
    retries = self.am_getOption( "ReconnectRetries", 10 )
    for i in range( retries ):
      self.log.notice( "Trying to reconnect..." )
      result = self.__msgClient.connect( **self.__connectKWArgs )
      if result[ 'OK' ]:
        self.log.notice( "Reconnect successful" )
        return S_OK()
      sleepTime = self.am_getOption( "ReconnectWaitTime", 10 )
      self.log.notice( "Failed to reconnect. Sleeping for %s seconds" % sleepTime )
      self.log.info( "Connect error was: %s" % result[ 'Message' ] )
      time.sleep( sleepTime )
    self.log.error( "Failed to reconnect after %s retries. Exiting" % retries )
    sys.exit( 1 )


  def __serialize( self, taskObj ):
    try:
      result = self.serializeTask( taskObj )
    except Exception, excp:
      gLogger.exception( "Exception while serializing task %s" % taskId )
      return S_ERROR( "Cannot serialize task %s: %s" % ( taskId, str( excp ) ) )
    if not isReturnStructure( result ):
      raise Exception( "serializeTask does not return a return structure" )
    return result

  def __deserialize( self, taskStub ):
    try:
      result = self.deserializeTask( taskStub )
    except Exception, excp:
      gLogger.exception( "Exception while deserializing task %s" % taskId )
      return S_ERROR( "Cannot deserialize task %s: %s" % ( taskId, str( excp ) ) )
    if not isReturnStructure( result ):
      raise Exception( "deserializeTask does not return a return structure" )
    return result

  def __executeTask( self, taskId, taskStub ):
    self.__taskData.freezeTime = 0
    self.log.verbose( "Task %s: Received" % str( taskId ) )
    result = self.__deserialize( taskStub )
    if not result[ 'OK' ]:
      self.log.error( "Task %s: Cannot deserialize: %s" % ( str( taskId ), result[ 'Message' ] ) )
      return result
    taskObj = result[ 'Value' ]
    try:
      procResult = self.processTask( taskId, taskObj )
      if not isReturnStructure( procResult ):
        raise Exception( "processTask does not return a return structure" )
    except Exception, e:
      errMsg = "Error while processing task %s" % taskId
      gLogger.exception( errMsg )
      result = self.__msgClient.createMessage( "ExecutorError" )
      if not result[ 'OK' ]:
        return result
      msgObj = result[ 'Value' ]
      msgObj.taskId = taskId
      msgObj.errorMsg = "%s: %s" % ( errMsg, str( e ) )
      return self.__msgClient.sendMessage( msgObj )

    if procResult[ 'OK' ] and procResult[ 'Value' ]:
      taskObj = procResult[ 'Value' ]
    result = self.__serialize( taskObj )
    if not result[ 'OK' ]:
      self.log.verbose( "Task %s: Cannot serialize: %s" % ( str( taskId ), result[ 'Message' ] ) )
      return result
    taskStub = result[ 'Value' ]

    #Send message back

    if not procResult[ 'OK' ]:
      msgName = "TaskError"
    elif self.__taskData.freezeTime:
      msgName = "TaskFreeze"
    else:
      msgName = "TaskDone"

    result = self.__msgClient.createMessage( msgName )
    if not result[ 'OK' ]:
        return result
    self.log.verbose( "Task %s: Sending %s" % ( str( taskId ), msgName ) )
    msgObj = result[ 'Value' ]
    msgObj.taskId = taskId
    msgObj.taskStub = taskStub
    if not procResult[ 'OK' ]:
      msgObj.errorMsg = procResult[ 'Message' ]
    if self.__taskData.freezeTime:
      msgObj.freezeTime = self.__taskData.freezeTime
    return self.__msgClient.sendMessage( msgObj )


  ####
  # Callable functions
  ####  

  def freezeTask( self, freezeTime ):
    self.__taskData.freezeTime = freezeTime

  ####
  # Need to overwrite this functions
  ####

  def serializeTask( self, taskObj ):
    raise Exception( "Method serializeTask has to be coded!" )

  def deserializeTask( self, taskStub ):
    raise Exception( "Method deserializeTask has to be coded!" )

  def processTask( self, taskId, taskObj ):
    raise Exception( "Method processTask has to be coded!" )
