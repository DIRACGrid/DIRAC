
from DIRAC.Core.DISET.MessageClient import MessageClient
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC.Core.Utilities.ProcessPool import ProcessPool

class Executor( AgentModule ):

  def connect( self, mindName, maxTasks = 1, useProcesses = False ):
    self.am_setOption( "PollingTime", 300 )
    self.__maxTasks = maxTasks
    self.__msgClient = MessageClient( mindName )
    self.__msgClient.subscribeToMessage( 'ProcessTask', self.__queueTask )
    self.__msgClient.subscribeToDisconnect( self.__disconnected )
    if maxTasks > 1:
      if useProcesses:
        self.__pool = ProcessPool( maxTasks )
      else:
        self.__pool = ThreadPool( maxTasks )
    return self.__msgClient.connect( executorName = self.am_getModuleParam( 'fullName' ),
                                     maxTasks = maxTasks )

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
      return self.__pool.createAndQueueTask( self.__executeTask, ( msgObj.taskId, msgObj.taskStub ) )
    return self.__executeTask( self, taskId, taskStub )

  def __disconnected( self ):
    pass


  def __executeTask( self, taskId, taskStub ):
    try:
      result = self.deserializeTask( taskObj )
    except Exception, excp:
      gLogger.exception( "Exception while deserializing task %s" % taskId )
      return S_ERROR( "Cannot deserialize task %s: %s" % ( taskId, str( excp ) ) )
    if not isReturnStructure( result ):
      raise Exception( "deserializeTask does not return a return structure" )
    if not result[ 'OK' ]:
      return result
    taskObj = result[ 'Value' ]
    try:
      result = self.processTask( taskObj )
      if not isReturnStructure( result ):
        raise Exception( "processTask does not return a return structure" )
    except:
      errMsg = "Error while processing task %s" % taskId
      gLogger.exception( errMsg )
      msgObj = self.__msgClient.createMessage( "ExecutorError" )
      msgObj.taskId = taskId
      msgObj.errorMsg = result[ 'Message' ]
      return self.__msgClient.sendMessage( msgObj )

    if result[ 'Error' ]:
      msgObj = self.__msgClient.createMessage( "TaskError" )
      msgObj.taskId = taskId
      msgObj.errorMsg = result[ 'Message' ]
      return self.__msgClient.sendMessage( msgObj )

    try:
      result = self.serializeTask( taskObj )
    except Exception, excp:
      gLogger.exception( "Exception while serializing task %s" % taskId )
      return S_ERROR( "Cannot serialize task %s: %s" % ( taskId, str( excp ) ) )
    if not isReturnStructure( result ):
      raise Exception( "serializeTask does not return a return structure" )
    if not result[ 'OK' ]:
      return result
    taskStub = result[ 'Value' ]
    msgObj = self.__msgClient.createMessage( "TaskDone" )
    msgObj.taskId = taskId
    msgObj.taskStub = taskStub
    return self.__msgClient.sendMessage( msgObj )



  ####
  # Need to overwrite this functions
  ####

  def serializeTask( self, taskObj ):
    raise Exception( "Method serialize has to be coded!" )

  def deserializeTask( self, taskStub ):
    raise Exception( "Method deserialize has to be coded!" )

  def processTask( self, taskObj ):
    raise Exception( "Method processTask has to be coded!" )
