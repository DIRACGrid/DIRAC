
import types
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR, isReturnStructure
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ExecutorDispatcher import ExecutorDispatcher, ExecutorDispatcherCallbacks


class ExecutorMindHandler( RequestHandler ):

  MSG_DEFINITIONS = { 'ProcessTask' : { 'taskId' : ( types.IntType, types.LongType ),
                                        'taskStub' : types.StringType,
                                        'eType' : types.StringType },
                      'TaskDone' : { 'taskId' : ( types.IntType, types.LongType ),
                                     'taskStub' : types.StringType },
                      'TaskFreeze' : { 'taskId' : ( types.IntType, types.LongType ),
                                       'taskStub' : types.StringType,
                                       'freezeTime' : ( types.IntType, types.LongType ) },
                      'TaskError' : { 'taskId': ( types.IntType, types.LongType ),
                                      'errorMsg' : types.StringType,
                                      'taskStub' : types.StringType },
                      'ExecutorError' : { 'taskId': ( types.IntType, types.LongType ),
                                          'errorMsg' : types.StringType } }

  class MindCallbacks( ExecutorDispatcherCallbacks ):

    def __init__( self, sendTaskCB, dispatchCB, disconnectCB, taskProcCB, taskFreezeCB, taskErrCB ):
      self.__sendTaskCB = sendTaskCB
      self.__dispatchCB = dispatchCB
      self.__disconnectCB = disconnectCB
      self.__taskProcDB = taskProcCB
      self.__taskFreezeCB = taskFreezeCB
      self.__taskErrCB = taskErrCB
      self.__allowedClients = []

    def cbSendTask( self, taskId, taskObj, eId, eType ):
      return self.__sendTaskCB(  taskId, taskObj, eId, eType )

    def cbDispatch( self, taskId, taskObj, pathExecuted ):
      return self.__dispatchCB( taskId, taskObj, pathExecuted )

    def cbDisconectExecutor( self, eId ):
      return self.__disconnectCB( eId )

    def cbTaskError( self, taskId, taskObj, errorMsg ):
      return self.__taskErrCB( taskId, taskObj, errorMsg )

    def cbTaskProcessed( self, taskId, taskObj, eType ):
      return self.__taskProcDB( taskId, taskObj, eType )

    def cbTaskFreeze( self, taskId, taskObj, eType ):
      return self.__taskFreezeCB( taskId, taskObj, eType )

  ###
  # End of callbacks
  ###

  @classmethod
  def initializeHandler( cls, serviceInfoDict ):
    gLogger.notice( "Initializing Executor dispatcher" )
    cls.__eDispatch = ExecutorDispatcher()
    cls.__callbacks = ExecutorMindHandler.MindCallbacks( cls.__sendTask,
                                                         cls.exec_dispatch,
                                                         cls.__execDisconnected,
                                                         cls.exec_taskProcessed,
                                                         cls.exec_taskFreeze,
                                                         cls.exec_taskError )
    cls.__eDispatch.setCallbacks( cls.__callbacks )
    cls.__allowedClients = []

  @classmethod
  def setAllowedClients( cls, aClients ):
    if type( aClients ) not in ( types.ListType, types.TupleType ):
      aClients = ( aClients, )
    cls.__allowedClients = aClients

  @classmethod
  def __sendTask( self, taskId, taskObj, eId, eType ):
    try:
      result = self.exec_prepareToSend( taskId, taskObj, eId )
      if not result[ 'OK' ]:
        return result
    except Exception, excp:
      gLogger.exception( "Exception while executing prepareToSend: %s" % str( excp ) )
      return S_ERROR( "Cannot presend task" )
    try:
      result = self.exec_serializeTask( taskObj )
    except Exception, excp:
      gLogger.exception( "Exception while serializing task %s" % taskId )
      return S_ERROR( "Cannot serialize task %s: %s" % ( taskId, str( excp ) ) )
    if not isReturnStructure( result ):
      raise Exception( "exec_serializeTask does not return a return structure" )
    if not result[ 'OK' ]:
      return result
    taskStub = result[ 'Value' ]
    result = self.srv_msgCreate( "ProcessTask" )
    if not result[ 'OK' ]:
      return result
    msgObj = result[ 'Value' ]
    msgObj.taskId = taskId
    msgObj.taskStub = taskStub
    msgObj.eType = eType
    return self.srv_msgSend( eId, msgObj )

  @classmethod
  def __execDisconnected( cls, trid ):
    result = cls.srv_disconnectClient( trid )
    if not result[ 'OK' ]:
      return result
    return cls.exec_executorDisconnected( trid )

  auth_conn_new = [ 'all' ]
  def conn_new( self, trid, identity, kwargs ):
    if 'executorTypes' in kwargs and kwargs[ 'executorTypes' ]:
      return S_OK()
    for aClient in self.__allowedClients:
      if aClient in kwargs and kwargs[ aClient ]:
        return S_OK()
    return S_ERROR( "Only executors are allowed to connect" )

  auth_conn_connected = [ 'all' ]
  def conn_connected( self, trid, identity, kwargs ):
    for aClient in self.__allowedClients:
      if aClient in kwargs and kwargs[ aClient ]:
        return S_OK()
    try:
      numTasks = max( 1, int( kwargs[ 'maxTasks' ] ) )
    except:
      numTasks = 1
    self.__eDispatch.addExecutor( trid, kwargs[ 'executorTypes' ] )
    return self.exec_executorConnected( trid, kwargs[ 'executorTypes' ] )

  auth_conn_drop = [ 'all' ]
  def conn_drop( self, trid ):
    self.__eDispatch.removeExecutor( trid )
    return S_OK()

  auth_msg_TaskDone = [ 'all' ]
  def msg_TaskDone( self, msgObj ):
    taskId = msgObj.taskId
    try:
      result = self.exec_deserializeTask( msgObj.taskStub )
    except Exception, excp:
      gLogger.exception( "Exception while deserializing task %s" % taskId )
      return S_ERROR( "Cannot deserialize task %s: %s" % ( taskId, str( excp ) ) )
    if not isReturnStructure( result ):
      raise Exception( "exec_deserializeTask does not return a return structure" )
    if not result[ 'OK' ]:
      return result
    taskObj = result[ 'Value' ]
    return self.__eDispatch.taskProcessed( self.srv_getTransportID(), msgObj.taskId, taskObj )

  auth_msg_TaskFreeze = [ 'all' ]
  def msg_TaskFreeze( self, msgObj ):
    taskId = msgObj.taskId
    try:
      result = self.exec_deserializeTask( msgObj.taskStub )
    except Exception, excp:
      gLogger.exception( "Exception while deserializing task %s" % taskId )
      return S_ERROR( "Cannot deserialize task %s: %s" % ( taskId, str( excp ) ) )
    if not isReturnStructure( result ):
      raise Exception( "exec_deserializeTask does not return a return structure" )
    if not result[ 'OK' ]:
      return result
    taskObj = result[ 'Value' ]
    return self.__eDispatch.freezeTask( self.srv_getTransportID(), msgObj.taskId,
                                        msgObj.freezeTime, taskObj )

  auth_msg_TaskError = [ 'all' ]
  def msg_TaskError( self, msgObj ):
    try:
      result = self.exec_deserializeTask( msgObj.taskStub )
    except Exception, excp:
      gLogger.exception( "Exception while deserializing task %s" % taskId )
      return S_ERROR( "Cannot deserialize task %s: %s" % ( taskId, str( excp ) ) )
    if not isReturnStructure( result ):
      raise Exception( "exec_deserializeTask does not return a return structure" )
    if not result[ 'OK' ]:
      return result
    taskObj = result[ 'Value' ]
    #TODO: Check the executor has privileges over the task
    self.__eDispatch.removeTask( msgObj.taskId )
    try:
      self.exec_taskError( msgObj.taskId, taskObj, msgObj.errorMsg )
    except:
      gLogger.exception( "Exception when processing task %s" % msgObj.taskId )
    return S_OK()

  auth_msg_ExecutorError = [ 'all' ]
  def msg_ExecutorError( self, msgObj ):
    gLogger.info( "Disconnecting executor by error: %s" % msgObj.errorMsg )
    self.__eDispatch.removeExecutor( self.srv_getTransportID() )
    return self.srv_disconnect()

  #######
  # Utilities functions
  #######

  @classmethod
  def getTaskIds( cls ):
    return cls.__eDispatch.getTaskIds()

  @classmethod
  def getExecutorsConnected( cls ):
    return cls.__eDispatch.getExecutorsConnected()

  @classmethod
  def setFailedOnTooFrozen( cls, value ):
    #If a task is frozen too many times, send error or forget task?
    cls.__eDispatch.setFailedOnTooFrozen( value )

  @classmethod
  def setFreezeOnFailedDispatch( cls, value ):
    #If a task fails to properly dispatch, freeze it?
    cls.__eDispatch.setFreezeOnFailedDispatch( value )

  @classmethod
  def setFreezeOnUnknownExecutor( cls, value ):
    #If a task needs to go to an executor that has not connected. Forget the task?
    cls.__eDispatch.setFreezeOnUnknownExecutor( value )

  #######
  # Methods that can be overwritten
  #######

  @classmethod
  def exec_executorDisconnected( cls, trid ):
    return S_OK()

  @classmethod
  def exec_executorConnected( cls, execName, trid ):
    return S_OK()

  @classmethod
  def exec_prepareToSend( cls, taskId, taskObj, eId ):
    return S_OK()

  ########
  #  Methods to be used by the real services
  ########

  @classmethod
  def executeTask( cls, taskId, taskObj ):
    return cls.__eDispatch.addTask( taskId, taskObj )

  @classmethod
  def forgetTask( cls, taskId ):
    return cls.__eDispatch.removeTask( taskId )

  ########
  #  Methods that need to be overwritten
  ########

  @classmethod
  def exec_dispatch( cls, taskId, taskObj, pathExecuted ):
    raise Exception( "No exec_dispatch defined or it is not a classmethod!!" )

  @classmethod
  def exec_serializeTask( cls, taskObj ):
    raise Exception( "No exec_serializeTask defined or it is not a classmethod!!" )

  @classmethod
  def exec_deserializeTask( cls, taskStub ):
    raise Exception( "No exec_deserializeTask defined or it is not a classmethod!!" )

  @classmethod
  def exec_taskError( cls, taskId, taskObj, errorMsg ):
    raise Exception( "No exec_taskError defined or it is not a classmethod!!" )

  @classmethod
  def exec_taskProcessed( cls, taskId, taskObj, eType ):
    raise Exception( "No exec_taskProcessed defined or it is not a classmethod!!" )

  @classmethod
  def exec_taskFreeze( cls, taskId, taskObj, eType ):
    return S_OK()

