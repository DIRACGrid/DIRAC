
import types
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR, isReturnStructure
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ExecutorDispatcher import ExecutorDispatcher, ExecutorDispatcherCallbacks


class ExecutorMindHandler( RequestHandler ):

  MSG_DEFINITIONS = { 'ProcessTask' : { 'taskId' : ( types.IntType, types.LongType ),
                                        'taskStub' : types.StringType },
                      'TaskDone' : { 'taskId' : ( types.IntType, types.LongType ),
                                     'taskStub' : types.StringType },
                      'TaskError' : { 'taskId': ( types.IntType, types.LongType ),
                                      'errorMsg' : types.StringType },
                      'ExecutorError' : { 'taskId': ( types.IntType, types.LongType ),
                                          'errorMsg' : types.StringType } }

  class MindCallbacks( ExecutorDispatcherCallbacks ):

    def __init__( self, sendTaskCB, dispatchCB, disconnectCB, taskErrCB ):
      self.__sendTaskCB = sendTaskCB
      self.__dispatchCB = dispatchCB
      self.__disconnectCB = disconnectCB
      self.__taskErrCB = taskErrCB


    def cbSendTask( self, eId, taskId, taskObj ):
      return self.__sendTaskCB( eId, taskId, taskObj )

    def cbDispatch( self, taskId, taskObj ):
      return self.__dispatchCB( taskId, taskObj )

    def cbDisconectExecutor( self, eId ):
      return self.__disconnectCB( eId )

    def cbTaskError( self, taskId, errorMsg ):
      return self.__taskErrCB( taskId, errorMsg )

  ###
  # End of callbacks
  ###

  @classmethod
  def initialize( cls, serviceInfoDict ):
    gLogger.notice( "Initializing Executor dispatcher" )
    cls.__eDispatch = ExecutorDispatcher()
    cls.__callbacks = ExecutorMindHandler.MindCallbacks( cls.__sendTask,
                                                         cls.exec_dispatch,
                                                         cls.exec_disconnectExecutor,
                                                         cls.exec_taskError )
    cls.__eDispatch.setCallbacks( cls.__callbacks )

  @classmethod
  def __sendTask( self, eId, taskId, taskObj ):
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
    return self.srv_msgSend( eId, msgObj )

  auth_conn_new = [ 'all' ]
  def conn_new( self, trid, identity, kwargs ):
    if 'executorName' not in kwargs or not kwargs[ 'executorName']:
      return S_ERROR( "Only executors are allowed to connect" )
    return S_OK()

  auth_conn_connected = [ 'all' ]
  def conn_connected( self, trid, identity, kwargs ):
    try:
      numTasks = max( 1, int( kwargs[ 'maxTasks' ] ) )
    except:
      numTasks = 1
    self.__eDispatch.addExecutor( kwargs[ 'executorName' ], trid )
    return S_OK()

  auth_conn_drop = [ 'all' ]
  def conn_drop( self, trid ):
    self.__eDispatch.removeExecutor( trid )
    return S_OK()


  auth_msg_taskDone = [ 'all' ]
  def msg_taskDone( self, msgObj ):
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
    return self.__eDispatch.taskProcessed( self.srv_getTransportID(), taskObj.taskId, taskObj )

  auth_msg_taskError = [ 'all' ]
  def msg_taskError( self, msgObj ):
    #TODO: Check the executor has privileges over the task
    self.__eDispatch.removeTask( msgObj.taskId )
    try:
      self.exec_taskError( msgObj.taskId, msgObj.errorMsg )
    except:
      gLogger.exception( "Exception when processing task %s" % msgObj.taskId )
    return S_OK()

  auth_msg_executorError = [ 'all' ]
  def msg_executorError( self, msgObj ):
    #TODO: Check the executor has privileges over the task
    self.__eDispatch.removeTask( msgObj.taskId )
    try:
      self.exec_taskError( msgObj.taskId, msgObj.errorMsg )
    except:
      gLogger.exception( "Exception when processing task %s" % msgObj.taskId )
    return S_OK()

  #######
  # Methods that can be overwritten
  #######

  @classmethod
  def exec_disconnectExecutor( self, trid ):
    return self.srv_msgDisconnectClient( trid )

  ########
  #  Methods to be used by the real services
  ########

  @classmethod
  def executeTask( self, taskId, taskObj ):
    return self.__eDispatch.addTask( taskId, taskObj )

  ########
  #  Methods that need to be overwritten
  ########

  @classmethod
  def exec_dispatch( self, taskId, taskObj ):
    raise Exception( "No exec_dispatch defined or it is not a classmethod!!" )

  @classmethod
  def exec_serializeTask( self, taskObj ):
    raise Exception( "No exec_serializeTask defined or it is not a classmethod!!" )

  @classmethod
  def exec_deserializeTask( self, taskStub ):
    raise Exception( "No exec_deserializeTask defined or it is not a classmethod!!" )

  @classmethod
  def exec_taskError( self, taskId, errorMsg ):
    raise Exception( "No exec_taskError defined or it is not a classmethod!!" )


