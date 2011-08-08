
import types
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR, isReturnStructure
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ExecutorDispatcher import ExecutorDispatcher, ExecutorDispatcherCallbacks


class ExecutorMindHandler( RequestHandler ):

  MSG_DEFINITIONS = { 'ProcessTask' : { 'taskId' : types.LongType,
                                        'taskStub' : types.StringType },
                      'TaskDone' : { 'taskId' : types.LongType,
                                     'taskStub' : types.StringType },
                      'TaskError' : { 'taskId': types.LongType,
                                      'errorMsg' : types.StringType },
                      'ExecutorError' : { 'taskId': types.LongType,
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
  def initialize( self, serviceInfoDict ):
    self.__eDispatch = ExecutorDispatcher()
    self.__callbacks = ExecutorMindHandler.MindCallbacks( self.__sendTask,
                                                          self.exec_dispatch,
                                                          self.exec_disconnectExecutor,
                                                          self.exec_taskError )
    self.__eDispatch.setCallbacks( self.__callbacks )

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
    msgObj = self.srv_msgCreate( "ProcessTask" )
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

  auth_msg_taskDone = [ 'all' ]
  def msg_taskDone( self, msgObj ):
    try:
      result = self.exec_deserializeTask( taskObj )
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

  def exec_disconnectExecutor( self, trid ):
    return self.srv_msgDisconnectClient( trid )

  ########
  #  Methods to be used by the real services
  ########

  def executeTask( self, taskId, taskObj ):
    return self.__eDispatch.addTask( taskId, taskObj )

  ########
  #  Methods that need to be overwritten
  ########

  def exec_dispatch( self, taskId, taskObj ):
    raise Exception( "No exec_dispatch defined!!" )

  def exec_serializeTask( self, taskId, taskObj ):
    raise Exception( "No exec_serializeTask defined!!" )

  def exec_deserializeTask( self, taskStub ):
    raise Exception( "No exec_deserializeTask defined!!" )

  def exec_taskError( self, taskId, errorMsg ):
    raise Exception( "No exec_taskError defined!!" )


