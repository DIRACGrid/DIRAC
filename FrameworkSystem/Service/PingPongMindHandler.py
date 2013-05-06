import types
import time
import random
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode, ThreadScheduler
from DIRAC.Core.Base.ExecutorMindHandler import ExecutorMindHandler
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.Client.JobState.CachedJobState import CachedJobState

random.seed()

class PingPongMindHandler( ExecutorMindHandler ):

  MSG_DEFINITIONS = { 'StartReaction' : { 'numBounces' : ( types.IntType, types.LongType ) } }

  auth_msg_StartReaction = [ 'all' ]
  def msg_StartReaction( self, msgObj ):
    bouncesLeft = msgObj.numBounces
    taskid = time.time() + random.random()
    taskData = { 'bouncesLeft' : bouncesLeft }
    return self.executeTask( time.time() + random.random(), taskData )

  @classmethod
  def exec_executorConnected( cls, trid, eTypes ):
    """
    This function will be called any time an executor reactor connects

    eTypes is a list of executor modules the reactor runs
    """
    return S_OK()

  @classmethod
  def exec_executorDisconnected( cls, trid ):
    """
    This function will be called any time an executor disconnects
    """
    return S_OK()

  @classmethod
  def exec_dispatch( cls, taskid, taskData, pathExecuted ):
    """
    Before a task can be executed, the mind has to know which executor module can process it
    """
    if taskData[ 'bouncesLeft' ] > 0:
      return S_OK( "Framework/PingPong" )
    return S_OK()

  @classmethod
  def exec_prepareToSend( cls, taskId, taskData, trid ):
    """
    """
    return S_OK()

  @classmethod
  def exec_serializeTask( cls, taskData ):
    return S_OK( jobState.serialize() )

  @classmethod
  def exec_deserializeTask( cls, taskStub ):
    return CachedJobState.deserialize( taskStub )

  @classmethod
  def exec_taskProcessed( cls, taskid, taskData, eType ):
    """
    This function will be called when a task has been processed and by which executor module
    """
    taskData[ 'bouncesLeft' ] -= 1
    return self.executeTask( taskid, taskData )

  @classmethod
  def exec_taskError( cls, taskid, taskData, errorMsg ):
    return jobState.setStatus( "Failed", errorMsg, source = 'OptimizationMindHandler' )

  @classmethod
  def exec_taskFreeze( cls, taskid, taskData, eType ):
    """
    A task can be frozen either because there are no executors connected that can handle it
     or becase an executor has requested it.
    """
    return S_OK()


