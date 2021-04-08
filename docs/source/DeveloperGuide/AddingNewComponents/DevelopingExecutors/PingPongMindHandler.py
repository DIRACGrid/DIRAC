""" Example of ExecutorMindHandler implementation
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import six
import time
import random
from DIRAC import S_OK, gLogger
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Base.ExecutorMindHandler import ExecutorMindHandler


class PingPongMindHandler(ExecutorMindHandler):

  MSG_DEFINITIONS = {'StartReaction': {'numBounces': six.integer_types}}

  auth_msg_StartReaction = ['all']

  def msg_StartReaction(self, msgObj):
    bouncesLeft = msgObj.numBounces
    taskid = time.time() + random.random()
    taskData = {'bouncesLeft': bouncesLeft}
    return self.executeTask(time.time() + random.random(), taskData)

  auth_startPingOfDeath = ['all']
  types_startPingOfDeath = [int]

  def export_startPingOfDeath(self, numBounces):
    taskData = {'bouncesLeft': numBounces}
    gLogger.info("START TASK = %s" % taskData)
    return self.executeTask(int(time.time() + random.random()), taskData)

  @classmethod
  def exec_executorConnected(cls, trid, eTypes):
    """
    This function will be called any time an executor reactor connects

    eTypes is a list of executor modules the reactor runs
    """
    gLogger.info("EXECUTOR CONNECTED OF TYPE %s" % eTypes)
    return S_OK()

  @classmethod
  def exec_executorDisconnected(cls, trid):
    """
    This function will be called any time an executor disconnects
    """
    return S_OK()

  @classmethod
  def exec_dispatch(cls, taskid, taskData, pathExecuted):
    """
    Before a task can be executed, the mind has to know which executor module can process it
    """
    gLogger.info("IN DISPATCH %s" % taskData)
    if taskData['bouncesLeft'] > 0:
      gLogger.info("SEND TO PLACE")
      return S_OK("Test/PingPongExecutor")
    return S_OK()

  @classmethod
  def exec_prepareToSend(cls, taskId, taskData, trid):
    """
    """
    return S_OK()

  @classmethod
  def exec_serializeTask(cls, taskData):
    gLogger.info("SERIALIZE %s" % taskData)
    return S_OK(DEncode.encode(taskData))

  @classmethod
  def exec_deserializeTask(cls, taskStub):
    gLogger.info("DESERIALIZE %s" % taskStub)
    return S_OK(DEncode.decode(taskStub)[0])

  @classmethod
  def exec_taskProcessed(cls, taskid, taskData, eType):
    """
    This function will be called when a task has been processed and by which executor module
    """
    gLogger.info("PROCESSED %s" % taskData)
    taskData['bouncesLeft'] -= 1
    return cls.executeTask(taskid, taskData)

  @classmethod
  def exec_taskError(cls, taskid, taskData, errorMsg):
    print("OOOOOO THERE WAS AN ERROR!!", errorMsg)
    return S_OK()

  @classmethod
  def exec_taskFreeze(cls, taskid, taskData, eType):
    """
    A task can be frozen either because there are no executors connected that can handle it
     or becase an executor has requested it.
    """
    print("OOOOOO THERE WAS A TASK FROZEN")
    return S_OK()
