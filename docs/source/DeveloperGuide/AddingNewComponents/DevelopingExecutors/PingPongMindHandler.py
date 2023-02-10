""" Example of ExecutorMindHandler implementation
"""

import time
import random
from DIRAC import S_OK, gLogger
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Base.ExecutorMindHandler import ExecutorMindHandler

random.seed()

sLog = gLogger.getSubLogger(__name__)


class PingPongMindHandler(ExecutorMindHandler):

    MSG_DEFINITIONS = {"StartReaction": {"numBounces": int}}

    auth_msg_StartReaction = ["all"]

    def msg_StartReaction(self, msgObj):
        bouncesLeft = msgObj.numBounces
        taskData = {"bouncesLeft": bouncesLeft}
        return self.executeTask(time.time() + random.random(), taskData)

    auth_startPingOfDeath = ["all"]
    types_startPingOfDeath = [int]

    def export_startPingOfDeath(self, numBounces):
        taskData = {"bouncesLeft": numBounces}
        sLog.info("START TASK", f"{taskData}")
        return self.executeTask(int(time.time() + random.random()), taskData)

    @classmethod
    def exec_executorConnected(cls, trid, eTypes):
        """
        This function will be called any time an executor reactor connects

        eTypes is a list of executor modules the reactor runs
        """
        sLog.info("EXECUTOR CONNECTED OF TYPE", f"{eTypes}")
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
        sLog.info("IN DISPATCH", f"{taskData}")
        if taskData["bouncesLeft"] > 0:
            sLog.info("SEND TO PLACE")
            return S_OK("Test/PingPongExecutor")
        return S_OK()

    @classmethod
    def exec_prepareToSend(cls, taskId, taskData, trid):
        """ """
        return S_OK()

    @classmethod
    def exec_serializeTask(cls, taskData):
        sLog.info("SERIALIZE", f"{taskData}")
        return S_OK(DEncode.encode(taskData))

    @classmethod
    def exec_deserializeTask(cls, taskStub):
        sLog.info("DESERIALIZE", f"{taskStub}")
        return S_OK(DEncode.decode(taskStub)[0])

    @classmethod
    def exec_taskProcessed(cls, taskid, taskData, eType):
        """
        This function will be called when a task has been processed and by which executor module
        """
        sLog.info("PROCESSED", f"{taskData}")
        taskData["bouncesLeft"] -= 1
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
