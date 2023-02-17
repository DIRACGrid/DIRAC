""" The mind is a service the distributes "task" to executors
"""
import pprint

from DIRAC import gLogger
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR, isReturnStructure
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ExecutorDispatcher import ExecutorDispatcher, ExecutorDispatcherCallbacks


class ExecutorMindHandler(RequestHandler):
    MSG_DEFINITIONS = {
        "ProcessTask": {"taskId": int, "taskStub": str, "eType": str},
        "TaskDone": {"taskId": int, "taskStub": str},
        "TaskFreeze": {
            "taskId": (int, str),
            "taskStub": str,
            "freezeTime": int,
        },
        "TaskError": {
            "taskId": int,
            "errorMsg": str,
            "taskStub": str,
            "eType": str,
        },
        "ExecutorError": {"taskId": int, "errorMsg": str, "eType": str},
    }

    class MindCallbacks(ExecutorDispatcherCallbacks):
        def __init__(self, sendTaskCB, dispatchCB, disconnectCB, taskProcCB, taskFreezeCB, taskErrCB):
            self.__sendTaskCB = sendTaskCB
            self.__dispatchCB = dispatchCB
            self.__disconnectCB = disconnectCB
            self.__taskProcDB = taskProcCB
            self.__taskFreezeCB = taskFreezeCB
            self.__taskErrCB = taskErrCB
            self.__allowedClients = []

        def cbSendTask(self, taskId, taskObj, eId, eType):
            return self.__sendTaskCB(taskId, taskObj, eId, eType)

        def cbDispatch(self, taskId, taskObj, pathExecuted):
            return self.__dispatchCB(taskId, taskObj, pathExecuted)

        def cbDisconectExecutor(self, eId):
            return self.__disconnectCB(eId)

        def cbTaskError(self, taskId, taskObj, errorMsg):
            return self.__taskErrCB(taskId, taskObj, errorMsg)

        def cbTaskProcessed(self, taskId, taskObj, eType):
            return self.__taskProcDB(taskId, taskObj, eType)

        def cbTaskFreeze(self, taskId, taskObj, eType):
            return self.__taskFreezeCB(taskId, taskObj, eType)

    ###
    # End of callbacks
    ###

    @classmethod
    def initializeHandler(cls, serviceInfoDict):
        gLogger.notice("Initializing Executor dispatcher")
        cls.__eDispatch = ExecutorDispatcher(cls.srv_getMonitor())
        cls.__callbacks = ExecutorMindHandler.MindCallbacks(
            cls.__sendTask,
            cls.exec_dispatch,
            cls.__execDisconnected,
            cls.exec_taskProcessed,
            cls.exec_taskFreeze,
            cls.exec_taskError,
        )
        cls.__eDispatch.setCallbacks(cls.__callbacks)
        cls.__allowedClients = []
        if cls.log.shown("VERBOSE"):
            gThreadScheduler.setMinValidPeriod(1)
            gThreadScheduler.addPeriodicTask(
                10,
                lambda: cls.log.verbose(
                    "== Internal state ==",
                    f"\n{pprint.pformat(cls.__eDispatch._internals())}\n===========",
                ),
            )
        return S_OK()

    @classmethod
    def setAllowedClients(cls, aClients):
        if not isinstance(aClients, (list, tuple)):
            aClients = (aClients,)
        cls.__allowedClients = aClients

    @classmethod
    def __sendTask(self, taskId, taskObj, eId, eType):
        try:
            result = self.exec_prepareToSend(taskId, taskObj, eId)
            if not result["OK"]:
                return result
        except Exception as excp:
            gLogger.exception("Exception while executing prepareToSend:", taskId, lException=excp)
            return S_ERROR("Cannot presend task")
        try:
            result = self.exec_serializeTask(taskObj)
        except Exception as excp:
            gLogger.exception("Exception while serializing task", taskId, lException=excp)
            return S_ERROR(f"Cannot serialize task {taskId}: {str(excp)}")
        if not isReturnStructure(result):
            raise Exception("exec_serializeTask does not return a return structure")
        if not result["OK"]:
            return result
        taskStub = result["Value"]
        result = self.srv_msgCreate("ProcessTask")
        if not result["OK"]:
            return result
        msgObj = result["Value"]
        msgObj.taskId = taskId
        msgObj.taskStub = taskStub
        msgObj.eType = eType
        return self.srv_msgSend(eId, msgObj)

    @classmethod
    def __execDisconnected(cls, trid):
        result = cls.srv_disconnectClient(trid)
        if not result["OK"]:
            return result
        return cls.exec_executorDisconnected(trid)

    auth_conn_new = ["all"]

    def conn_new(self, trid, identity, kwargs):
        if "executorTypes" in kwargs and kwargs["executorTypes"]:
            return S_OK()
        for aClient in self.__allowedClients:
            if aClient in kwargs and kwargs[aClient]:
                return S_OK()
        return S_ERROR("Only executors are allowed to connect")

    auth_conn_connected = ["all"]

    def conn_connected(self, trid, identity, kwargs):
        for aClient in self.__allowedClients:
            if aClient in kwargs and kwargs[aClient]:
                return S_OK()
        try:
            numTasks = max(1, int(kwargs["maxTasks"]))
        except Exception:
            numTasks = 1
        self.__eDispatch.addExecutor(trid, kwargs["executorTypes"])
        return self.exec_executorConnected(trid, kwargs["executorTypes"])

    auth_conn_drop = ["all"]

    def conn_drop(self, trid):
        gLogger.warn("Triggered conn_drop for", trid)
        self.__eDispatch.removeExecutor(trid)
        return self.srv_disconnect(trid)

    auth_msg_TaskDone = ["all"]

    def msg_TaskDone(self, msgObj):
        taskId = msgObj.taskId
        try:
            result = self.exec_deserializeTask(msgObj.taskStub)
        except Exception as excp:
            gLogger.exception("Exception while deserializing task", taskId, lException=excp)
            return S_ERROR(f"Cannot deserialize task {taskId}: {str(excp)}")
        if not isReturnStructure(result):
            raise Exception("exec_deserializeTask does not return a return structure")
        if not result["OK"]:
            return result
        taskObj = result["Value"]
        result = self.__eDispatch.taskProcessed(self.srv_getTransportID(), msgObj.taskId, taskObj)
        if not result["OK"]:
            gLogger.error("There was a problem processing task", f"{taskId}: {result['Message']}")
        return S_OK()

    auth_msg_TaskFreeze = ["all"]

    def msg_TaskFreeze(self, msgObj):
        taskId = msgObj.taskId
        try:
            result = self.exec_deserializeTask(msgObj.taskStub)
        except Exception as excp:
            gLogger.exception("Exception while deserializing task", taskId, lException=excp)
            return S_ERROR(f"Cannot deserialize task {taskId}: {str(excp)}")
        if not isReturnStructure(result):
            raise Exception("exec_deserializeTask does not return a return structure")
        if not result["OK"]:
            return result
        taskObj = result["Value"]
        result = self.__eDispatch.freezeTask(self.srv_getTransportID(), msgObj.taskId, msgObj.freezeTime, taskObj)
        if not result["OK"]:
            gLogger.error("There was a problem freezing task", f"{taskId}: {result['Message']}")
        return S_OK()

    auth_msg_TaskError = ["all"]

    def msg_TaskError(self, msgObj):
        taskId = msgObj.taskId
        try:
            result = self.exec_deserializeTask(msgObj.taskStub)
        except Exception as excp:
            gLogger.exception("Exception while deserializing task", taskId, lException=excp)
            return S_ERROR(f"Cannot deserialize task {taskId}: {str(excp)}")
        if not isReturnStructure(result):
            raise Exception("exec_deserializeTask does not return a return structure")
        if not result["OK"]:
            return result
        taskObj = result["Value"]
        # TODO: Check the executor has privileges over the task
        self.__eDispatch.removeTask(msgObj.taskId)
        try:
            self.exec_taskError(msgObj.taskId, taskObj, msgObj.errorMsg)
        except Exception as excp:
            gLogger.exception("Exception when processing task", msgObj.taskId, lException=excp)
        return S_OK()

    auth_msg_ExecutorError = ["all"]

    def msg_ExecutorError(self, msgObj):
        gLogger.info("Disconnecting executor by error:", msgObj.errorMsg)
        self.__eDispatch.removeExecutor(self.srv_getTransportID())
        return self.srv_disconnect()

    #######
    # Utilities functions
    #######

    @classmethod
    def getTaskIds(cls):
        return cls.__eDispatch.getTaskIds()

    @classmethod
    def getExecutorsConnected(cls):
        return cls.__eDispatch.getExecutorsConnected()

    @classmethod
    def setFailedOnTooFrozen(cls, value):
        # If a task is frozen too many times, send error or forget task?
        cls.__eDispatch.setFailedOnTooFrozen(value)

    @classmethod
    def setFreezeOnFailedDispatch(cls, value):
        # If a task fails to properly dispatch, freeze it?
        cls.__eDispatch.setFreezeOnFailedDispatch(value)

    @classmethod
    def setFreezeOnUnknownExecutor(cls, value):
        # If a task needs to go to an executor that has not connected. Forget the task?
        cls.__eDispatch.setFreezeOnUnknownExecutor(value)

    #######
    # Methods that can be overwritten
    #######

    @classmethod
    def exec_executorDisconnected(cls, trid):
        return S_OK()

    @classmethod
    def exec_executorConnected(cls, execName, trid):
        return S_OK()

    @classmethod
    def exec_prepareToSend(cls, taskId, taskObj, eId):
        return S_OK()

    ########
    #  Methods to be used by the real services
    ########

    @classmethod
    def executeTask(cls, taskId, taskObj):
        return cls.__eDispatch.addTask(taskId, taskObj)

    @classmethod
    def forgetTask(cls, taskId):
        return cls.__eDispatch.removeTask(taskId)

    ########
    #  Methods that need to be overwritten
    ########

    @classmethod
    def exec_dispatch(cls, taskId, taskObj, pathExecuted):
        raise Exception("No exec_dispatch defined or it is not a classmethod!!")

    @classmethod
    def exec_serializeTask(cls, taskObj):
        raise Exception("No exec_serializeTask defined or it is not a classmethod!!")

    @classmethod
    def exec_deserializeTask(cls, taskStub):
        raise Exception("No exec_deserializeTask defined or it is not a classmethod!!")

    @classmethod
    def exec_taskError(cls, taskId, taskObj, errorMsg):
        raise Exception("No exec_taskError defined or it is not a classmethod!!")

    @classmethod
    def exec_taskProcessed(cls, taskId, taskObj, eType):
        raise Exception("No exec_taskProcessed defined or it is not a classmethod!!")

    @classmethod
    def exec_taskFreeze(cls, taskId, taskObj, eType):
        return S_OK()
