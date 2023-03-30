########################################################################
# File :   ExecutorReactor.py
# Author : Adria Casajus
########################################################################
"""
  DIRAC class to execute Executors

  Executors are an active part of DIRAC.

  All DIRAC executors must inherit from the basic class ExecutorModule

  In the most common case, DIRAC Executors are executed using the dirac-executor command.
  dirac-execuot accepts a list positional arguments.

  dirac-executo then:
  - produces a instance of ExecutorReactor

  Executor modules must be placed under the Executor directory of a DIRAC System.
  DIRAC Systems are called XXXSystem where XXX is the [DIRAC System Name], and
  must inherit from the base class ExecutorModule

"""
import time
import threading
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.MessageClient import MessageClient
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.private.ModuleLoader import ModuleLoader


class ExecutorReactor:
    class AliveLock:
        def __init__(self):
            self.__alive = 0
            self.__cond = threading.Condition(threading.Lock())

        def alive(self):
            self.__cond.acquire()
            self.__alive += 1
            self.__cond.release()

        def dead(self):
            self.__cond.acquire()
            self.__alive -= 1
            self.__cond.notify()
            self.__cond.release()

        def lockUntilAllDead(self):
            self.__cond.acquire()
            while True:
                if self.__alive < 1:
                    break
                self.__cond.wait(1)
            self.__cond.release()

    class MindCluster:
        def __init__(self, mindName, aliveLock):
            self.__mindName = mindName
            self.__modules = {}
            self.__maxTasks = 1
            self.__reconnectSleep = 1
            self.__reconnectRetries = 10
            self.__extraArgs = {}
            self.__instances = {}
            self.__instanceLock = threading.Lock()
            self.__aliveLock = aliveLock

        def updateMaxTasks(self, mt):
            self.__maxTasks = max(self.__maxTasks, mt)

        def addModule(self, name, exeClass):
            self.__modules[name] = exeClass
            self.__maxTasks = max(self.__maxTasks, exeClass.ex_getOption("MaxTasks", 0))
            self.__reconnectSleep = max(self.__reconnectSleep, exeClass.ex_getOption("ReconnectSleep", 0))
            self.__reconnectRetries = max(self.__reconnectRetries, exeClass.ex_getOption("ReconnectRetries", 0))
            self.__extraArgs[name] = exeClass.ex_getExtraArguments()

        def connect(self):
            self.__msgClient = MessageClient(self.__mindName)
            self.__msgClient.subscribeToMessage("ProcessTask", self.__processTask)
            self.__msgClient.subscribeToDisconnect(self.__disconnected)
            result = self.__msgClient.connect(
                executorTypes=list(self.__modules), maxTasks=self.__maxTasks, extraArgs=self.__extraArgs
            )
            if result["OK"]:
                self.__aliveLock.alive()
                gLogger.info(f"Connected to {self.__mindName}")
            return result

        def __disconnected(self, msgClient):
            retryCount = 0
            while True:
                gLogger.notice(f"Trying to reconnect to {self.__mindName}")
                result = self.__msgClient.connect(
                    executorTypes=list(self.__modules), maxTasks=self.__maxTasks, extraArgs=self.__extraArgs
                )

                if result["OK"]:
                    if retryCount >= self.__reconnectRetries:
                        self.__aliveLock.alive()
                    gLogger.notice(f"Reconnected to {self.__mindName}")
                    return S_OK()
                retryCount += 1
                if retryCount == self.__reconnectRetries:
                    self.__aliveLock.alive()
                gLogger.info(f"Connect error failed: {result['Message']}")
                gLogger.notice("Failed to reconnect. Sleeping for %d seconds" % self.__reconnectSleep)
                time.sleep(self.__reconnectSleep)

        def __storeInstance(self, modName, modObj):
            self.__instanceLock.acquire()
            try:
                self.__instances[modName].append(modObj)
            finally:
                self.__instanceLock.release()

        def __getInstance(self, moduleName):
            self.__instanceLock.acquire()
            try:
                if moduleName not in self.__instances:
                    self.__instances[moduleName] = []
                try:
                    return S_OK(self.__instances[moduleName].pop(0))
                except IndexError:
                    pass
            finally:
                self.__instanceLock.release()
            try:
                modObj = self.__modules[moduleName]
            except KeyError:
                return S_ERROR("Unknown %s executor")
            modInstance = modObj()
            return S_OK(modInstance)

        def __sendExecutorError(self, eType, taskId, errMsg):
            result = self.__msgClient.createMessage("ExecutorError")
            if not result["OK"]:
                return result
            msgObj = result["Value"]
            msgObj.taskId = taskId
            msgObj.errorMsg = errMsg
            msgObj.eType = eType
            return self.__msgClient.sendMessage(msgObj)

        def __processTask(self, msgObj):
            eType = msgObj.eType
            taskId = msgObj.taskId
            taskStub = msgObj.taskStub

            result = self.__moduleProcess(eType, taskId, taskStub)
            if not result["OK"]:
                return self.__sendExecutorError(eType, taskId, result["Message"])
            msgName, taskStub, extra = result["Value"]

            result = self.__msgClient.createMessage(msgName)
            if not result["OK"]:
                return self.__sendExecutorError(eType, taskId, f"Can't generate {msgName} message: {result['Message']}")
            gLogger.verbose(f"Task {str(taskId)}: Sending {msgName}")
            msgObj = result["Value"]
            msgObj.taskId = taskId
            msgObj.taskStub = taskStub
            if msgName == "TaskError":
                msgObj.errorMsg = extra
                msgObj.eType = eType
            elif msgName == "TaskFreeze":
                msgObj.freezeTime = extra
            return self.__msgClient.sendMessage(msgObj)

        def __moduleProcess(self, eType, taskId, taskStub, fastTrackLevel=0):
            result = self.__getInstance(eType)
            if not result["OK"]:
                return result
            modInstance = result["Value"]
            try:
                result = modInstance._ex_processTask(taskId, taskStub)
            except Exception as excp:
                gLogger.exception(f"Error while processing task {taskId}", lException=excp)
                return S_ERROR(f"Error processing task {taskId}: {excp}")

            self.__storeInstance(eType, modInstance)

            if not result["OK"]:
                return S_OK(("TaskError", taskStub, f"Error: {result['Message']}"))
            taskStub, freezeTime, fastTrackType = result["Value"]
            if freezeTime:
                return S_OK(("TaskFreeze", taskStub, freezeTime))
            if fastTrackType:
                if fastTrackLevel < 10 and fastTrackType in self.__modules:
                    gLogger.notice(f"Fast tracking task {taskId} to {fastTrackType}")
                    return self.__moduleProcess(fastTrackType, taskId, taskStub, fastTrackLevel + 1)
                else:
                    gLogger.notice(f"Stopping {taskId} fast track. Sending back to the mind")

            return S_OK(("TaskDone", taskStub, True))

    #####
    # Start of ExecutorReactor
    #####

    def __init__(self):
        self.__aliveLock = self.AliveLock()
        self.__executorModules = {}
        self.__minds = {}
        self.__loader = ModuleLoader("Executor", PathFinder.getExecutorSection)

    def loadModules(self, modulesList, hideExceptions=False):
        """
        Load all modules required in moduleList
        """
        result = self.__loader.loadModules(modulesList, hideExceptions=hideExceptions)
        if not result["OK"]:
            return result
        self.__executorModules = self.__loader.getModules()
        return S_OK()

    # Go!
    def go(self):
        for name in self.__executorModules:
            exeClass = self.__executorModules[name]["classObj"]
            result = exeClass._ex_initialize(name, self.__executorModules[name]["loadName"])
            if not result["OK"]:
                return result
            mind = exeClass.ex_getMind()
            if mind not in self.__minds:
                self.__minds[mind] = self.MindCluster(mind, self.__aliveLock)
            mc = self.__minds[mind]
            mc.addModule(name, exeClass)
        for mindName in self.__minds:
            gLogger.info(f"Trying to connect to {mindName}")
            result = self.__minds[mindName].connect()
            if not result["OK"]:
                return result
        self.__aliveLock.lockUntilAllDead()
        return S_OK()
