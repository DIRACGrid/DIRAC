""" a scheduler of threads, of course!
"""
import hashlib
import threading
import time

from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.Core.Utilities.ThreadSafe import Synchronizer

gSchedulerLock = Synchronizer()


class ThreadScheduler:
    def __init__(self, enableReactorThread=True, minPeriod=60):
        self.__thId = False
        self.__minPeriod = minPeriod
        self.__taskDict = {}
        self.__hood = []
        self.__createReactorThread = enableReactorThread
        self.__nowEpoch = time.time
        self.__sleeper = time.sleep
        self.__min = min

    def setMinValidPeriod(self, period):
        self.__minPeriod = period

    def disableCreateReactorThread(self):
        self.__createReactorThread = False

    def addPeriodicTask(self, period, taskFunc, taskArgs=(), executions=0, elapsedTime=0):
        if not callable(taskFunc):
            return S_ERROR("%s is not callable" % str(taskFunc))
        period = max(period, self.__minPeriod)
        elapsedTime = min(elapsedTime, period - 1)
        md = hashlib.md5()
        task = {
            "period": period,
            "func": taskFunc,
            "args": taskArgs,
        }
        md.update(str(task).encode())
        taskId = md.hexdigest()
        if taskId in self.__taskDict:
            return S_ERROR("Task %s is already added" % taskId)
        if executions:
            task["executions"] = executions
        self.__taskDict[taskId] = task
        retVal = self.__scheduleTask(taskId, elapsedTime)
        if not retVal["OK"]:
            return retVal
        self.__createExecutorIfNeeded()
        return S_OK(taskId)

    def setTaskPeriod(self, taskId, period):
        try:
            period = int(period)
        except ValueError:
            return S_ERROR("Period must be a number")
        period = max(period, self.__minPeriod)
        try:
            self.__taskDict[taskId]["period"] = period
        except KeyError:
            return S_ERROR("Unknown task %s" % taskId)
        return S_OK()

    @gSchedulerLock
    def removeTask(self, taskId):
        if taskId not in self.__taskDict:
            return S_ERROR("Task %s does not exist" % taskId)
        del self.__taskDict[taskId]
        for i in range(len(self.__hood)):
            if self.__hood[i][0] == taskId:
                del self.__hood[i]
                break
        return S_OK()

    def addSingleTask(self, taskFunc, taskArgs=()):
        return self.addPeriodicTask(self.__minPeriod, taskFunc, taskArgs, executions=1, elapsedTime=self.__minPeriod)

    @gSchedulerLock
    def __scheduleTask(self, taskId, elapsedTime=0):
        executeInSecs = self.__taskDict[taskId]["period"]
        elapsedTime = min(elapsedTime, executeInSecs - 1)
        if elapsedTime:
            executeInSecs -= elapsedTime

        now = time.time()
        for i in range(len(self.__hood)):
            if taskId == self.__hood[i][0]:
                tD = self.__hood[i][1] - now
                if abs(tD - executeInSecs) < 30:
                    return S_OK()
                else:
                    del self.__hood[i]
                    break

        executionTime = now + executeInSecs
        inserted = False
        for i in range(len(self.__hood)):
            if executionTime < self.__hood[i][1]:
                self.__hood.insert(i, (taskId, executionTime))
                inserted = True
                break
        if not inserted:
            self.__hood.append((taskId, executionTime))

        return S_OK()

    def __executorThread(self):
        while self.__hood:
            timeToNext = self.executeNextTask()
            if timeToNext is None:
                break
            if timeToNext and timeToNext > 0.1:
                self.__sleeper(self.__min(timeToNext, 1))
        # If we are leaving
        self.__destroyExecutor()

    def executeNextTask(self):
        if not self.__hood:
            return None
        timeToWait = self.__timeToNextTask()
        if timeToWait and timeToWait > 0:
            return timeToWait
        taskId = self.__popNextTaskId()
        startTime = time.time()
        self.__executeTask(taskId)
        elapsedTime = time.time() - startTime
        self.__scheduleIfNeeded(taskId, elapsedTime)
        return self.__timeToNextTask()

    @gSchedulerLock
    def __createExecutorIfNeeded(self):
        if not self.__createReactorThread:
            return
        if self.__thId:
            return
        self.__thId = threading.Thread(target=self.__executorThread)
        self.__thId.daemon = True
        self.__thId.start()

    @gSchedulerLock
    def __destroyExecutor(self):
        self.__thId = False

    @gSchedulerLock
    def __timeToNextTask(self):
        if not self.__hood:
            return None
        return self.__hood[0][1] - self.__nowEpoch()

    @gSchedulerLock
    def __popNextTaskId(self):
        if len(self.__hood) == 0:
            return None
        return self.__hood.pop(0)[0]

    @gSchedulerLock
    def getNextTaskId(self):
        if len(self.__hood) == 0:
            return None
        return self.__hood[0][0]

    @gSchedulerLock
    def setNumExecutionsForTask(self, taskId, numExecutions):
        if taskId not in self.__taskDict:
            return False
        if numExecutions:
            self.__taskDict[taskId]["executions"] = numExecutions
        else:
            del self.__taskDict[taskId]["executions"]

    def __executeTask(self, taskId):
        if taskId not in self.__taskDict:
            return False
        task = self.__taskDict[taskId]
        if "executions" in task:
            task["executions"] -= 1
        try:
            task["func"](*task["args"])
        except Exception as lException:
            gLogger.exception("Exception while executing scheduled task", lException=lException)
            return False
        return True

    def __scheduleIfNeeded(self, taskId, elapsedTime=0):
        if "executions" in self.__taskDict[taskId]:
            if self.__taskDict[taskId]["executions"] == 0:
                del self.__taskDict[taskId]
                return True
        return self.__scheduleTask(taskId, elapsedTime)


gThreadScheduler = ThreadScheduler()
