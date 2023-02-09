""" Used by the executors for dispatching events (IIUC)
"""
import threading
import time

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.ReturnValues import isReturnStructure
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.MonitoringSystem.Client.MonitoringReporter import MonitoringReporter


class ExecutorState:
    def __init__(self, log=False):
        if log:
            self.__log = log
        else:
            self.__log = gLogger
        self.__lock = threading.Lock()
        self.__typeToId = {}
        self.__maxTasks = {}
        self.__execTasks = {}
        self.__taskInExec = {}

    def _internals(self):
        return {
            "type2id": dict(self.__typeToId),
            "maxTasks": dict(self.__maxTasks),
            "execTasks": dict(self.__execTasks),
            "tasksInExec": dict(self.__taskInExec),
            "locked": self.__lock.locked(),  # pylint: disable=no-member
        }

    def addExecutor(self, eId, eTypes, maxTasks=1):
        self.__lock.acquire()
        try:
            self.__maxTasks[eId] = max(1, maxTasks)
            if eId not in self.__execTasks:
                self.__execTasks[eId] = set()
            if not isinstance(eTypes, (list, tuple)):
                eTypes = [eTypes]
            for eType in eTypes:
                if eType not in self.__typeToId:
                    self.__typeToId[eType] = set()
                self.__typeToId[eType].add(eId)
        finally:
            self.__lock.release()

    def removeExecutor(self, eId):
        self.__lock.acquire()
        try:
            tasks = []
            for eType in self.__typeToId:
                if eId in self.__typeToId[eType]:
                    self.__typeToId[eType].remove(eId)
            for taskId in self.__execTasks[eId]:
                self.__taskInExec.pop(taskId)
                tasks.append(taskId)
            self.__execTasks.pop(eId)
            self.__maxTasks.pop(eId)
            return tasks
        finally:
            self.__lock.release()

    def getTasksForExecutor(self, eId):
        try:
            return set(self.__execTasks[eId])
        except KeyError:
            return set()

    def full(self, eId):
        try:
            return len(self.__execTasks[eId]) >= self.__maxTasks[eId]
        except KeyError:
            return True

    def freeSlots(self, eId):
        try:
            return self.__maxTasks[eId] - len(self.__execTasks[eId])
        except KeyError:
            return 0

    def getFreeExecutors(self, eType):
        execs = {}
        try:
            eids = self.__typeToId[eType]
        except KeyError:
            return execs
        try:
            for eid in eids:
                freeSlots = self.freeSlots(eid)
                if freeSlots:
                    execs[eid] = freeSlots
        except RuntimeError:
            pass
        return execs

    def getIdleExecutor(self, eType):
        idleId = None
        maxFreeSlots = 0
        try:
            for eId in self.__typeToId[eType]:
                freeSlots = self.freeSlots(eId)
                if freeSlots > maxFreeSlots:
                    maxFreeSlots = freeSlots
                    idleId = eId
        except KeyError:
            pass
        return idleId

    def addTask(self, eId, taskId):
        self.__lock.acquire()
        try:
            try:
                self.__taskInExec[taskId] = eId
                self.__execTasks[eId].add(taskId)
                return len(self.__execTasks[eId])
            except KeyError:
                return 0
        finally:
            self.__lock.release()

    def getExecutorOfTask(self, taskId):
        try:
            return self.__taskInExec[taskId]
        except KeyError:
            return None

    def removeTask(self, taskId, eId=None):
        self.__lock.acquire()
        try:
            try:
                if eId is None:
                    eId = self.__taskInExec[taskId]
                self.__execTasks[eId].remove(taskId)
                self.__taskInExec.pop(taskId)
                return True
            except KeyError:
                return False
        finally:
            self.__lock.release()


class ExecutorQueues:
    def __init__(self, log=False):
        if log:
            self.__log = log
        else:
            self.__log = gLogger
        self.__lock = threading.Lock()
        self.__queues = {}
        self.__lastUse = {}
        self.__taskInQueue = {}

    def _internals(self):
        return {
            "queues": dict(self.__queues),
            "lastUse": dict(self.__lastUse),
            "taskInQueue": dict(self.__taskInQueue),
            "locked": self.__lock.locked(),  # pylint: disable=no-member
        }

    def getExecutorList(self):
        return [eType for eType in self.__queues]

    def pushTask(self, eType, taskId, ahead=False):
        self.__log.verbose(f"Pushing task {taskId} into waiting queue for executor {eType}")
        self.__lock.acquire()
        try:
            if taskId in self.__taskInQueue:
                if self.__taskInQueue[taskId] != eType:
                    errMsg = "Task {} cannot be queued because it's already queued for {}".format(
                        taskId,
                        self.__taskInQueue[taskId],
                    )
                    self.__log.fatal(errMsg)
                    return 0
                return len(self.__queues[eType])
            if eType not in self.__queues:
                self.__queues[eType] = []
            self.__lastUse[eType] = time.time()
            if ahead:
                self.__queues[eType].insert(0, taskId)
            else:
                self.__queues[eType].append(taskId)
            self.__taskInQueue[taskId] = eType
            return len(self.__queues[eType])
        finally:
            self.__lock.release()

    def popTask(self, eTypes):
        if not isinstance(eTypes, (list, tuple)):
            eTypes = [eTypes]
        self.__lock.acquire()
        for eType in eTypes:
            try:
                taskId = self.__queues[eType].pop(0)
                del self.__taskInQueue[taskId]
                # Found! release and return!
                self.__lock.release()
                self.__lastUse[eType] = time.time()
                self.__log.verbose(f"Popped task {taskId} from executor {eType} waiting queue")
                return (taskId, eType)
            except IndexError:
                continue
            except KeyError:
                continue
        self.__lock.release()
        # Not found. release and return None
        return None

    def getState(self):
        self.__lock.acquire()
        try:
            qInfo = {}
            for qName in self.__queues:
                qInfo[qName] = list(self.__queues[qName])
        finally:
            self.__lock.release()
        return qInfo

    def deleteTask(self, taskId):
        self.__log.verbose(f"Deleting task {taskId} from waiting queues")
        self.__lock.acquire()
        try:
            try:
                eType = self.__taskInQueue[taskId]
                del self.__taskInQueue[taskId]
                self.__lastUse[eType] = time.time()
            except KeyError:
                return False
            try:
                iPos = self.__queues[eType].index(taskId)
            except ValueError:
                return False
            del self.__queues[eType][iPos]
            return True
        finally:
            self.__lock.release()

    def waitingTasks(self, eType):
        self.__lock.acquire()
        try:
            try:
                return len(self.__queues[eType])
            except KeyError:
                return 0
        finally:
            self.__lock.release()


class ExecutorDispatcherCallbacks:
    def cbDispatch(self, taskId, taskObj, pathExecuted):
        return S_ERROR("No dispatch callback defined")

    def cbSendTask(self, taskId, taskObj, eId, eType):
        return S_ERROR("No send task callback defined")

    def cbDisconectExecutor(self, eId):
        return S_ERROR("No disconnect callback defined")

    def cbTaskError(self, taskId, taskObj, errorMsg):
        return S_ERROR("No error callback defined")

    def cbTaskProcessed(self, taskId, taskObj, eType):
        return S_OK()

    def cbTaskFreeze(self, taskId, taskObj, eType):
        return S_OK()


class ExecutorDispatcher:
    class ETask:
        def __init__(self, taskId, taskObj):
            self.taskId = taskId
            self.taskObj = taskObj
            self.pathExecuted = []
            self.freezeTime = 60
            self.frozenTime = 0
            self.frozenSince = 0
            self.frozenCount = 0
            self.frozenMsg = False
            self.eType = False
            self.sendTime = 0
            self.retries = 0

        def __repr__(self):
            rS = f"<ETask {self.taskId}"
            if self.eType:
                rS += f" eType={self.eType}>"
            else:
                rS += ">"
            return rS

    def __init__(self, monitor=None):
        """
        :param monitor: good question.... what's meant to be used for monitoring.
            Either a :py:class`DIRAC.FrameworkSystem.Client.MonitoringClient.MonitoringClient` or a
            :py:class`DIRAC.MonitoringSystem.Client.MonitoringReporter.MonitoringReporter`
        """
        self.__idMap = {}
        self.__execTypes = {}
        self.__executorsLock = threading.Lock()
        self.__tasksLock = threading.Lock()
        self.__freezerLock = threading.Lock()
        self.__tasks = {}
        self.__log = gLogger.getSubLogger(self.__class__.__name__)
        self.__taskFreezer = []
        self.__queues = ExecutorQueues(self.__log)
        self.__states = ExecutorState(self.__log)
        self.__cbHolder = ExecutorDispatcherCallbacks()
        self.__monitor = None
        if isinstance(monitor, MonitoringReporter):
            self.__monitoringReporter = monitor
        gThreadScheduler.addPeriodicTask(60, self.__doPeriodicStuff)
        # If a task is frozen too many times, send error or forget task?
        self.__failedOnTooFrozen = True
        # If a task fails to properly dispatch, freeze or forget task?
        self.__freezeOnFailedDispatch = True
        # If a task needs to go to an executor that has not connected. Freeze or forget the task?
        self.__freezeOnUnknownExecutor = True

    def setFailedOnTooFrozen(self, value):
        self.__failedOnTooFrozen = value

    def setFreezeOnFailedDispatch(self, value):
        self.__freezeOnFailedDispatch = value

    def setFreezeOnUnknownExecutor(self, value):
        self.__freezeOnUnknownExecutor = value

    def _internals(self):
        return {
            "idMap": dict(self.__idMap),
            "execTypes": dict(self.__execTypes),
            "tasks": sorted(self.__tasks),
            "freezer": list(self.__taskFreezer),
            "queues": self.__queues._internals(),
            "states": self.__states._internals(),
            "locked": {
                "exec": self.__executorsLock.locked(),  # pylint: disable=no-member
                "tasks": self.__tasksLock.locked(),  # pylint: disable=no-member
                "freezer": self.__freezerLock.locked(),  # pylint: disable=no-member
            },
        }

    def setCallbacks(self, callbacksObj):
        if not isinstance(callbacksObj, ExecutorDispatcherCallbacks):
            return S_ERROR("Callbacks object does not inherit from ExecutorDispatcherCallbacks")
        self.__cbHolder = callbacksObj
        return S_OK()

    def __doPeriodicStuff(self):
        self.__unfreezeTasks()
        for eType in self.__execTypes:
            self.__fillExecutors(eType)
        if not self.__monitor:
            return
        eTypes = self.__execTypes

    def addExecutor(self, eId, eTypes, maxTasks=1):
        self.__log.verbose("Adding new executor to the pool", f"{eId}: {', '.join(eTypes)}")
        self.__executorsLock.acquire()
        try:
            if eId in self.__idMap:
                return
            if not isinstance(eTypes, (list, tuple)):
                eTypes = [eTypes]
            self.__idMap[eId] = list(eTypes)
            self.__states.addExecutor(eId, eTypes, maxTasks)
            for eType in eTypes:
                if eType not in self.__execTypes:
                    self.__execTypes[eType] = 0
                self.__execTypes[eType] += 1
        finally:
            self.__executorsLock.release()
        for eType in eTypes:
            self.__fillExecutors(eType)

    def removeExecutor(self, eId):
        self.__log.info("Removing executor", eId)
        self.__executorsLock.acquire()
        try:
            if eId not in self.__idMap:
                return
            eTypes = self.__idMap.pop(eId)
            for eType in eTypes:
                self.__execTypes[eType] -= 1
            tasksInExec = self.__states.removeExecutor(eId)
            for taskId in tasksInExec:
                try:
                    eTask = self.__tasks[taskId]
                except KeyError:
                    # Task already removed
                    pass
                if eTask.eType:
                    self.__queues.pushTask(eTask.eType, taskId, ahead=True)
                else:
                    self.__dispatchTask(taskId)
        finally:
            self.__executorsLock.release()
        try:
            self.__cbHolder.cbDisconectExecutor(eId)
        except Exception:
            self.__log.exception(f"Exception while disconnecting agent {eId}")
        for eType in eTypes:
            self.__fillExecutors(eType)

    def __freezeTask(self, taskId, errMsg, eType=False, freezeTime=60):
        self.__log.verbose("Freezing task", taskId)
        self.__freezerLock.acquire()
        try:
            if taskId in self.__taskFreezer:
                return False
            try:
                eTask = self.__tasks[taskId]
            except KeyError:
                return False
            eTask.freezeTime = freezeTime
            eTask.frozenMessage = errMsg
            eTask.frozenSince = time.time()
            eTask.frozenCount += 1
            eTask.eType = eType
            isFrozen = False
            if eTask.frozenCount < 10:
                self.__taskFreezer.append(taskId)
                isFrozen = True
        finally:
            self.__freezerLock.release()
        if not isFrozen:
            self.removeTask(taskId)
            if self.__failedOnTooFrozen:
                self.__cbHolder.cbTaskError(taskId, eTask.taskObj, f"Retried more than 10 times. Last error: {errMsg}")
            return False
        return True

    def __isFrozen(self, taskId):
        return taskId in self.__taskFreezer

    def __removeFromFreezer(self, taskId):
        self.__freezerLock.acquire()
        try:
            try:
                iP = self.__taskFreezer.index(taskId)
            except ValueError:
                return False
            self.__taskFreezer.pop(iP)
            try:
                eTask = self.__tasks[taskId]
            except KeyError:
                return False
            eTask.frozenTime += time.time() - eTask.frozenSince
        finally:
            self.__freezerLock.release()
        return True

    def __unfreezeTasks(self, eType=False):
        iP = 0
        while iP < len(self.__taskFreezer):
            self.__freezerLock.acquire()
            try:
                try:
                    taskId = self.__taskFreezer[iP]
                except IndexError:
                    return
                try:
                    eTask = self.__tasks[taskId]
                except KeyError:
                    self.__log.notice(f"Removing task {taskId} from the freezer. Somebody has removed the task")
                    self.__taskFreezer.pop(iP)
                    continue
                # Current taskId/eTask is the one to defrost
                if eType and eType != eTask.eType:
                    iP += 1
                    continue
                if time.time() - eTask.frozenSince < eTask.freezeTime:
                    iP += 1
                    continue
                self.__taskFreezer.pop(iP)
            finally:
                self.__freezerLock.release()
            # Out of the lock zone to minimize zone of exclusion
            eTask.frozenTime += time.time() - eTask.frozenSince
            self.__log.verbose(f"Unfreezed task {taskId}")
            self.__dispatchTask(taskId, defrozeIfNeeded=False)

    def __addTaskIfNew(self, taskId, taskObj):
        self.__tasksLock.acquire()
        try:
            if taskId in self.__tasks:
                self.__log.verbose(f"Task {taskId} was already known")
                return False
            self.__tasks[taskId] = ExecutorDispatcher.ETask(taskId, taskObj)
            self.__log.verbose(f"Added task {taskId}")
            return True
        finally:
            self.__tasksLock.release()

    def getTask(self, taskId):
        try:
            return self.__tasks[taskId].taskObj
        except KeyError:
            return None

    def __dispatchTask(self, taskId, defrozeIfNeeded=True):
        self.__log.verbose(f"Dispatching task {taskId}")
        # If task already in executor skip
        if self.__states.getExecutorOfTask(taskId):
            return S_OK()
        self.__removeFromFreezer(taskId)

        result = self.__getNextExecutor(taskId)

        if not result["OK"]:
            self.__log.warn("Error while calling dispatch callback", result["Message"])
            if self.__freezeOnFailedDispatch:
                if self.__freezeTask(taskId, result["Message"]):
                    return S_OK()
                return result
            taskObj = self.getTask(taskId)
            self.removeTask(taskId)
            self.__cbHolder.cbTaskError(taskId, taskObj, f"Could not dispatch task: {result['Message']}")
            return S_ERROR("Could not add task. Dispatching task failed")

        eType = result["Value"]
        if not eType:
            self.__log.verbose(f"No more executors for task {taskId}")
            return self.removeTask(taskId)

        self.__log.verbose(f"Next executor type is {eType} for task {taskId}")
        if eType not in self.__execTypes:
            if self.__freezeOnUnknownExecutor:
                self.__log.verbose(f"Executor type {eType} has not connected. Freezing task {taskId}")
                self.__freezeTask(taskId, f"Unknown executor {eType} type", eType=eType, freezeTime=0)
                return S_OK()
            self.__log.verbose(f"Executor type {eType} has not connected. Forgetting task {taskId}")
            return self.removeTask(taskId)

        self.__queues.pushTask(eType, taskId)
        self.__fillExecutors(eType, defrozeIfNeeded=defrozeIfNeeded)
        return S_OK()

    def __taskProcessedCallback(self, taskId, taskObj, eType):
        try:
            result = self.__cbHolder.cbTaskProcessed(taskId, taskObj, eType)
        except Exception:
            self.__log.exception("Exception while calling taskDone callback")
            return S_ERROR("Exception while calling taskDone callback")

        if not isReturnStructure(result):
            errMsg = "taskDone callback did not return a S_OK/S_ERROR structure"
            self.__log.fatal(errMsg)
            return S_ERROR(errMsg)

        return result

    def __taskFreezeCallback(self, taskId, taskObj, eType):
        try:
            result = self.__cbHolder.cbTaskFreeze(taskId, taskObj, eType)
        except Exception:
            self.__log.exception("Exception while calling taskFreeze callback")
            return S_ERROR("Exception while calling taskFreeze callback")

        if not isReturnStructure(result):
            errMsg = "taskFreeze callback did not return a S_OK/S_ERROR structure"
            self.__log.fatal(errMsg)
            return S_ERROR(errMsg)

        return result

    def __getNextExecutor(self, taskId):
        try:
            eTask = self.__tasks[taskId]
        except KeyError:
            msg = "Task was deleted prematurely while being dispatched"
            self.__log.error(msg, f"{taskId}")
            return S_ERROR(msg)
        try:
            result = self.__cbHolder.cbDispatch(taskId, eTask.taskObj, tuple(eTask.pathExecuted))
        except Exception:
            self.__log.exception("Exception while calling dispatch callback")
            return S_ERROR("Exception while calling dispatch callback")

        if not isReturnStructure(result):
            errMsg = "Dispatch callback did not return a S_OK/S_ERROR structure"
            self.__log.fatal(errMsg)
            return S_ERROR(errMsg)

        # Assign the next executor type to the task
        if result["OK"]:
            eTask.eType = result["Value"]

        return result

    def getTaskIds(self):
        return list(self.__tasks)

    def getExecutorsConnected(self):
        return dict(self.__execTypes)

    def addTask(self, taskId, taskObj):
        if not self.__addTaskIfNew(taskId, taskObj):
            self.__unfreezeTasks()
            return S_OK()
        return self.__dispatchTask(taskId)

    def removeTask(self, taskId):
        try:
            self.__tasks.pop(taskId)
        except KeyError:
            self.__log.verbose(f"Task {taskId} is already removed")
            return S_OK()
        self.__log.verbose(f"Removing task {taskId}")
        eId = self.__states.getExecutorOfTask(taskId)
        self.__queues.deleteTask(taskId)
        self.__states.removeTask(taskId)
        self.__freezerLock.acquire()
        try:
            try:
                self.__taskFreezer.pop(self.__taskFreezer.index(taskId))
            except KeyError:
                pass
            except ValueError:
                pass
        finally:
            self.__freezerLock.release()
        if eId:
            # Send task to executor if idle
            self.__sendTaskToExecutor(eId, checkIdle=True)
        return S_OK()

    def __taskReceived(self, taskId, eId):
        try:
            eTask = self.__tasks[taskId]
        except KeyError:
            errMsg = f"Task {taskId} is not known"
            self.__log.error("Task is not known", f"{taskId}")
            return S_ERROR(errMsg)
        if not self.__states.removeTask(taskId, eId):
            self.__log.info(f"Executor {eId} says it's processed task {taskId} but it didn't have it")
            return S_OK()
        if eTask.eType not in self.__idMap[eId]:
            errMsg = f"Executor type invalid for {eId}. Redoing task {taskId}"
            self.__log.error("Executor type invalid. Redoing task", f"Type {eId}, Task {taskId}")
            self.removeExecutor(eId)
            self.__dispatchTask(taskId)
            return S_ERROR(errMsg)
        return S_OK(eTask.eType)

    def freezeTask(self, eId, taskId, freezeTime, taskObj=False):
        result = self.__taskReceived(taskId, eId)
        if not result["OK"]:
            return result
        eType = result["Value"]
        # Executor didn't have the task.
        if not eType:
            # Fill the executor
            self.__sendTaskToExecutor(eId)
            return S_OK()
        if not taskObj:
            taskObj = self.__tasks[taskId].taskObj
        result = self.__taskFreezeCallback(taskId, taskObj, eType)
        if not result["OK"]:
            # Fill the executor
            self.__sendTaskToExecutor(eId)
            return result
        try:
            self.__tasks[taskId].taskObj = taskObj
        except KeyError:
            self.__log.error("Task seems to have been removed while being processed!", f"{taskId}")
            self.__sendTaskToExecutor(eId, eType)
            return S_OK()
        self.__freezeTask(taskId, f"Freeze request by {eType} executor", eType=eType, freezeTime=freezeTime)
        self.__sendTaskToExecutor(eId, eType)
        return S_OK()

    def taskProcessed(self, eId, taskId, taskObj=False):
        result = self.__taskReceived(taskId, eId)
        if not result["OK"]:
            return result
        eType = result["Value"]
        # Executor didn't have the task.
        if not eType:
            # Fill the executor
            self.__sendTaskToExecutor(eId)
            return S_OK()
        # Call the done callback
        if not taskObj:
            taskObj = self.__tasks[taskId].taskObj
        result = self.__taskProcessedCallback(taskId, taskObj, eType)
        if not result["OK"]:
            # Fill the executor
            self.__sendTaskToExecutor(eId)
            # Remove the task
            self.removeTask(taskId)
            return result
        # Up until here it's an executor error. From now on it can be a task error
        try:
            self.__tasks[taskId].taskObj = taskObj
            self.__tasks[taskId].pathExecuted.append(eType)
        except KeyError:
            self.__log.error("Task seems to have been removed while being processed!", f"{taskId}")
            self.__sendTaskToExecutor(eId, eType)
            return S_OK()
        self.__log.verbose(f"Executor {eId} processed task {taskId}")
        result = self.__dispatchTask(taskId)
        self.__sendTaskToExecutor(eId, eType)
        return result

    def retryTask(self, eId, taskId):
        if taskId not in self.__tasks:
            errMsg = f"Task {taskId} is not known"
            self.__log.error("Task is not known", f"{taskId}")
            return S_ERROR(errMsg)
        if not self.__states.removeTask(taskId, eId):
            self.__log.info(f"Executor {eId} says it's processed task {taskId} but it didn't have it")
            self.__sendTaskToExecutor(eId)
            return S_OK()
        self.__log.verbose(f"Executor {eId} did NOT process task {taskId}, retrying")
        try:
            self.__tasks[taskId].retries += 1
        except KeyError:
            self.__log.error("Task seems to have been removed while waiting for retry!", f"{taskId}")
            return S_OK()
        return self.__dispatchTask(taskId)

    def __fillExecutors(self, eType, defrozeIfNeeded=True):
        if defrozeIfNeeded:
            self.__log.verbose(f"Unfreezing tasks for {eType}")
            self.__unfreezeTasks(eType)
        self.__log.verbose(f"Filling {eType} executors")
        eId = self.__states.getIdleExecutor(eType)
        while eId:
            result = self.__sendTaskToExecutor(eId, eType)
            if not result["OK"]:
                self.__log.error("Could not send task to executor", f"{result['Message']}")
            else:
                if not result["Value"]:
                    # No more tasks for eType
                    break
                self.__log.verbose(f"Task {result['Value']} was sent to {eId}")
            eId = self.__states.getIdleExecutor(eType)
        self.__log.verbose(f"No more idle executors for {eType}")

    def __sendTaskToExecutor(self, eId, eTypes=False, checkIdle=False):
        if checkIdle and self.__states.freeSlots(eId) == 0:
            return S_OK()
        try:
            searchTypes = list(reversed(self.__idMap[eId]))
        except KeyError:
            self.__log.verbose(f"Executor {eId} invalid/disconnected")
            return S_ERROR("Invalid executor")
        if eTypes:
            if not isinstance(eTypes, (list, tuple)):
                eTypes = [eTypes]
            for eType in reversed(eTypes):
                try:
                    searchTypes.remove(eType)
                except ValueError:
                    pass
                searchTypes.append(eType)
        pData = self.__queues.popTask(searchTypes)
        if pData is None:
            self.__log.verbose(f"No more tasks for {eTypes}")
            return S_OK()
        taskId, eType = pData
        self.__log.verbose(f"Sending task {taskId} to {eType}={eId}")
        self.__states.addTask(eId, taskId)
        try:
            self.__msgTaskToExecutor(taskId, eId, eType)
        except Exception:
            self.__log.exception("Exception while sending task to executor")
            if taskId in self.__tasks:
                self.__queues.pushTask(eType, taskId, ahead=False)
            self.__states.removeTask(taskId)
            return S_ERROR("Exception while sending task to executor")
        return S_OK(taskId)

    def __msgTaskToExecutor(self, taskId, eId, eType):
        self.__tasks[taskId].sendTime = time.time()
        result = self.__cbHolder.cbSendTask(taskId, self.__tasks[taskId].taskObj, eId, eType)
        if not isReturnStructure(result):
            errMsg = "Send task callback did not send back an S_OK/S_ERROR structure"
            self.__log.fatal(errMsg)
            raise ValueError(errMsg)
        if not result["OK"]:
            self.__log.error("Failed to cbSendTask", f"{result!r}")
            raise RuntimeError(result)
