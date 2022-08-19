""" :mod: ProcessPoolTests
  =======================

  .. module: ProcessPoolTests
  :synopsis: unit tests for ProcessPool
  .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

  unit tests for ProcessPool
"""
import os
import sys
import random
import time
import threading

import pytest

from DIRAC import gLogger
from DIRAC.Core.Utilities.ProcessPool import ProcessPool

# Mark this entire module as slow
pytestmark = pytest.mark.slow


@pytest.fixture(autouse=True)
def capture_wrap():
    """Avoid https://github.com/pytest-dev/pytest/issues/5502"""
    sys.stderr.close = lambda *args: None
    sys.stdout.close = lambda *args: None
    yield


def ResultCallback(task, taskResult):
    """dummy result callback"""
    print(f"callback for {task.getTaskID()} result is {taskResult}")


def ExceptionCallback(task, exec_info):
    """dummy exception callback"""
    print(f"callback for {task.getTaskID()} exception is {exec_info}")


def CallableFunc(taskID, timeWait, raiseException=False):
    """global function to be executed in task"""
    print(f"pid={os.getpid()} task={taskID} will sleep for {timeWait} s")
    time.sleep(timeWait)
    if raiseException:
        raise Exception("testException")
    return timeWait


class CallableClass:
    """callable class to be executed in task"""

    def __init__(self, taskID, timeWait, raiseException=False):
        self.log = gLogger.getSubLogger(self.__class__.__name__ + "/%s" % taskID)
        self.taskID = taskID
        self.timeWait = timeWait
        self.raiseException = raiseException

    def __call__(self):
        self.log.always(f"pid={os.getpid()} task={self.taskID} will sleep for {self.timeWait} s")
        time.sleep(self.timeWait)
        if self.raiseException:
            raise Exception("testException")
        return self.timeWait


# global locked lock
gLock = threading.Lock()
# make sure it is locked
gLock.acquire()


# dummy callable locked class
class LockedCallableClass:
    """callable and locked class"""

    def __init__(self, taskID, timeWait, raiseException=False):
        self.log = gLogger.getSubLogger(self.__class__.__name__ + "/%s" % taskID)
        self.taskID = taskID
        self.log.always(f"pid={os.getpid()} task={self.taskID} I'm locked")
        gLock.acquire()
        self.log.always("you can't see that line, object is stuck by gLock")
        self.timeWait = timeWait
        self.raiseException = raiseException
        gLock.release()

    def __call__(self):
        self.log.always("If you see this line, miracle had happened!")
        self.log.always("will sleep for %s" % self.timeWait)
        time.sleep(self.timeWait)
        if self.raiseException:
            raise Exception("testException")
        return self.timeWait


########################################################################
@pytest.fixture()
def processPool():
    gLogger.showHeaders(True)
    log = gLogger.getSubLogger("TaskCallbacksTests")
    processPool = ProcessPool(4, 8, 8)
    processPool.daemonize()
    yield processPool


def test_TaskCallbacks_CallableClass(processPool):
    """CallableClass and task callbacks test"""
    i = 0
    while True:
        if processPool.getFreeSlots() > 0:
            timeWait = random.randint(0, 5)
            raiseException = False
            if not timeWait:
                raiseException = True
            result = processPool.createAndQueueTask(
                CallableClass,
                taskID=i,
                args=(i, timeWait, raiseException),
                callback=ResultCallback,
                exceptionCallback=ExceptionCallback,
                blocking=True,
            )
            if result["OK"]:
                print("CallableClass enqueued to task %s" % i)
                i += 1
            else:
                continue
        if i == 10:
            break
    processPool.finalize(2)


def test_TaskCallbacks_CallableFunc(processPool):
    """CallableFunc and task callbacks test"""
    i = 0
    while True:
        if processPool.getFreeSlots() > 0:
            timeWait = random.randint(0, 5)
            raiseException = False
            if not timeWait:
                raiseException = True
            result = processPool.createAndQueueTask(
                CallableFunc,
                taskID=i,
                args=(i, timeWait, raiseException),
                callback=ResultCallback,
                exceptionCallback=ExceptionCallback,
                blocking=True,
            )
            if result["OK"]:
                print("CallableClass enqueued to task %s" % i)
                i += 1
            else:
                continue
        if i == 10:
            break
    processPool.finalize(2)


########################################################################
@pytest.fixture()
def processPoolWithCallbacks():
    gLogger.showHeaders(True)
    log = gLogger.getSubLogger("ProcessPoolCallbacksTests")
    processPoolWithCallbacks = ProcessPool(
        4,
        8,
        8,
        poolCallback=lambda taskID, taskResult: log.always(f"callback result for {taskID} is {taskResult}"),
        poolExceptionCallback=lambda taskID, taskException: log.always(
            f"callback exception for {taskID} is {taskException}"
        ),
    )
    processPoolWithCallbacks.daemonize()
    yield processPoolWithCallbacks


def test_ProcessPoolCallbacks_CallableClass(processPoolWithCallbacks):
    """CallableClass and pool callbacks test"""
    i = 0
    while True:
        if processPoolWithCallbacks.getFreeSlots() > 0:
            timeWait = random.randint(0, 5)
            raiseException = False
            if not timeWait:
                raiseException = True
            result = processPoolWithCallbacks.createAndQueueTask(
                CallableClass,
                taskID=i,
                args=(i, timeWait, raiseException),
                usePoolCallbacks=True,
                blocking=True,
            )
            if result["OK"]:
                print("CallableClass enqueued to task %s" % i)
                i += 1
            else:
                continue
        if i == 10:
            break
    processPoolWithCallbacks.finalize(2)


def test_ProcessPoolCallbacks_CallableFunc(processPoolWithCallbacks):
    """CallableFunc and pool callbacks test"""
    i = 0
    while True:
        if processPoolWithCallbacks.getFreeSlots() > 0:
            timeWait = random.randint(0, 5)
            raiseException = False
            if not timeWait:
                raiseException = True
            result = processPoolWithCallbacks.createAndQueueTask(
                CallableFunc,
                taskID=i,
                args=(i, timeWait, raiseException),
                usePoolCallbacks=True,
                blocking=True,
            )
            if result["OK"]:
                print("CallableFunc enqueued to task %s" % i)
                i += 1
            else:
                continue
        if i == 10:
            break
    processPoolWithCallbacks.finalize(2)


########################################################################
@pytest.fixture()
def processPoolWithCallbacks2():
    gLogger.showHeaders(True)
    log = gLogger.getSubLogger("TaskTimeOutTests")
    processPoolWithCallbacks2 = ProcessPool(
        2,
        4,
        8,
        poolCallback=lambda taskID, taskResult: log.always(f"callback result for {taskID} is {taskResult}"),
        poolExceptionCallback=lambda taskID, taskException: log.always(
            f"callback exception for {taskID} is {taskException}"
        ),
    )
    processPoolWithCallbacks2.daemonize()
    yield processPoolWithCallbacks2


def test_TaskTimeOut_CallableClass(processPoolWithCallbacks2):
    """CallableClass and task time out test"""
    i = 0
    while True:
        if processPoolWithCallbacks2.getFreeSlots() > 0:
            timeWait = random.randint(0, 5) * 1
            raiseException = False
            if not timeWait:
                raiseException = True
            result = processPoolWithCallbacks2.createAndQueueTask(
                CallableClass,
                taskID=i,
                args=(i, timeWait, raiseException),
                timeOut=1.5,
                usePoolCallbacks=True,
                blocking=True,
            )
            if result["OK"]:
                print(f"CallableClass enqueued to task {i} timeWait={timeWait} exception={raiseException}")
                i += 1
            else:
                continue
        if i == 16:
            break
    processPoolWithCallbacks2.finalize(2)


def test_TaskTimeOut_CallableFunc(processPoolWithCallbacks2):
    """CallableFunc and task timeout test"""
    i = 0
    while True:
        if processPoolWithCallbacks2.getFreeSlots() > 0:
            timeWait = random.randint(0, 5) * 0.5
            raiseException = False
            if not timeWait:
                raiseException = True
            result = processPoolWithCallbacks2.createAndQueueTask(
                CallableFunc,
                taskID=i,
                args=(i, timeWait, raiseException),
                timeOut=1.5,
                usePoolCallbacks=True,
                blocking=True,
            )
            if result["OK"]:
                print(f"CallableFunc enqueued to task {i} timeWait={timeWait} exception={raiseException}")
                i += 1
            else:
                continue
        if i == 16:
            break
    processPoolWithCallbacks2.finalize(2)


def test_TaskTimeOut_LockedClass(processPoolWithCallbacks2):
    """LockedCallableClass and task time out test"""
    for loop in range(2):
        print("loop %s" % loop)
        i = 0
        while i < 16:
            if processPoolWithCallbacks2.getFreeSlots() > 0:
                timeWait = random.randint(0, 5) * 0.5
                raiseException = False
                if timeWait == 0.5:
                    raiseException = True
                klass = CallableClass
                if timeWait >= 2.0:
                    klass = LockedCallableClass
                result = processPoolWithCallbacks2.createAndQueueTask(
                    klass,
                    taskID=i,
                    args=(i, timeWait, raiseException),
                    timeOut=1.5,
                    usePoolCallbacks=True,
                    blocking=True,
                )
                if result["OK"]:
                    print(
                        "%s enqueued to task %s timeWait=%s exception=%s"
                        % (klass.__name__, i, timeWait, raiseException)
                    )
                    i += 1
                else:
                    continue
        print("being idle for a while")
        for _ in range(100000):
            for _ in range(1000):
                pass

    print("finalizing...")
    processPoolWithCallbacks2.finalize(10)
    # unlock
    gLock.release()
