"""
Usage of ThreadPool

ThreadPool creates a pool of worker threads to process a queue of tasks
much like the producers/consumers paradigm. Users just need to fill the queue
with tasks to be executed and worker threads will execute them

To start working with the ThreadPool first it has to be instanced::

    threadPool = ThreadPool( minThreads, maxThreads, maxQueuedRequests )

    minThreads        -> at all times no less than <minThreads> workers will be alive
    maxThreads        -> at all times no more than <maxThreads> workers will be alive
    maxQueuedRequests -> No more than <maxQueuedRequests> can be waiting to be executed
                         If another request is added to the ThreadPool, the thread will
                         lock until another request is taken out of the queue.

The ThreadPool will automatically increase and decrease the pool of workers as needed

To add requests to the queue::

     threadPool.generateJobAndQueueIt( <functionToExecute>,
                                       args = ( arg1, arg2, ... ),
                                       oCallback = <resultCallbackFunction> )

or::

     request = ThreadedJob( <functionToExecute>,
                            args = ( arg1, arg2, ... )
                            oCallback = <resultCallbackFunction> )
     threadPool.queueJob( request )

The result callback and the parameters are optional arguments.
Once the requests have been added to the pool. They will be executed as soon as possible.
Worker threads automatically return the return value of the requests. To run the result callback
functions execute::

   threadPool.generateJobAndQueueIt( <functionToExecute>,
                                     args = ( arg1, arg2, ... ),
                                     oCallback = <resultCallbackFunction> )

or::

   request = ThreadedJob( <functionToExecute>,
                          args = ( arg1, arg2, ... )
                          oCallback = <resultCallbackFunction> )
   threadPool.queueJob( request )

The result callback and the parameters are optional arguments.
Once the requests have been added to the pool. They will be executed as soon as possible.
Worker threads automatically return the return value of the requests. To run the result callback
functions execute::

   threadPool.processRequests()

This method will process the existing return values of the requests. Even if the requests do not return
anything this method (or any process result method) has to be called to clean the result queues.

To wait until all the requests are finished and process their result call::

   threadPool.processAllRequests()

This function will block until all requests are finished and their result values have been processed.

It is also possible to set the threadPool in auto processing results mode. It'll process the results as
soon as the requests have finished. To enable this mode call::

   threadPool.daemonize()

"""
import time
import sys
import queue
import threading

try:
    from DIRAC.FrameworkSystem.Client.Logger import gLogger
except Exception:
    gLogger = False
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR


class WorkingThread(threading.Thread):
    def __init__(self, oPendingQueue, oResultsQueue, **kwargs):
        threading.Thread.__init__(self, **kwargs)
        self.daemon = True
        self.__pendingQueue = oPendingQueue
        self.__resultsQueue = oResultsQueue
        self.__threadAlive = True
        self.__working = False
        self.start()

    def isWorking(self):
        return self.__working

    def kill(self):
        self.__threadAlive = False

    def run(self):
        while self.__threadAlive:
            oJob = self.__pendingQueue.get(block=True)
            if not self.__threadAlive:
                self.__pendingQueue.put(oJob)
                break
            self.__working = True
            oJob.process()
            self.__working = False
            if oJob.hasCallback():
                self.__resultsQueue.put(oJob, block=True)


class ThreadedJob:
    def __init__(self, oCallable, args=None, kwargs=None, sTJId=None, oCallback=None, oExceptionCallback=None):
        self.__jobFunction = oCallable
        self.__jobArgs = args or []
        self.__jobKwArgs = kwargs or {}
        self.__tjID = sTJId
        self.__resultCallback = oCallback
        self.__exceptionCallback = oExceptionCallback
        self.__done = False
        self.__exceptionRaised = False
        self.__jobResult = None
        self.__jobException = None

    def __showException(self, threadedJob, exceptionInfo):
        if gLogger:
            gLogger.exception("Exception in thread", lExcInfo=exceptionInfo)

    def jobId(self):
        return self.__tjID

    def hasCallback(self):
        return self.__resultCallback or self.__exceptionCallback

    def exceptionRaised(self):
        return self.__exceptionRaised

    def doExceptionCallback(self):
        if self.__done and self.__exceptionRaised and self.__exceptionCallback:
            self.__exceptionCallback(self, self.__jobException)

    def doCallback(self):
        if self.__done and not self.__exceptionRaised and self.__resultCallback:
            self.__resultCallback(self, self.__jobResult)

    def process(self):
        self.__done = True
        try:
            self.__jobResult = self.__jobFunction(*self.__jobArgs, **self.__jobKwArgs)
        except Exception as lException:
            self.__exceptionRaised = True
            if not self.__exceptionCallback:
                if gLogger:
                    gLogger.exception("Exception in thread", lException=lException)
            else:
                self.__jobException = sys.exc_info()


class ThreadPool(threading.Thread):
    def __init__(self, iMinThreads, iMaxThreads=0, iMaxQueuedRequests=0, strictLimits=True):
        threading.Thread.__init__(self)
        if iMinThreads < 1:
            self.__minThreads = 1
        else:
            self.__minThreads = iMinThreads
        if iMaxThreads < self.__minThreads:
            self.__maxThreads = self.__minThreads
        else:
            self.__maxThreads = iMaxThreads
        self.__strictLimits = strictLimits
        self.__pendingQueue = queue.Queue(iMaxQueuedRequests)
        self.__resultsQueue = queue.Queue(iMaxQueuedRequests + iMaxThreads)
        self.__workingThreadsList = []
        self.__spawnNeededWorkingThreads()

    def getMaxThreads(self):
        return self.__maxThreads

    def getMinThreads(self):
        return self.__minThreads

    def numWorkingThreads(self):
        return self.__countWorkingThreads()

    def numWaitingThreads(self):
        return self.__countWaitingThreads()

    def __spawnWorkingThread(self):
        self.__workingThreadsList.append(WorkingThread(self.__pendingQueue, self.__resultsQueue))

    def __killWorkingThread(self):
        if self.__strictLimits:
            for i in range(len(self.__workingThreadsList)):
                wT = self.__workingThreadsList[i]
                if not wT.isWorking():
                    wT.kill()
                    del self.__workingThreadsList[i]
                    break
        else:
            self.__workingThreadsList[0].kill()
            del self.__workingThreadsList[0]

    def __countWaitingThreads(self):
        iWaitingThreads = 0
        for oWT in self.__workingThreadsList:
            if not oWT.isWorking():
                iWaitingThreads += 1
        return iWaitingThreads

    def __countWorkingThreads(self):
        iWorkingThreads = 0
        for oWT in self.__workingThreadsList:
            if oWT.isWorking():
                iWorkingThreads += 1
        return iWorkingThreads

    def __spawnNeededWorkingThreads(self):
        while len(self.__workingThreadsList) < self.__minThreads:
            self.__spawnWorkingThread()
        while self.__countWaitingThreads() == 0 and len(self.__workingThreadsList) < self.__maxThreads:
            self.__spawnWorkingThread()

    def __killExceedingWorkingThreads(self):
        threadsToKill = len(self.__workingThreadsList) - self.__maxThreads
        for _ in range(max(threadsToKill, 0)):
            self.__killWorkingThread()
        threadsToKill = self.__countWaitingThreads() - self.__minThreads
        for _ in range(max(threadsToKill, 0)):
            self.__killWorkingThread()

    def queueJob(self, oTJob, blocking=True):
        if not isinstance(oTJob, ThreadedJob):
            raise TypeError("Jobs added to the thread pool must be ThreadedJob instances")
        try:
            self.__pendingQueue.put(oTJob, block=blocking)
        except queue.Full:
            return S_ERROR("Queue is full")
        return S_OK()

    def generateJobAndQueueIt(
        self, oCallable, args=None, kwargs=None, sTJId=None, oCallback=None, oExceptionCallback=None, blocking=True
    ):
        oTJ = ThreadedJob(oCallable, args, kwargs, sTJId, oCallback, oExceptionCallback)
        return self.queueJob(oTJ, blocking)

    def pendingJobs(self):
        return self.__pendingQueue.qsize()

    def isFull(self):
        return self.__pendingQueue.full()

    def isWorking(self):
        return not self.__pendingQueue.empty() or self.__countWorkingThreads()

    def processResults(self):
        iProcessed = 0
        while True:
            self.__spawnNeededWorkingThreads()
            if self.__resultsQueue.empty():
                self.__killExceedingWorkingThreads()
                break
            oJob = self.__resultsQueue.get()
            oJob.doExceptionCallback()
            oJob.doCallback()
            iProcessed += 1
            self.__killExceedingWorkingThreads()
        return iProcessed

    def processAllResults(self):
        while not self.__pendingQueue.empty() or self.__countWorkingThreads():
            self.processResults()
            time.sleep(0.1)
        self.processResults()

    def daemonize(self):
        self.daemon = True
        self.start()

    # This is the ThreadPool threaded function. YOU ARE NOT SUPPOSED TO CALL THIS FUNCTION!!!
    def run(self):
        while True:
            self.processResults()
            time.sleep(1)


gThreadPool = False


def getGlobalThreadPool():
    global gThreadPool
    if not gThreadPool:
        gThreadPool = ThreadPool(1, 500)
        gThreadPool.daemonize()
    return gThreadPool


if __name__ == "__main__":
    import random

    def doSomething(iNumber):
        time.sleep(random.randint(1, 5))
        fResult = random.random() * iNumber
        if fResult > 3:
            raise Exception("TEST EXCEPTION")
        return fResult

    def showResult(oTJ, fResult):
        print(f"Result {fResult} from {oTJ}")

    def showException(oTJ, exc_info):
        print(f"Exception {exc_info[1]} from {oTJ}")

    OTP = ThreadPool(5, 10)

    def generateWork(iWorkUnits):
        for iNumber in [random.randint(1, 20) for _ in range(iWorkUnits)]:
            oTJ = ThreadedJob(doSomething, args=(iNumber,), oCallback=showResult, oExceptionCallback=showException)
            OTP.queueJob(oTJ)

    print("MaxThreads =", OTP.getMaxThreads())
    print("MinThreads =", OTP.getMinThreads())

    generateWork(30)
    while True:
        time.sleep(1)
        gIResult = OTP.processResults()
        gINew = gIResult + random.randint(-3, 2)
        print(f"Processed {gIResult}, generating {gINew}..")
        generateWork(gINew)
        print(f"Threads {OTP.numWorkingThreads()}", OTP.pendingJobs())
