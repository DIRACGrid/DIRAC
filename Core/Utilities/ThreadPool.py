#################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/ThreadPool.py,v 1.11 2008/12/09 17:39:28 acasajus Exp $
#################################################################

__RCSID__ = "$Id: ThreadPool.py,v 1.11 2008/12/09 17:39:28 acasajus Exp $"

import time
import sys
import Queue
import threading
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

class WorkingThread( threading.Thread ):

  def __init__( self, oPendingQueue, oResultsQueue, **kwargs ):
    threading.Thread.__init__( self, **kwargs )
    self.setDaemon(1)
    self.__pendingQueue = oPendingQueue
    self.__resultsQueue = oResultsQueue
    self.__threadAlive = True
    self.__working = False
    self.start()

  def isWorking(self):
    return self.__working

  def kill( self ):
    self.__threadAlive = False

  def run( self ):
    while self.__threadAlive:
      oJob = self.__pendingQueue.get( block = True )
      if not self.__threadAlive:
        self.__pendingQueue.put( oJob )
        break
      self.__working = True
      oJob.process()
      self.__working = False
      if oJob.hasCallback():
        self.__resultsQueue.put( oJob, block = True )


class ThreadedJob:

  def __init__( self,
                oCallable,
                args = None,
                kwargs = None,
                sTJId = None,
                oCallback = None,
                oExceptionCallback = None ):
    self.__jobFunction = oCallable
    self.__jobArgs = args or []
    self.__jobKwArgs = kwargs or {}
    self.__tjID = sTJId
    self.__resultCallback = oCallback
    self.__exceptionCallback = oExceptionCallback
    self.__done = False
    self.__exceptionRaised = False

  def jobId( self ):
    return self.__tjID

  def hasCallback( self ):
    return self.__resultCallback or self.__exceptionCallback

  def exceptionRaised( self ):
    return self.__exceptionRaised

  def doExceptionCallback( self ):
    if self.__done and self.__exceptionRaised and self.__exceptionCallback:
      self.__exceptionCallback( self, self.__jobException )

  def doCallback( self ):
    if self.__done and not self.__exceptionRaised and self.__resultCallback:
      self.__resultCallback( self, self.__jobResult )

  def process( self ):
    self.__done = True
    try:
      self.__jobResult = self.__jobFunction( *self.__jobArgs, **self.__jobKwArgs)
    except:
      self.__exceptionRaised = True
      self.__jobException = sys.exc_info()

class ThreadPool( threading.Thread ):

  def __init__( self, iMinThreads, iMaxThreads = 0, iMaxQueuedRequests = 0 ):
    threading.Thread.__init__( self )
    if iMinThreads < 1:
      self.__minThreads = 1
    else:
      self.__minThreads = iMinThreads
    if iMaxThreads < self.__minThreads:
      self.__maxThreads = self.__minThreads
    else:
      self.__maxThreads = iMaxThreads
    self.__pendingQueue = Queue.Queue( iMaxQueuedRequests )
    self.__resultsQueue = Queue.Queue( iMaxQueuedRequests + iMaxThreads )
    self.__workingThreadsList = []
    self.__spawnNeededWorkingThreads()

  def getMaxThreads( self ):
    return self.__maxThreads

  def getMinThreads( self ):
    return self.__minThreads

  def numWorkingThreads( self ):
    return self.__countWorkingThreads()

  def __spawnWorkingThread( self ):
    self.__workingThreadsList.append( WorkingThread( self.__pendingQueue, self.__resultsQueue ) )

  def __killWorkingThread( self ):
    self.__workingThreadsList[0].kill()
    del( self.__workingThreadsList[0] )

  def __countWaitingThreads(self ):
    iWaitingThreads = 0
    for oWT in self.__workingThreadsList:
      if not oWT.isWorking():
        iWaitingThreads += 1
    return iWaitingThreads

  def __countWorkingThreads(self ):
    iWorkingThreads = 0
    for oWT in self.__workingThreadsList:
      if oWT.isWorking():
        iWorkingThreads += 1
    return iWorkingThreads

  def __spawnNeededWorkingThreads( self ):
    while len( self.__workingThreadsList ) < self.__minThreads:
      self.__spawnWorkingThread()
    while self.__countWaitingThreads() == 0 and \
          len( self.__workingThreadsList ) < self.__maxThreads:
      self.__spawnWorkingThread()

  def __killExceedingWorkingThreads( self ):
    while len( self.__workingThreadsList ) > self.__maxThreads:
      self.__killWorkingThread()
    while self.__countWaitingThreads() > self.__minThreads:
      self.__killWorkingThread()

  def queueJob( self, oTJob, blocking = True ):
    if not isinstance( oTJob, ThreadedJob ):
      raise TypeError( "Jobs added to the thread pool must be ThreadedJob instances" )
    try:
      self.__pendingQueue.put( oTJob, block = blocking )
    except Queue.Full:
      return S_ERROR( "Queue is full" )
    return S_OK()

  def generateJobAndQueueIt( self,
                             oCallable,
                             args = None,
                             kwargs = None,
                             sTJId = None,
                             oCallback = None,
                             oExceptionCallback = None,
                             blocking = True ):
    oTJ = ThreadedJob( oCallable, args, kwargs, sTJId, oCallback, oExceptionCallback )
    return self.queueJob( oTJ, blocking )

  def pendingJobs( self ):
    return self.__pendingQueue.qsize()

  def isFull( self ):
    return self.__pendingQueue.full()

  def isWorking( self ):
    return not self.__pendingQueue.empty() or self.__countWorkingThreads()

  def processResults( self ):
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

  def processAllResults( self ):
    while not self.__pendingQueue.empty() or self.__countWorkingThreads():
      self.processResults()
      time.sleep( 0.1 )
    self.processResults()

  def daemonize( self ):
    self.setDaemon(1)
    self.start()

  #This is the ThreadPool threaded function. YOU ARE NOT SUPPOSED TO CALL THIS FUNCTION!!!
  def run( self ):
    import time
    while True:
      self.processResults()
      time.sleep( 1 )

if __name__=="__main__":
  import random
  import time

  def doSomething( iNumber ):
    time.sleep( random.randint( 1, 5 ) )
    fResult = random.random() * iNumber
    if fResult > 3:
      raise Exception( "TEST EXCEPTION" )
    return fResult

  def showResult( oTJ, fResult ):
    print "Result %s from %s" % ( fResult, oTJ )

  def showException( oTJ, exc_info ):
    print "Exception %s from %s" % ( exc_info[1], oTJ )

  oTP = ThreadPool( 5, 10 )

  def generateWork( iWorkUnits ):
    for iNumber in [ random.randint( 1,20 ) for uNothing in range( iWorkUnits ) ]:
      oTJ = ThreadedJob( doSomething,
                       args = ( iNumber, ),
                       oCallback = showResult,
                       oExceptionCallback = showException )
      oTP.queueJob( oTJ )

  print 'MaxThreads =', oTP.getMaxThreads()
  print 'MinThreads =', oTP.getMinThreads()

  generateWork( 30 )
  while True:
    time.sleep(1)
    iResult = oTP.processResults()
    iNew = iResult + random.randint(-3,2)
    print "Processed %s, generating %s.." % ( iResult, iNew )
    generateWork( iNew )
    print "Threads %s" % oTP.numWorkingThreads(), oTP.pendingJobs()
