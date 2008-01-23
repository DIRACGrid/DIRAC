#################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/ThreadPool.py,v 1.9 2008/01/23 12:09:42 acasajus Exp $
#################################################################

__RCSID__ = "$Id: ThreadPool.py,v 1.9 2008/01/23 12:09:42 acasajus Exp $"

import time
import sys
import Queue
import threading
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

class WorkingThread( threading.Thread ):

  def __init__( self, oPendingQueue, oResultsQueue, **kwargs ):
    threading.Thread.__init__( self, **kwargs )
    self.setDaemon(1)
    self.oPendingQueue = oPendingQueue
    self.oResultsQueue = oResultsQueue
    self.bAlive = True
    self.bWorking = False
    self.start()

  def isWorking(self):
    return self.bWorking

  def kill( self ):
    self.bAlive = False

  def run( self ):
    while self.bAlive:
      oJob = self.oPendingQueue.get( block = True )
      if not self.bAlive:
        self.oPendingQueue.put( oJob )
        break
      self.bWorking = True
      oJob.process()
      self.bWorking = False
      if oJob.oCallback or oJob.oExceptionCallback:
        self.oResultsQueue.put( oJob, block = True )


class ThreadedJob:

  def __init__( self,
                oCallable,
                args = None,
                kwargs = None,
                sTJId = None,
                oCallback = None,
                oExceptionCallback = None ):
    self.oCallable = oCallable
    self.args = args or []
    self.kwargs = kwargs or {}
    self.sTJId = sTJId
    self.oCallback = oCallback
    self.oExceptionCallback = oExceptionCallback
    self.bProcessed = False
    self.bExceptionRaised = False

  def jobId( self ):
    return self.sTJId

  def exceptionRaised( self ):
    return self.bExceptionRaised

  def doExceptionCallback( self ):
    if self.bProcessed and self.bExceptionRaised and self.oExceptionCallback:
      self.oExceptionCallback( self, self.uException )

  def doCallback( self ):
    if self.bProcessed and not self.bExceptionRaised and self.oCallback:
      self.oCallback( self, self.uResult )

  def process( self ):
    self.bProcessed = True
    try:
      self.uResult = self.oCallable( *self.args, **self.kwargs)
    except:
      self.bExceptionRaised = True
      self.uException = sys.exc_info()

class ThreadPool( threading.Thread ):

  def __init__( self, iMinThreads, iMaxThreads = 0, iMaxQueuedRequests = 0 ):
    threading.Thread.__init__( self )
    if iMinThreads < 1:
      self.iMinThreads = 1
    else:
      self.iMinThreads = iMinThreads
    if iMaxThreads < self.iMinThreads:
      self.iMaxThreads = self.iMinThreads
    else:
      self.iMaxThreads = iMaxThreads
    self.oPendingQueue = Queue.Queue( iMaxQueuedRequests )
    self.oResultsQueue = Queue.Queue( iMaxQueuedRequests + iMaxThreads )
    self.lWorkingThreads = []
    self.__spawnNeededWorkingThreads()

  def numWorkingThreads( self ):
    return self.__countWorkingThreads()

  def __spawnWorkingThread( self ):
    self.lWorkingThreads.append( WorkingThread( self.oPendingQueue, self.oResultsQueue ) )

  def __killWorkingThread( self ):
    self.lWorkingThreads[0].kill()
    del( self.lWorkingThreads[0] )

  def __countWaitingThreads(self ):
    iWaitingThreads = 0
    for oWT in self.lWorkingThreads:
      if not oWT.isWorking():
        iWaitingThreads += 1
    return iWaitingThreads

  def __countWorkingThreads(self ):
    iWorkingThreads = 0
    for oWT in self.lWorkingThreads:
      if oWT.isWorking():
        iWorkingThreads += 1
    return iWorkingThreads

  def __spawnNeededWorkingThreads( self ):
    while len( self.lWorkingThreads ) < self.iMinThreads:
      self.__spawnWorkingThread()
    while self.__countWaitingThreads() == 0 and \
          len( self.lWorkingThreads ) < self.iMaxThreads:
      self.__spawnWorkingThread()

  def __killExceedingWorkingThreads( self ):
    while len( self.lWorkingThreads ) > self.iMaxThreads:
      self.__killWorkingThread()
    while self.__countWaitingThreads() > self.iMinThreads:
      self.__killWorkingThread()

  def queueJob( self, oTJob, blocking = True ):
    if not isinstance( oTJob, ThreadedJob ):
      raise TypeError( "Jobs added to the thread pool must be ThreadedJob instances" )
    try:
      self.oPendingQueue.put( oTJob, block = blocking )
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
    return self.oPendingQueue.qsize()

  def isFull( self ):
    return self.oPendingQueue.full()

  def processResults( self ):
    iProcessed = 0
    while True:
      self.__spawnNeededWorkingThreads()
      if self.oResultsQueue.empty():
        self.__killExceedingWorkingThreads()
        break
      oJob = self.oResultsQueue.get()
      oJob.doExceptionCallback()
      oJob.doCallback()
      iProcessed += 1
      self.__killExceedingWorkingThreads()
    return iProcessed

  def processAllResults( self ):
    while not self.oPendingQueue.empty() or self.__countWorkingThreads():
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

  print 'MaxThreads =', oTP.iMaxThreads
  print 'MinThreads =', oTP.iMinThreads

  generateWork( 30 )
  while True:
    time.sleep(1)
    iResult = oTP.processResults()
    iNew = iResult + random.randint(-2,2)
    print "Processed %s, generating %s.." % ( iResult, iNew )
    generateWork( iNew )
    print "Threads %s" % oTP.numWorkingThreads(), oTP.pendingJobs()
