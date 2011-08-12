#################################################################
# $HeadURL$
#################################################################

__RCSID__ = "$Id$"

import multiprocessing
import Queue
import sys
import time
import threading

try:
  from DIRAC.FrameworkSystem.Client.Logger import gLogger
except ImportError:
  gLogger = False

try:
  from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
except ImportError:
  def S_OK( val = "" ):
    return { 'OK' : True, 'Value' : val }
  def S_ERROR( mess ):
    return { 'OK' : False, 'Message' : mess }

class WorkingProcess( multiprocessing.Process ):

  def __init__( self, pendingQueue, resultsQueue ):
    multiprocessing.Process.__init__( self )
    self.daemon = True
    self.__working = multiprocessing.Value( 'i', 0 )
    self.__alive = multiprocessing.Value( 'i', 1 )
    self.__pendingQueue = pendingQueue
    self.__resultsQueue = resultsQueue
    self.start()

  def isWorking( self ):
    return self.__working.value == 1

  def kill( self ):
    self.__alive.value = 0

  def run( self ):
    while self.__alive.value:
      self.__working.value = 0
      task = self.__pendingQueue.get( block = True )
      self.__working.value = 1
      if not self.__alive.value:
        self.__pendingQueue.put( task )
        break
      task.process()
      if task.hasCallback():
        self.__resultsQueue.put( task, block = True )


class ProcessTask:

  def __init__( self,
                taskFunction,
                args = None,
                kwargs = None,
                taskID = None,
                callback = None,
                exceptionCallback = None ):
    self.__taskFunction = taskFunction
    self.__taskArgs = args or []
    self.__taskKwArgs = kwargs or {}
    self.__taskID = taskID
    self.__resultCallback = callback
    self.__exceptionCallback = exceptionCallback
    self.__done = False
    self.__exceptionRaised = False
    self.__taskException = None
    self.__taskResult = None

  def getTaskID( self ):
    return self.__taskID

  def hasCallback( self ):
    return self.__resultCallback or self.__exceptionCallback

  def exceptionRaised( self ):
    return self.__exceptionRaised

  def doExceptionCallback( self ):
    if self.__done and self.__exceptionRaised and self.__exceptionCallback:
      self.__exceptionCallback( self, self.__taskException )

  def doCallback( self ):
    if self.__done and not self.__exceptionRaised and self.__resultCallback:
      self.__resultCallback( self, self.__taskResult )

  def process( self ):
    self.__done = True
    try:
      self.__taskResult = self.__taskFunction( *self.__taskArgs, **self.__taskKwArgs )
    except Exception, x:
      self.__exceptionRaised = True

      if not self.__exceptionCallback and gLogger:
        gLogger.exception( "Exception in process of pool " )

      if self.__exceptionCallback:
        retDict = S_ERROR( 'Exception' )
        retDict['Value'] = str( x )
        retDict['Exc_info'] = sys.exc_info()[1]
        self.__taskException = retDict

class ProcessPool:

  def __init__( self, minSize = 2, maxSize = 0, maxQueuedRequests = 10, strictLimits = True ):
    self.__minSize = max( 1, minSize )
    self.__maxSize = max( self.__minSize, maxSize )
    self.__maxQueuedRequests = maxQueuedRequests
    self.__strictLimits = strictLimits
    self.__pendingQueue = multiprocessing.Queue( self.__maxQueuedRequests )
    self.__resultsQueue = multiprocessing.Queue( 0 )
    self.__workingProcessList = []
    self.__daemonProcess = False
    self.__spawnNeededWorkingProcesses()

  def getMaxSize( self ):
    return self.__maxSize

  def getMinSize( self ):
    return self.__minSize

  def getNumWorkingProcesses( self ):
    counter = 0
    for proc in self.__workingProcessList:
      if proc.isWorking():
        counter += 1
    return counter

  def getNumIdleProcesses( self ):
    counter = 0
    for proc in self.__workingProcessList:
      if not proc.isWorking():
        counter += 1
    return counter

  def getFreeSlots( self ):
    return max( 0, self.__maxSize - self.getNumWorkingProcesses() )

  def __spawnWorkingProcess( self ):
    self.__workingProcessList.append( WorkingProcess( self.__pendingQueue, self.__resultsQueue ) )

  def __killWorkingProcess( self ):
    if self.__strictLimits:
      for i in range( len( self.__workingProcessList ) ):
        process = self.__workingProcessList[i]
        if not process.isWorking():
          process.kill()
          del( self.__workingProcessList[i] )
          break
    else:
      self.__workingProcessList[0].kill()
      del( self.__workingProcessList[0] )

  def __spawnNeededWorkingProcesses( self ):
    while len( self.__workingProcessList ) < self.__minSize:
      self.__spawnWorkingProcess()

    while self.hasPendingTasks() and \
          self.getNumIdleProcesses() == 0 and \
          len( self.__workingProcessList ) < self.__maxSize:
      self.__spawnWorkingProcess()
      time.sleep( 0.1 )

  def __killExceedingWorkingProcesses( self ):
    toKill = len( self.__workingProcessList ) - self.__maxSize
    for i in range ( max( toKill, 0 ) ):
      self.__killWorkingProcess()
    toKill = self.getNumIdleProcesses() - self.__minSize
    for i in range ( max( toKill, 0 ) ):
      self.__killWorkingProcess()

  def queueTask( self, task, blocking = True ):
    if not isinstance( task, ProcessTask ):
      raise TypeError( "Tasks added to the process pool must be ProcessTask instances" )
    try:
      self.__pendingQueue.put( task, block = blocking )
    except Queue.Full:
      return S_ERROR( "Queue is full" )
    # Throttle a bit to allow task state propagation
    time.sleep( 0.01 )
    return S_OK()

  def createAndQueueTask( self,
                             taskFunction,
                             args = None,
                             kwargs = None,
                             taskID = None,
                             callback = None,
                             exceptionCallback = None,
                             blocking = True ):
    task = ProcessTask( taskFunction, args, kwargs, taskID, callback, exceptionCallback )
    return self.queueTask( task, blocking )

  def hasPendingTasks( self ):
    return not self.__pendingQueue.empty()

  def isFull( self ):
    return self.__pendingQueue.full()

  def isWorking( self ):
    return not self.__pendingQueue.empty() or self.getNumWorkingProcesses()

  def processResults( self ):
    processed = 0
    while True:
      self.__spawnNeededWorkingProcesses()
      time.sleep( 0.1 )
      if self.__resultsQueue.empty():
        self.__killExceedingWorkingProcesses()
        break
      task = self.__resultsQueue.get()
      task.doExceptionCallback()
      task.doCallback()
      self.__killExceedingWorkingProcesses()
      processed += 1
    return processed

  def processAllResults( self ):
    while not self.__pendingQueue.empty() or self.getNumWorkingProcesses():
      self.processResults()
      time.sleep( 0.1 )
    self.processResults()

  def daemonize( self ):
    if self.__daemonProcess:
      return
    self.__daemonProcess = threading.Thread( target = self.__backgroundProcess )
    self.__daemonProcess.setDaemon( 1 )
    self.__daemonProcess.start()

  def __backgroundProcess( self ):
    while True:
      self.processResults()
      time.sleep( 1 )

if __name__ == "__main__":

  import random

  def doSomething( number, r ):
    rnd = random.randint( 1, 5 )
    print "sleeping %s secs for task number %s" % ( rnd, number )
    time.sleep( rnd )

    rnd = random.random() * number
    if rnd > 3:
      print "raising exception for task %s" % number
      raise Exception( "TEST EXCEPTION" )
    print "task number %s done" % number
    return rnd

  def showResult( task, ret ):
    print "Result %s from %s" % ( ret, task )

  def showException( task, exc_info ):
    print "Exception %s from %s" % ( exc_info, task )

  pPool = ProcessPool( 1, 20 )
  pPool.daemonize()

  count = 0
  rmax = 0
  while count < 20:
    print "FREE SLOTS", pPool.getFreeSlots()
    print "PENDING", pPool.hasPendingTasks()
    if pPool.getFreeSlots() > 0:
      print "spawning task %d" % count
      r = random.randint( 1, 5 )
      if r > rmax:
        rmax = r
      result = pPool.createAndQueueTask( doSomething, args = ( count, r, ),
                                      callback = showResult,
                                      exceptionCallback = showException )
      count += 1
    else:
      print "no free slots"
      time.sleep( 1 )

  pPool.processAllResults()

  print "Max sleep", rmax
