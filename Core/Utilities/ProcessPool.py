#################################################################
# $HeadURL:  $
#################################################################

__RCSID__ = "$Id:  $"

import multiprocessing
import sys
import time

from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
#def S_OK():
#  pass
#def S_ERROR(m):
#  return {}

PROCESS_IDLE = 0
PROCESS_WORKING = 1
PROCESS_NONE = 3

class WorkingProcess(multiprocessing.Process):
  
  def __init__( self, pendingQueue, resultsQueue, statusArray, processNumber, **kwargs ):
    multiprocessing.Process.__init__( self, **kwargs )
    self.daemon = True
    self.__pendingQueue = pendingQueue
    self.__resultsQueue = resultsQueue
    self.__processAlive = True
    self.__processNumber = processNumber 
    self.__status = statusArray
    self.__status[self.__processNumber] = PROCESS_IDLE
    self.start()

  def isWorking( self ):
    return self.__status[self.__processNumber] == PROCESS_WORKING

  def kill( self ):
    self.__processAlive = False
    self.terminate()
    self.__status[self.__processNumber] = PROCESS_NONE

  def run( self ):
    while self.__processAlive:
      print "Attempt to pick up task for process %s" % self.__processNumber
      task = self.__pendingQueue.get( block = True )
      if not self.__processAlive:
        self.__pendingQueue.put( task )
        break
      self.__status[self.__processNumber] = PROCESS_WORKING
      task.process()
      self.__status[self.__processNumber] = PROCESS_IDLE
      if task.hasCallback():
        self.__resultsQueue.put( task, block = True )

class ProcessTask:

  def __init__( self,
                callable,
                args = None,
                kwargs = None,
                taskID = None,
                callback = None,
                exceptionCallback = None ):
    self.__taskFunction = callable
    self.__taskArgs = args or []
    self.__taskKwArgs = kwargs or {}
    self.__taskID = taskID
    self.__resultCallback = callback
    self.__exceptionCallback = exceptionCallback
    self.__done = False
    self.__exceptionRaised = False

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
    except Exception,x:
      self.__exceptionRaised = True
      if self.__exceptionCallback:
        result = S_ERROR('Exception')
        result['Value'] = str(x)
        result['Exc_info'] = sys.exc_info()[1]
        self.__taskException = result
        
class ProcessPool( multiprocessing.Process ):

  def __init__( self, minSize = 2, maxSize=0, maxQueuedRequests = 10, strictLimits = True, **kwargs ):
    multiprocessing.Process.__init__( self, **kwargs )
    if minSize < 1:
      self.__minSize = 1
    else:
      self.__minSize = minSize
    if maxSize < self.__minSize:
      self.__maxSize = self.__minSize
    else:
      self.__maxSize = maxSize
    self.__maxQueuedRequests = maxQueuedRequests
    self.__strictLimits = strictLimits
    self.__pendingQueue = multiprocessing.Queue( self.__maxQueuedRequests )
    self.__resultsQueue = multiprocessing.Queue( 0 )
    self.__statusArray = multiprocessing.Array('I',self.__maxSize)
    self.__workingProcessList = []
    self.__spawnNeededWorkingProcesses()

  def getMaxSize( self ):
    return self.__maxSize

  def getMinSize( self ):
    return self.__minSize

  def getNumWorkingProcesses( self ):
    count = 0
    for p in self.__workingProcessList:
      if p.isWorking():
        count += 1
    return count     
  
  def getNumIdleProcesses( self ):
    count = 0
    for p in self.__workingProcessList:
      if not p.isWorking():
        count += 1
    return count   

  def getFreeSlots( self ):
    return self.getNumIdleProcesses()

  def __spawnWorkingProcess( self ):
    # Get the process number
    nProcess = -1
    for i in range(len(self.__statusArray)):
      if self.__statusArray[i] == PROCESS_NONE:
        nProcess = -1
        break
    if nProcess == -1:
      nProcess = len(self.__workingProcessList)  
    p = WorkingProcess( self.__pendingQueue, 
                        self.__resultsQueue, 
                        self.__statusArray, 
                        nProcess ) 
    self.__workingProcessList.append( p )
    
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
    while self.getNumIdleProcesses() == 0 and len( self.__workingProcessList ) < self.__maxSize:
      self.__spawnWorkingProcess()

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
    except multiprocessing.Queue.Full:
      return S_ERROR( "Queue is full" )
    # Throttle a bit to allow task state propagation
    time.sleep(0.01)
    return S_OK()

  def createAndQueueTask( self,
                             callable,
                             args = None,
                             kwargs = None,
                             taskID = None,
                             callback = None,
                             exceptionCallback = None,
                             blocking = True ):
    task = ProcessTask( callable, args, kwargs, taskID, callback, exceptionCallback )
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
      time.sleep(0.1)
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
    self.daemon = True
    self.start()

  def run( self ):
    while True:
      self.processResults()
      time.sleep( 1 )  
    
if __name__ == "__main__":
  
  import random
  import time

  def doSomething( number, r ):
    r = random.randint( 1, 5 )
    print "sleeping %s secs for task number %s" % (r,number)
    time.sleep( r )
    
    result = random.random() * number
    if result > 3:
      print "raising exception for task %s" % number
      raise Exception( "TEST EXCEPTION" )
    print "task number %s done" % number
    return result

  def showResult( task, result ):
    print "Result %s from %s" % ( result, task )

  def showException( task, exc_info ):
    print "Exception %s from %s" % ( exc_info, task )
    
  pp = ProcessPool(20)
  
  count = 0
  rmax = 0
  while count < 20:
    if pp.getFreeSlots() > 0 and not pp.hasPendingTasks():
      print "spawning task %d" % count
      r = random.randint( 1, 5 )
      if r > rmax: rmax = r
      result = pp.createAndQueueTask(doSomething,args=(count,r,),callback=showResult,exceptionCallback=showException)
      count += 1
    else:
      print "no free slots"  
      time.sleep(1)
      
  pp.processAllResults()
  
  print "Max sleep", rmax      
