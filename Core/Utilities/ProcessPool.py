#################################################################
# $HeadURL$
#################################################################
""" :mod: ProcessPool 
    =================
    .. module: ProcessPool
    :synopsis: ProcessPool and related classes

ProcessPool
-----------

ProcessPool creates a pool of worker subprocesses to handle a queue of tasks
much like the producers/consumers paradigm. Users just need to fill the queue
with tasks to be executed and worker tasks will execute them.

To construct ProcessPool one first should call its contructor::

  pool = ProcessPool( minSize, maxSize, maxQueuedRequests )

where parameters are::

:param int minSize: at least <minSize> workers will be alive all the time
:param int maxSize: no more than <maxSize> workers will be alive all the time
:param int maxQueuedRequests: size for request waiting in a queue to be executed  

In case another request is added to the full queue, the execution will
lock until another request is taken out. The ProcessPool will automatically increase and 
decrease the pool of workers as needed, of course not exceeding above limits.

To add a task to the queue one should execute::

  pool.createAndQueueTask( funcDef,
                           args = ( arg1, arg2, ... ),
                           kwargs = { "kwarg1" : value1, "kwarg2" : value2 },
                           callback = callbackDef,
                           exceptionCallback = exceptionCallBackDef )

or alternatively by using ProcessTask instance::

  task = ProcessTask( funcDef ,
                      args = ( arg1, arg2, ... )
                      kwargs = { "kwarg1" : value1, .. },
                      callback = callbackDef,
                      exceptionCallback = exceptionCallbackDef )
  pool.queueTask( task )

where parameters are::

  :param funcDef: callable py object definition (function, lambda, class with __call__ slot defined
  :param list args: argument list
  :param dict kwargs: keyword arguments dictionary
  :param callback: callback function definition
  :param exceptionCallback: exception callback function definition

The callback, exceptionCallbaks and the parameters are all optional. Once task has been added to the pool, 
it will be executed as soon as possible. Worker subprocesses automatically return the return value of the task. 
To obtain those results one has to execute::

  pool.processRequests()

This method will process the existing return values of the task, even if the task does not return
anything. This method has to be called to clean the result queues. To wait until all the requests are finished 
and process their result call::

 pool.processAllRequests()

This function will block until all requests are finished and their result values have been processed.

It is also possible to set the ProcessPool in daemon mode, in which all results are automatically 
processed as soon they are available, just after finalisation of task execution. To enable this mode one 
has to call::

  pool.daemonize()

Callback functions
------------------

There are two types of callbacks that can be executed for each tasks: exception callback function and 
results callback function. The the firts one is executed when unhandled excpetion has been raised during 
task processing, and hence no task results are available, otherwise the execution of second callback type 
is performed. 
 
The callbacks could be attached in a two places::

- directly in ProcessTask, in that case those have to be shelvable/picklable, so they should be defined as 
  global functions with the signature :callback( task, taskResult ): where :task: is a :ProcessTask: 
  reference and :taskResult: is whatever task callable it returning for restuls callback and 
  :exceptionCallback( task, exc_info): where exc_info is a 
  :S_ERROR{ "Exception": { "Value" : exceptionName, "Exc_info" : exceptionInfo }:

- in ProcessPool, in that case there is no limitation on the function type, except the signature, which
  should follow :callback( task ): or :exceptionCallback( task ):, as those callbacks definitions 
  are not put into the queues

The first types of callbacks could be used in case various callable objects are put into the ProcessPool, 
so you probably want to handle them differently dependin on their results, while the second types are for 
executing same type of callables in subprocesses and  hence you are expecting the same type of results
everywhere. 
"""

__RCSID__ = "$Id$"

import multiprocessing
import sys
import time
import threading
import os
import signal
import Queue
from types import *

try:
  from DIRAC.FrameworkSystem.Client.Logger import gLogger
except ImportError:
  gLogger = False

try:
  from DIRAC.Core.Utilities.LockRing import LockRing
except ImportError:
  LockRing = False

try:
  from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
except ImportError:
  def S_OK( val = "" ):
    return { 'OK' : True, 'Value' : val }
  def S_ERROR( mess ):
    return { 'OK' : False, 'Message' : mess }

class WorkingProcess( multiprocessing.Process ):
  """
  .. class:: WorkingProcess

  WorkingProcess is a class that represents activity that is run in a separate process. 
  """

  def __init__( self, pendingQueue, resultsQueue ):
    """ c'tor

    :param self: self refernce
    :param multiprocess.Queue pendingQueue: queue storing ProcessTask before exection
    :param multiprocess.Queue resultsQueue: queue storing callbacks and exceptionCallbacks
    """
    multiprocessing.Process.__init__( self )
    self.daemon = True
    if LockRing:
      #Reset all locks
      lr = LockRing()
      lr._openAll()
      lr._setAllEvents()
    self.__working = multiprocessing.Value( 'i', 0 )
    self.__pendingQueue = pendingQueue
    self.__resultsQueue = resultsQueue
    self.start()

  def isWorking( self ):
    """ check if process is running

    :param self: self reference
    """
    return self.__working.value == 1

  def run( self ):
    """ task execution

    reads and executes ProcessTask :task: out of pending queue and then pushes it 
    to the results queue for callback execution
    
    :param self: self reference
    """
    while True:
      try:
        task = self.__pendingQueue.get( block = True, timeout = 10 )
      except Queue.Empty:
        continue
      if task.isBullet():
        break
      self.__working.value = 1
      try:
        task.process()
      finally:
        self.__working.value = 0
      if task.hasCallback() or task.usePoolCallbacks():
        self.__resultsQueue.put( task, block = True )

class BulletTask:
  def isBullet( self ):
    return True

class ProcessTask:
  """ .. class:: ProcessTask
  
  Defines task to be executed in WorkingProcess together with its callbacks.
  """

  def __init__( self,
                taskFunction,
                args = None,
                kwargs = None,
                taskID = None,
                callback = None,
                exceptionCallback = None,
                usePoolCallbacks = False ):
    """ c'tor

    :warning: taskFunction has to be callable: it could be a function, lambda OR a class with 
    __call__ operator defined. But be carefull with interpretation of args and kwargs, as they 
    are passed to different places in above cases:

    1. for functions or lambdas args and kwargs are just treated as function parameters
    
    2. for callable classess (say MyTask) args and kwargs are passed to class contructor 
       (MyTask.__init__) and MyTask.__call__ should be a method without parameters, i.e. 
       MyTask definition should be::

      class MyTask:
        def __init__( self, *args, **kwargs ):
          ...
        def __call__( self ):
          ...

    :param self: self reference
    :param mixed taskFunction: definition of callable object to be executed in this task
    :param tuple args: non-keyword arguments
    :param dict kwargs: keyword arguments
    :param int taskID: task id
    :param mixed callback: result callback function 
    :param mixed exceptionCallback: callback function to be fired upon exception in taskFunction
    """
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
    self.__usePoolCallbacks = usePoolCallbacks 

  def taskResults( self ):
    """ get task results

    :param self: self reference
    """
    return self.__taskResult

  def taskException( self ):
    """ get task exception

    :param self: self reference
    """
    return self.__taskException

  def enablePoolCallbacks( self ):
    self.__usePoolCallbacks = True

  def disablePoolCallbacks( self ):
    self.__usePoolCallbacks = False

  def usePoolCallbacks( self ):
    """ check if results should be processed by callbacks defind in the pool

    :param self: self reference
    """
    return self.__usePoolCallbacks
 
  def isBullet( self ):
    return False

  def getTaskID( self ):
    """ taskID getter

    :param self: self reference
    """
    return self.__taskID

  def hasCallback( self ):
    """ callback existence checking 

    :param self: self reference
    :return: True if callbak or exceptionCallback has been defined, False otherwise 
    """
    return self.__resultCallback or self.__exceptionCallback or self.__usePoolCallbacks
 
  def exceptionRaised( self ):
    """ flag to determine exception in process

    :param self: self reference
    """
    return self.__exceptionRaised

  def doExceptionCallback( self ):
    """ execute exceptionCallback 
    
    :param self: self reference
    """
    if self.__done and self.__exceptionRaised and self.__exceptionCallback:
      self.__exceptionCallback( self, self.__taskException )

  def doCallback( self ):
    """ execute result callback function

    :param self: self reference
    """
    if self.__done and not self.__exceptionRaised and self.__resultCallback:
      self.__resultCallback( self, self.__taskResult )

  def process( self ):
    """ execute task
    
    :param self: self reference
    """
    self.__done = True
    try:
      ## it's a function?
      if type( self.__taskFunction ) is FunctionType:
        self.__taskResult = self.__taskFunction( *self.__taskArgs, **self.__taskKwArgs )
      ## or a class? 
      elif type( self.__taskFunction ) in ( TypeType, ClassType ):
        ## create new instance
        taskObj = self.__taskFunction( *self.__taskArgs, **self.__taskKwArgs )
        ### check if it is callable, raise TypeError if not
        if not callable( taskObj ):
          raise TypeError( "__call__ operator not defined not in %s class" % taskObj.__class__.__name__ )
        ### call it at least
        self.__taskResult = taskObj()
    except Exception, x:
      self.__exceptionRaised = True

      if gLogger:
        gLogger.exception( "Exception in process of pool" )
      if self.__exceptionCallback:
        retDict = S_ERROR( 'Exception' )
        retDict['Value'] = str( x )
        retDict['Exc_info'] = sys.exc_info()[1]
        self.__taskException = retDict

class ProcessPool:
  """
  .. class:: ProcessPool
  
  """
  
  def __init__( self, minSize = 2, maxSize = 0, maxQueuedRequests = 10, 
                strictLimits = True, poolCallback=None, poolExceptionCallback=None ):
    """ c'tor

    :param self: self reference
    :param int minSize: minimal number of simultaniously executed tasks
    :param int maxSize: maximal number of simultaniously executed tasks 
    :param int maxQueueRequests: size of pending tasks queue
    :param bool strictLimits: flag to kill/terminate idle workers above the limits 
    :param callable poolCallbak: results callback
    :param callable poolExceptionCallback: exception callback
    """
    self.__minSize = max( 1, minSize )
    self.__maxSize = max( self.__minSize, maxSize )
    self.__maxQueuedRequests = maxQueuedRequests
    self.__strictLimits = strictLimits
    self.__poolCallback = poolCallback
    self.__poolExceptionCallback = poolExceptionCallback
    self.__pendingQueue = multiprocessing.Queue( self.__maxQueuedRequests )
    self.__resultsQueue = multiprocessing.Queue( 0 )
    self.__prListLock = threading.Lock()
    self.__workingProcessList = []
    self.__draining = False
    self.__bullet = BulletTask()
    self.__bulletCounter = 0
    self.__daemonProcess = False
    self.__spawnNeededWorkingProcesses()
    
  def setPoolCallback( self, callback ):
    """ set ProcessPool callback function

    :param self: self reference
    :param callable callback: callback function
    """
    self.__poolCallback = callback

  def setPoolExceptionCallback( self, exceptionCallback ):
    """ set ProcessPool exception callback function 

    :param self: self refernce
    :param callable exceptionCallback: exsception callback function
    """
    self.__poolExceptionCallback = exceptionCallback

  def getMaxSize( self ):
    """ maxSize getter

    :param self: self reference
    """
    return self.__maxSize

  def getMinSize( self ):
    """ minSize getter
    
    :param self: self reference
    """
    return self.__minSize

  def getNumWorkingProcesses( self ):
    """ count processes currently being executed  
    
    :param self: self reference
    """
    counter = 0
    self.__prListLock.acquire()
    try:
      for proc in self.__workingProcessList:
        if proc.isWorking():
          counter += 1
    finally:
      self.__prListLock.release()
    return counter

  def getNumIdleProcesses( self ):
    """ count processes being idle
    
    :param self: self reference
    """
    counter = 0
    self.__prListLock.acquire()
    try:
      for proc in self.__workingProcessList:
        if not proc.isWorking():
          counter += 1
    finally:
      self.__prListLock.release()
    return counter

  def getFreeSlots( self ):
    """ get number of free slots availablr for workers

    :param self: self reference
    """
    return max( 0, self.__maxSize - self.getNumWorkingProcesses() )

  def __spawnWorkingProcess( self ):
    """ create new process 

    :param self: self reference
    """
    self.__workingProcessList.append( WorkingProcess( self.__pendingQueue, self.__resultsQueue ) )

  def __killWorkingProcess( self ):
    """ suspend execution of WorkingProcesses exceeding queue limits
    :param self: self reference
    """
    try:
      self.__pendingQueue.put( self.__bullet, block = True )
    except Queue.Full:
      return S_ERROR( "Queue is full" )
    self.__prListLock.acquire()
    try:
      self.__bulletCounter += 1
    finally:
      self.__prListLock.release()
    self.__cleanDeadProcesses()

  def __cleanDeadProcesses( self ):
    ## check wounded processes
    self.__prListLock.acquire()
    try:
      stillAlive = []
      for wP in self.__workingProcessList:
        if wP.is_alive():
          stillAlive.append( wP )
        else:
          self.__bulletCounter -= 1
      self.__workingProcessList = stillAlive
    finally:
      self.__prListLock.release()

  def __spawnNeededWorkingProcesses( self ):
    """ create N working process (at least self.__minSize, but no more than self.__maxSize)

    :param self: self reference
    """
    self.__cleanDeadProcesses()
    #If we are draining do not spawn processes
    if self.__draining:
      return
    while len( self.__workingProcessList ) < self.__minSize:
      self.__spawnWorkingProcess()

    while self.hasPendingTasks() and \
          self.getNumIdleProcesses() == 0 and \
          len( self.__workingProcessList ) < self.__maxSize:
      self.__spawnWorkingProcess()
      time.sleep( 0.1 )

  def __killExceedingWorkingProcesses( self ):
    """ suspend executuion of working processes exceeding the limits

    :param self: self reference
    """
    self.__cleanDeadProcesses()
    toKill = len( self.__workingProcessList ) - self.__maxSize
    for i in range ( max( toKill, 0 ) ):
      self.__killWorkingProcess()
    toKill = self.getNumIdleProcesses() - self.__minSize
    for i in range ( max( toKill, 0 ) ):
      self.__killWorkingProcess()

  def queueTask( self, task, blocking = True ):
    """ enqueue new task into pending queue

    :param self: self reference
    :param ProcessTask task: new task to execute
    :param bool blocking: flag to block if necessary and new empty slot is available (default = block)
    """
    if not isinstance( task, ProcessTask ):
      raise TypeError( "Tasks added to the process pool must be ProcessTask instances" )
    try:
      self.__pendingQueue.put( task, block = blocking )
    except Queue.Full:
      return S_ERROR( "Queue is full" )
    self.__spawnNeededWorkingProcesses()
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
                          blocking = True,
                          usePoolCallbacks = False ):
    """ create new processTask and enqueue it in pending task queue

    :param self: self reference
    :param mixed taskFunction: callable object definition (FunctionType, LambdaType, callable class) 
    :param tuple args: non-keyword arguments passed to taskFunction c'tor 
    :param dict kwargs: keyword arguments passed to taskFunction c'tor
    :param int taskID: task Id
    :param mixed callback: callback handler, callable object executed after task's execution
    :param mixed exceptionCallback: callback handler executed if testFunction had raised an exception
    :param bool blocking: flag to block queue if necessary until free slot is available
    :param bool usePoolCallbacks: fire execution of pool defined callbacks after task callbacks
    """
    task = ProcessTask( taskFunction, args, kwargs, taskID, callback, exceptionCallback, usePoolCallbacks )
    return self.queueTask( task, blocking )

  def hasPendingTasks( self ):
    """ check if taks are present in pending queue 
    
    :param self: self reference

    :warning: results may be misleading if elements put into the queue are big
    """
    return not self.__pendingQueue.empty()

  def isFull( self ):
    """ check in peding queue is full

    :param self: self reference

    :warning: results may be misleading if elements put into the queue are big
    """
    return self.__pendingQueue.full()

  def isWorking( self ):
    """ check existence of working subprocesses

    :param self: self reference
    """
    return not self.__pendingQueue.empty() or self.getNumWorkingProcesses()

  def processResults( self ):
    """ execute tasks' callbacks removing them from results queue

    :param self: self reference
    """
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
      ## execute pool callbacks
      if task.usePoolCallbacks():
        ## exception first
        if self.__poolExceptionCallback and task.taskException():
          self.__poolExceptionCallback( task ) 
        elif self.__poolCallback and task.taskResults():
          self.__poolCallback( task )

      self.__killExceedingWorkingProcesses()
      processed += 1
    return processed

  def processAllResults( self ):
    """ process all enqueued tasks at once
    
    :param self: self reference
    """
    while not self.__pendingQueue.empty() or self.getNumWorkingProcesses():
      self.processResults()
      time.sleep( 1 )
    self.processResults()

  def finalize( self, timeout = 10 ):
    #Process all tasks
    self.processAllResults()
    #Drain via bullets processes
    self.__draining = True
    try:
      bullets = len( self.__workingProcessList ) - self.__bulletCounter
      for i in range( bullets ):
        self.__killWorkingProcess()
      start = time.time()
      self.__cleanDeadProcesses()
      while len( self.__workingProcessList ) > 0:
        if timeout <= 0 or time.time() - start >= timeout:
          break
        time.sleep( 0.1 )
        self.__cleanDeadProcesses()
    finally:
      self.__draining = False
    #Terminate them (just in case)
    for wp in self.__workingProcessList:
      if wp.is_alive():
        wp.terminate()
    self.__cleanDeadProcesses()
    #Kill 'em all!!
    self.__filicide()

  def __filicide( self ):
    """ Kill all children (processes :P) Kill 'em all!
    """
    wpL = [ ( wp, 0 ) for wp in self.__workingProcessList ]
    self.__workingProcessList = []
    while wpL:
      wp, count = wpL.pop( 0 )
      if wp.pid == None:
        if count > 5:
          wp.terminate()
        else:
          wpL.append( ( wp, count + 1 ) )
        continue
      os.kill( wp.pid, signal.SIGKILL )

  def daemonize( self ):
    """ Make ProcessPool a finite being for opening and closing doors between chambers.
        Also just run it in a separate background thread to the death of PID 0.
        
    :param self: self reference
    """
    if self.__daemonProcess:
      return
    self.__daemonProcess = threading.Thread( target = self.__backgroundProcess )
    self.__daemonProcess.setDaemon( 1 )
    self.__daemonProcess.start()

  def __backgroundProcess( self ):
    """ daemon thread target
    
    :param self: self reference
    """
    while True:
      self.processResults()
      time.sleep( 1 )

class doSomething( object ):

  def __init__( self, number, r ):
    self.number = number
    self.r = r
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()
    from DIRAC.FrameworkSystem.Client.Logger import gLogger
    gLogger.showHeaders( True )
    self.log = gLogger.getSubLogger( "doSomething%s" % self.number )

  def __call__( self ):
    self.log.error( "in call" )
    rnd = random.randint( 1, 5 )
    print "sleeping %s secs for task number %s" % ( rnd, self.number )
    time.sleep( rnd )

    rnd = random.random() * self.number
    if rnd < 3:
      print "raising exception for task %s" % self.number
      raise Exception( "TEST EXCEPTION" )
    print "task number %s done" % self.number
    return { "OK" : True, "Value" : [1, 2, 3] }


class PoolOwner( object ):

  def __init__( self ):
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()
    from DIRAC.FrameworkSystem.Client.Logger import gLogger
    gLogger.showHeaders( True )
    self.log = gLogger.getSubLogger( "PoolOwner" )
    self.processPool = ProcessPool( poolCallback = self.callback, poolExceptionCallback = self.exceptionCallback )
    self.processPool.daemonize()
    

  def callback( self, task ):
    self.log.always( "PoolOwner callback, task is %s, result is %s" % ( task.getTaskID(), task.taskResults() ) )
    
  def exceptionCallback( self, task ):
    self.log.always( "PoolOwner exceptionCallback, task is %s, exception is %s" % ( task.getTaskID(), task.taskException() ) )
 
  def execute( self ):
    i = 0
    while True: 
      if self.processPool.getFreeSlots() > 0:
        r = random.randint(1, 5)
        result = self.processPool.createAndQueueTask( doSomething,
                                                      taskID = i,
                                                      args = ( i, r, ),
                                                      usePoolCallbacks = True )    
        i += 1
        self.log.always("doSomething enqueued to task %s" % i )
      if i == 20:
        break

    self.processPool.processAllResults() 
    self.processPool.finalize()


## test execution
if __name__ == "__main__":

  import random

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
      result = pPool.createAndQueueTask( doSomething,
                                         args = ( count, r, ),
                                         callback = showResult,
                                         exceptionCallback = showException )
      count += 1
    else:
      print "no free slots"
      time.sleep( 1 )

  pPool.processAllResults()

  print "Max sleep", rmax


  poolOwner = PoolOwner()
  
  poolOwner.execute()
  
