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
from types import FunctionType, TypeType, ClassType

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
    """ dummy S_OK """
    return { 'OK' : True, 'Value' : val }
  def S_ERROR( mess ):
    """ dummy S_ERROR """
    return { 'OK' : False, 'Message' : mess }


class ProcessTaskTimeOutError( Exception ):
  """ dummy exception rainsed in SIGALRM handler """
  pass

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
    ## daemonize
    self.daemon = True
    ## flag to see if task is being treated
    self.__working = multiprocessing.Value( 'i', 0 )
    ## task counter
    self.__taskCounter = multiprocessing.Value( 'i', 0 )
    ## task queue
    self.__pendingQueue = pendingQueue
    ## results queue
    self.__resultsQueue = resultsQueue
    ## placeholder for watchdog thread
    self.__watchdogThread = None
    ## placeholder for timer thread
    self.__timerThread = None
    ## read write lock
    self.rwLock = threading.Lock()
    ## put event
    self.putEvent = threading.Event()
    ## start yourself at least    
    self.start()

  def __watchdog( self ):
    """ watchdog thread target

    terminating/killing WorkingProcess when parent process is dead

    :param self: self reference
    """
    while True:
      ## parent is dead,  commit suicide
      if os.getppid() == 1:
        os.kill( self.pid, signal.SIGTERM )
        ## wait for a while and if still alive use REAL silencer
        time.sleep(30)
        self.rwLock.acquire()
        ## now you're dead
        os.kill( self.pid, signal.SIGKILL )
        self.rwLock.release()
      time.sleep(5)

  def isWorking( self ):
    """ check if process is running

    :param self: self reference
    """
    return self.__working.value == 1

  def taskProcessed( self ):
    """ for better monitoring tell how many tasks have been processed so far

    :param self: self reference
    """
    return self.__taskCounter

  def __timer( self ):
    if self.task.hasCallback() or self.task.hasPoolCallback():
      if self.task.taskResults():
        self.__resultsQueue.put( self.task )
        self.putEvent.set()
        return
      self.task.setResult( S_ERROR( "Timed out" ) )
      self.__resultsQueue.put( self.task )
      self.putEvent.set()
    os.kill( self.pid, signal.SIGKILL )
    
  def run( self ):
    """ task execution

    reads and executes ProcessTask :task: out of pending queue and then pushes it
    to the results queue for callback execution

    :param self: self reference
    """
    ## start watchdog thread
    self.__watchdogThread = threading.Thread( target = self.__watchdog )
    self.__watchdogThread.daemon = True
    self.__watchdogThread.start()

    ## http://cdn.memegenerator.net/instances/400x/19450565.jpg
    if LockRing:
      # Reset all locks
      lr = LockRing()
      lr._openAll()
      lr._setAllEvents()

    taskCounter = 0
    ## main loop
    while True:

      self.task = None

      if self.__timerThread:
        self.__timerThread.cancel()

      ## read from queue
      try:
        task = self.__pendingQueue.get( block = True, timeout = 10 )   
      except Queue.Empty:
        continue

      ## conventional murder
      if task.isBullet():
        break

      ## toggle __working flag
      self.__working.value = 1

      ## save task
      self.task = task

      ## start timer thread
      if self.task.getTimeOut():
        self.__timerThread = threading.Timer( task.getTimeOut()+5, self.__timer )
        self.__timerThread.start()

      ## process task
      try:
        task.process()
        ## cancel timer, results are ready
        if self.__timerThread:
          self.__timerThread.cancel()  
        ## put back results
        if not self.putEvent.is_set() and self.task.hasCallback() or self.task.usePoolCallbacks():
          self.__resultsQueue.put( task )
          self.putEvent.set()
      finally:
        ## increase task counter
        taskCounter += 1
        self.__taskCounter = taskCounter 
        ## toggle __working flag
        self.__working.value = 0
        self.putEvent.clear()
       
class BulletTask:
  """ dum-dum bullet """
  def isBullet( self ):
    """ Fire in the hole! Take coover! """
    return True

class ProcessTask:
  """ .. class:: ProcessTask

  Defines task to be executed in WorkingProcess together with its callbacks.
  """
  ## taskID
  taskID = 0

  def __init__( self,
                taskFunction,
                args = None,
                kwargs = None,
                taskID = None,
                callback = None,
                exceptionCallback = None,
                usePoolCallbacks = False,
                timeOut = 0 ):
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

    :warning: depending on :timeOut: value, taskFunction execution can be forcefully terminated
    using SIGALRM after :timeOut: seconds spent, :timeOut: equal to zero means there is no any
    time out at all, except those during :ProcessPool: finalization

    :param self: self reference
    :param mixed taskFunction: definition of callable object to be executed in this task
    :param tuple args: non-keyword arguments
    :param dict kwargs: keyword arguments
    :param int taskID: task id, if not set,
    :param int timeOut: estimated time to execute taskFunction in seconds (default = 0, no timeOut at all)
    :param mixed callback: result callback function
    :param mixed exceptionCallback: callback function to be fired upon exception in taskFunction
    """
    self.__taskFunction = taskFunction
    self.__taskArgs = args or []
    self.__taskKwArgs = kwargs or {}
    self.__taskID = taskID
    self.__resultCallback = callback
    self.__exceptionCallback = exceptionCallback
    self.__timeOut = 0
    ## set time out
    self.setTimeOut( timeOut )
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
    """ (re)enable use of ProcessPool callbacks """
    self.__usePoolCallbacks = True

  def disablePoolCallbacks( self ):
    """ disable execution of ProcessPool callbacks """
    self.__usePoolCallbacks = False

  def usePoolCallbacks( self ):
    """ check if results should be processed by callbacks defined in the :ProcessPool:

    :param self: self reference
    """
    return self.__usePoolCallbacks

  def hasPoolCallback( self ):
    """ check if asked to execute :ProcessPool: callbacks

    :param self: self reference
    """
    return self.__usePoolCallbacks

  def setTimeOut( self, timeOut ):
    """ set time out (in seconds)

    :param self: selt reference
    :param int timeOut: new time out value
    """
    try:
      self.__timeOut = int( timeOut )
      return { "OK" : True, "Value" : self.__timeOut }
    except (TypeError, ValueError), error:
      return { "OK" : False, "Message" : str(error) }

  def getTimeOut( self ):
    """ get timeOut value

    :param self: self reference
    """
    return self.__timeOut

  def hasTimeOutSet( self ):
    """ check if timeout is set

    :param self: self reference
    """
    return bool( self.__timeOut != 0 )

  def isBullet( self ):
    """ No, I'm not. """
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

  def setResult( self, result ):
    self.__taskResult = result

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
      if self.__exceptionCallback or self.usePoolCallbacks():
        retDict = S_ERROR( 'Exception' )
        retDict['Value'] = str( x )
        retDict['Exc_info'] = sys.exc_info()[1]
        self.__taskException = retDict
        
class ProcessPool:
  """
  .. class:: ProcessPool

  """

  def __init__( self, minSize = 2, maxSize = 0, maxQueuedRequests = 10,
                strictLimits = True, poolCallback=None, poolExceptionCallback=None,
                killPeriod = 60 ):
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

    self.__workersDict = {}

    self.__draining = False
    self.__bullet = BulletTask()
    self.__daemonProcess = False
    self.__spawnNeededWorkingProcesses()
    self.__killPeriod = killPeriod
    self.__killIdle = []
    self.__killPeriodStart = time.time()

  def stopProcessing( self ):
    """ case fire

    :param self: self reference
    """
    self.finalize()

  def startProcessing( self ):
    """ restrat processing again 

    :param self: self reference 
    """
    self.__draining = False 
    self.daemonize()
    
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
      counter = len( [ pid for pid, worker in self.__workersDict.items() if worker.isWorking() ] )
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
      counter = len( [ pid for pid, worker in self.__workersDict.items() if not worker.isWorking() ] )
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
    self.__prListLock.acquire()
    try:
      worker = WorkingProcess( self.__pendingQueue, self.__resultsQueue )
      while worker.pid == None:
        time.sleep(0.1)
      self.__workersDict[ worker.pid ] = worker
    finally:
      self.__prListLock.release()

  def __killWorkingProcess( self ):
    """ suspend execution of WorkingProcesses exceeding queue limits
    :param self: self reference
    """
    self.__prListLock.acquire()
    try:
      self.__pendingQueue.put( self.__bullet, block = True )
    except Queue.Full:
      pass
    finally:
      self.__prListLock.release()

    self.__cleanDeadProcesses()

  def __cleanDeadProcesses( self ):
    """ delete references of dead workingProcesses from ProcessPool.__workingProcessList """
    ## check wounded processes
    self.__prListLock.acquire()
    try:
      for pid, worker in self.__workersDict.items():
        if not worker.is_alive():
          del self.__workersDict[pid]
    finally:
      self.__prListLock.release()

  def __spawnNeededWorkingProcesses( self ):
    """ create N working process (at least self.__minSize, but no more than self.__maxSize)

    :param self: self reference
    """
    self.__cleanDeadProcesses()
    # If we are draining do not spawn processes
    if self.__draining:
      return

    while len( self.__workersDict ) < self.__minSize:
      if self.__draining: 
        return
      self.__spawnWorkingProcess()

    while self.hasPendingTasks() and \
          self.getNumIdleProcesses() == 0 and \
          len( self.__workersDict ) < self.__maxSize:
      if self.__draining:
        return
      self.__spawnWorkingProcess()
      time.sleep( 0.1 )

  def __killExceedingWorkingProcesses( self ):
    """ suspend executuion of working processes exceeding the limits

    :param self: self reference
    """
    self.__cleanDeadProcesses()
    now = time.time()
    if now - self.__killPeriodStart < self.__killPeriod:
      self.__killIdle.append( self.getNumIdleProcesses() )
      return
    self.__killPeriodStart = now
    # Kill exceeding processes over the max + average idle processes
    toKill = max( len( self.__workersDict ) - self.__maxSize, 0 )
    for iP in self.__killIdle:
      toKill += iP
    if not self.__killIdle: 
      return
    toKill = toKill / len( self.__killIdle )
    self.__killIdle = []
    while toKill:
      self.__killWorkingProcess()
      toKill = toKill - 1

  def queueTask( self, task, blocking = True, usePoolCallbacks= False ):
    """ enqueue new task into pending queue

    :param self: self reference
    :param ProcessTask task: new task to execute
    :param bool blocking: flag to block if necessary and new empty slot is available (default = block)
    :param bool usePoolCallbacks: flag to trigger execution of pool callbacks (default = don't execute)
    """
    if not isinstance( task, ProcessTask ):
      raise TypeError( "Tasks added to the process pool must be ProcessTask instances" )
    if usePoolCallbacks and ( self.__poolCallback or self.__poolExceptionCallback ):
      task.enablePoolCallbacks()

    self.__prListLock.acquire()
    try:
      self.__pendingQueue.put( task, block = blocking )
    except Queue.Full:
      self.__prListLock.release()
      return S_ERROR( "Queue is full" )
    finally:
      self.__prListLock.release()

    self.__spawnNeededWorkingProcesses()
    # Throttle a bit to allow task state propagation
    time.sleep( 0.1 )
    return S_OK()

  def createAndQueueTask( self,
                          taskFunction,
                          args = None,
                          kwargs = None,
                          taskID = None,
                          callback = None,
                          exceptionCallback = None,
                          blocking = True,
                          usePoolCallbacks = False,
                          timeOut = 0):
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
    :param int timeOut: time you want to spend executing :taskFunction:
    """
    task = ProcessTask( taskFunction, args, kwargs, taskID, callback, exceptionCallback, usePoolCallbacks, timeOut )
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
      ## get task
      task = self.__resultsQueue.get()
      ## execute task callbacks
      task.doExceptionCallback()
      task.doCallback()
      ## execute pool callbacks
      if task.usePoolCallbacks():
        if self.__poolExceptionCallback and task.exceptionRaised():
          self.__poolExceptionCallback( task.getTaskID(), task.taskException() )
        if self.__poolCallback and task.taskResults():
          self.__poolCallback( task.getTaskID(), task.taskResults() )
      self.__killExceedingWorkingProcesses()
      processed += 1
    return processed

  def processAllResults( self, timeout=10 ):
    """ process all enqueued tasks at once

    :param self: self reference
    """
    start = time.time()
    while self.getNumWorkingProcesses() or not self.__pendingQueue.empty():
      self.processResults()      
      time.sleep( 1 )
      if time.time() - start > timeout:
        break
    self.processResults()

  def finalize( self, timeout = 10 ):
    """ drain pool, shutdown processing in more or less clean way

    :param self: self reference
    :param timeout: seconds to wait before killing
    """
    # Drain via bullets processes
    self.__draining = True
    ## join deamon process
    self.__daemonProcess.join(timeout)
    ## process all tasks
    self.processAllResults( timeout )
    ## first clean up - join idle workers
    self.__cleanDeadProcesses()
    ## second clean up - send bullets, clean up wounded workers
    bullets = len([ worker for worker in self.__workersDict.values() 
                    if worker.is_alive() and not worker.isWorking() ] ) 
    while bullets > 0:
      self.__killWorkingProcess()
      bullets = bullets - 1
    self.__cleanDeadProcesses()
    start = time.time()
    while self.__workersDict:
      if timeout <= 0 or time.time() - start >= timeout:
        break
      time.sleep( 0.1 )
    self.__cleanDeadProcesses()
        
    ## third clean up - join and terminate workers
    for worker in self.__workersDict.values():
      if worker.is_alive():
        worker.terminate()
        worker.join(5)
    self.__cleanDeadProcesses()
    ## fourth clean up - kill'em all!!!
    self.__filicide()
    ## rest counter
    self.__bulletCounter = 0

  def __filicide( self ):
    """ Kill all children (processes :P) 
    Kill'em all! ...and justice for all!
    
    :param self: self reference
    """
    while self.__workersDict:
      pid = self.__workersDict.keys().pop(0)
      worker = self.__workersDict[pid]
      if worker.is_alive():
        os.kill( pid, signal.SIGKILL )
      del self.__workersDict[pid]
  
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
      if self.__draining:
        return
      self.processResults()
      time.sleep( 1 )

  def __del__( self ):
    """ del slot

    :param self: self reference
    """
    self.finalize()

