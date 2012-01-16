#################################################################
# $HeadURL$
#################################################################

"""
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

"""

__RCSID__ = "$Id$"

import multiprocessing
import Queue
import sys
import time
import threading
import os
import string 
from types import *

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
    self.__working = multiprocessing.Value( 'i', 0 )
    self.__alive = multiprocessing.Value( 'i', 1 )
    self.__pendingQueue = pendingQueue
    self.__resultsQueue = resultsQueue
    self.start()

  def isWorking( self ):
    """ check if process is running

    :param self: self reference
    """
    return self.__working.value == 1

  def kill( self ):
    """ suspend subprocess exection 

    :param self: self reference
    """
    self.__alive.value = 0

  def run( self ):
    """ task execution

    reads and executes ProcessTask :task: out of pending queue and then pushes it 
    to the results queue for callback execution
    
    :param self: self reference
    """
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
  """ .. class:: ProcessTask
  
  Defines task to be executed in WorkingProcess together with its callbacks.
  """

  def __init__( self,
                taskFunction,
                args = None,
                kwargs = None,
                taskID = None,
                callback = None,
                exceptionCallback = None ):
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
    return self.__resultCallback or self.__exceptionCallback

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
      if type(self.__taskFunction) is FunctionType:
        self.__taskResult = self.__taskFunction( *self.__taskArgs, **self.__taskKwArgs )
      ## or a class? 
      elif type(self.__taskFunction) in ( TypeType, ClassType ):
        ## create new instance
        taskObj = self.__taskFunction( *self.__taskArgs, **self.__taskKwArgs )
        ### check if it is callable, raise TypeError if not
        if not callable(taskObj):
          raise TypeError("__call__ operator not defined not in %s class" % taskObj.__class__.__name__ )
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

  def __init__( self, minSize = 2, maxSize = 0, maxQueuedRequests = 10, strictLimits = True ):
    """ c'tor

    :param self: self reference
    :param int minSize: minimal number of simultaniously executed tasks
    :param int maxSize: maximal number of simultaniously executed tasks 
    :param int maxQueueRequests: size of pending tasks queue
    :param bool strictLimits: flag to 
    """
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
    for proc in self.__workingProcessList:
      if proc.isWorking():
        counter += 1
    return counter

  def getNumIdleProcesses( self ):
    """ count processes being idle
    
    :param self: self reference
    """
    counter = 0
    for proc in self.__workingProcessList:
      if not proc.isWorking():
        counter += 1
    return counter

  def getFreeSlots( self ):
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
    """ create N working process (at least self.__minSize, but no more than self.__maxSize)
   
    :param self: self reference
    """
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
    """ create new processTask and enqueue it in pending task queue

    :param self: self reference
    :param mixed taskFunction: callable object definition (FunctionType, LambdaType, callable class) 
    :param tuple args: non-keyword arguments passed to taskFunction c'tor 
    :param dict kwargs: keyword arguments passed to taskFunction c'tor
    :param int taskID: task Id
    :param mixed callback: callback handler, callable object executed after task's execution
    :param mixed exceptionCallback: callback handler executed if testFunction had raised an exception
    :param bool blocking: flag to block queue if necessary until free slot is available
    """
    task = ProcessTask( taskFunction, args, kwargs, taskID, callback, exceptionCallback )
    return self.queueTask( task, blocking )

  def hasPendingTasks( self ):
    """ check if taks are present in pending queue 
    
    :param self: self reference
    """
    return not self.__pendingQueue.empty()

  def isFull( self ):
    """ check in peding queue is full

    :param self: self reference
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
      self.__killExceedingWorkingProcesses()
      processed += 1
    return processed

  def processAllResults( self ):
    """ process all enqueued tasks at once
    
    :param self: self reference
    """
    while not self.__pendingQueue.empty() or self.getNumWorkingProcesses():
      self.processResults()
      time.sleep( 0.1 )
    self.processResults()

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
    return { "OK" : True, "Value" : [1,2,3] }



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
