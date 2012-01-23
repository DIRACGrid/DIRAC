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
:param int maxQueuedRequests: size for task queue waiting to be executed  

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

##
# @file ProcessPool.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/01/19 09:32:30
# @brief Definition of ProcessPool and related classes.

## imports
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
    return { "OK" : True, "Value" : val }
  def S_ERROR( message ):
    return { "OK" : False, "Message" : message }


class Assassin( object ):
  """ 
  .. class:: Assassin
  a dummy class to terminate daemon WorkignProcesses
 
  """
  pass


class WorkingProcess( multiprocessing.Process ):
  """
  .. class:: WorkingProcess

  Task executor in a separate subprocess.

  """
  def __init__( self, inQueue, outQueue, daemon = True ):
    """ c'tor
    
    :param multiprocessing.Queue inQueue: input queue
    :param muliiprocessing.Queue iutQueue: output queue
    :param bool daemon: flag to create daemon or not-daemon process (deafult: daemon)
    """
    multiprocessing.Process.__init__( self )
    if daemon:
      self.daemon = True 
    self.__inQueue = inQueue
    self.__outQueue = outQueue
    self.start()

  def run( self ):
    """ run Forrest, run!
    
    :param self: self reference
    """
    while True:
      task = self.__inQueue.get( block = True  )
      if isinstance( task, Assassin ):
        return
      else:
        task.process()
        if task.hasCallback():
          self.__outQueue.put( task, block = True )

class ProcessTask( object ):
  """
  .. class:: ProcessTask

  Task to be executed in a subprocess. This is a bag object storing real code to be executed
  together with its callbacks, results and exceptions raised during run. 

  """
  __gTaskID = 0
  
  def __init__( self, 
                callMe, 
                args = None, 
                kwargs = None, 
                taskID = None, 
                callback = None, 
                exceptCallback = None ):
    """ c'tor
    
    :param self: self reference
    :param callable callMe: callable definition that has to be executed
    :param tuple args: unnamed args tuple
    :param dict kwargs: named args dict
    :param taskID: task ID
    :param funcdef callback: callback function definition
    :param funcdef exceptCallback: exception callback function definition 

    """
    self.__callMe = callMe
    self.__args = args or ()
    self.__kwargs = kwargs or {}
    self.__taskID = taskID
    if not self.__taskID:
      self.__taskID = str(self)
    self.__callback = callback
    self.__exceptCallback = exceptCallback
    self.__done = False
    self.__exceptionRaised = False
    self.__taskException = None
    self.__result = None

  def getTaskID( self ):
    """ taskID getter
    
    :param self: self reference
    """
    return self.__taskID

  def taskID( self ):
    """ task ID getter
    
    :param self: self reference
    """
    return self.__taskID

  def hasCallback( self ):
    """ checking presence of callback or exceptCallback
    
    :param self: self reference
    """
    return self.__callback or self.__exceptCallback 

  def callback( self ):
    """ callback execution

    :param self: self reference
    """
    if self.__done and not self.__exceptionRaised and self.__callback:
      self.__callback( self, self.__result )

  def exceptCallback( self ):
    """ exception callback execution

    :param self: self reference
    """
    if self.__done and self.__exceptionRaised and self.__exceptCallback:
      self.__exceptCallback( self, self.__taskException )

  def result( self ):
    """ results getter

    :param self: self reference
    """
    if self.__done and self.__result:
      return self.__result

  def process( self ):
    """ callMe execution

    :param self: self reference
    """
    try:
      if type(self.__callMe) is FunctionType:
        self.__result = self.__callMe( *self.__args, **self.__kwargs )
      elif type(self.__callMe) in ( TypeType, ClassType ):
        taskObj = self.__callMe( *self.__args, **self.__kwargs )
        if not callable(taskObj):
          raise TypeError("__call__ operator not defined not in %s class" % taskObj.__class__.__name__ )
        self.__result = taskObj()
    except Exception, error:
       self.__exceptionRaised = True
       retDict = { "OK" : False } 
       retDict["Message"] = "Exception"
       retDict['Value'] = str( error )
       retDict['Exc_info'] = sys.exc_info()[1]
       self.__taskException = retDict
    self.__done = True 

class ProcessPool( object ):
  """ 
  ... class:: ProcessPool

  Sub-processes manager.
  """

  def __init__( self, minSize=1, maxSize=4, queueSize=10 ):
    """ c'tor
    
    :param self: self reference
    :param int minSize: minimal number of workers in the pool
    :param int maxSize: maximal number of workers in the pool
    :param int queueSize: size for queue with waiting tasks 
    """
    self.__queueSize = queueSize
    self.__inQueue = multiprocessing.Queue( queueSize )
    self.__outQueue = multiprocessing.Queue( 0 )
    self.__minSize = max( 1, minSize )
    self.__maxSize = max( minSize, maxSize )
    self.__daemon = False     
    self.__workers = list()
    self.__destroy = False

  def minSize( self ):
    """ minSize getter

    :param self: self reference
    """
    return self.__minSize

  def maxSize( self ):
    """ maxSize getter

    :param self: self reference
    """
    return self.__maxSize

  def queueSize( self ):
    """ waiting tasks queue size getter

    :param self: self reference
    """
    return self.__queueSize

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

  def queueTask( self, task, block = True ):
    """ put task into waiting queue

    :param self: self reference
    :param ProcessTask task: a task to be executed
    :param bool block: flag for steering queue blocking
    """
    if isinstance( task, ProcessTask ) or isinstance( task, Assassin ):
      while True:
        try:
          self.__inQueue.put( task, block = block )
          self.spawn()
          break
        except Queue.Full:
          pass
    time.sleep( 1 )
    return { "OK" : True, "Value" : "" }

  def spawn( self ):
    """ spawn new worker

    :param self: self reference
    """
    if len( self.__workers ) < self.__maxSize:
      worker = WorkingProcess( self.__inQueue, self.__outQueue )
      self.__workers.append( worker )

  def processAllResults( self, closeQueues = True ):
    """ finalize tasks processing

    :param self: self reference
    :param bool closeQueues: flag to steer inQueue and outQueue closure
    """
    self.finalize( closeQueues )

  def finalize( self, closeQueues = True ):
    """ finalize processing

    :param self: self reference
    :param bool closeQueues: flag to steer inQueue and outQueue closure
    """
    self.__terminateWorkers()
    if closeQueues:
      while True:
        if self.__inQueue.empty():
          self.__inQueue.close()
          self.__inQueue = None
          break
      while True:
        if self.__outQueue.empty():
          self.__outQueue.close()
          self.__outQueue = None
          break
      
  def processResults( self ):
    """ callbak and exception callback execution for results read from outQueue

    :param self: self reference
    """
    processed = 0
    while True:
      if self.__outQueue:
        if self.__outQueue.empty():
          break
        task = self.__outQueue.get()
        task.exceptCallback()
        task.callback()
    return processed

  def __backgroundProcess( self ):
    """ daemon thread target

    :param self: self reference
    """
    while True:
      self.processResults()        
      time.sleep(1)

  def daemonize( self ):
    """ Make ProcessPool a finite being for opening and closing doors between chambers.
        Also just run it in a separate background thread to the death of PID 0 or parent process.

    :param self: self reference
    """
    if self.__daemon:
      return
    self.__daemon = threading.Thread( target = self.__backgroundProcess )
    self.__daemon.setDaemon( 1 )
    self.__daemon.start()

  def __terminateWorkers( self ):
    """ retire all workers in the pool 

    :param self: self reference
    """
    for i in range(len(self.__workers)):
      while True:
        try:
          assasin = Assassin()
          self.__inQueue.put( assasin  )
          time.sleep(1)
          break
        except Queue.Full:    
          pass
 
    while self.__workers:
      worker = self.__workers.pop()
      while True:
        if not worker.is_alive():
          worker.terminate()
          break     

class doSomething( object ):
  """
  .. class doSomething:: dummy test class

  """

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

  pPool = ProcessPool( 1, 4, 10 )
  pPool.daemonize()

  count = 0
  rmax = 0
  while count < 10:
    r = random.randint( 1, 5 )
    if r > rmax:
      rmax = r
    result = pPool.createAndQueueTask( doSomething, 
                                       args = ( count, r, ),
                                       callback = showResult,
                                       exceptionCallback = showException )
    count += 1

  pPool.processAllResults( closeQueues = True )
