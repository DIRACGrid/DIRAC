
import threading, time
from DIRAC import S_OK, S_ERROR, gLogger

class ExecutorQueues:

  def __init__( self, log = False ):
    if log:
      self.__log = log
    else:
      self.__log = gLogger
    self.__lock = threading.Lock()
    self.__queues = {}
    self.__lastUse = {}
    self.__taskInQueue = {}

  def getExecutorList( self ):
    return [ exName for exName in self.__queues ]

  def pushTask( self, exName, taskId, ahead = False ):
    self.__log.info( "Pushing task %d into waiting queue for executor %s" % ( jid, exName ) )
    self.__lock.acquire()
    try:
      if taskId in self.__taskInQueue:
        if self.__taskInQueue[ taskId ] != exName:
          raise Exception( "Task %s cannot be queued because it's already queued" % taskId )
        else:
          return
      if exName not in self.__queues:
        self.__queues[ exName ] = []
      self.__lastUse[ exName ] = time.time()
      if ahead:
        self.__queues[ exName ].insert( 0, taskId )
      else:
        self.__queues[ exName ].append( taskId )
      self.__taskInQueue[ taskId ] = exName
    finally:
      self.__lock.release()

  def popTask( self, exName ):
    self.__lock.acquire()
    try:
      try:
        taskId = self.__queues[ exName ].pop( 0 )
        del( self.__taskInQueue[ taskId ] )
      except IndexError:
        return None
      except KeyError:
        return None
    finally:
      self.__lock.release()
    self.__log.info( "Popped task %s from executor %s waiting queue" % ( taskId, exName ) )
    return taskId

  def getState( self ):
    self.__lock.acquire()
    try:
      qInfo = {}
      for qName in self.__queues:
        qInfo[ qName ] = list( self.__queues[ qName ] )
    finally:
      self.__lock.release()
    return qInfo

  def deleteTask( self, taskId ):
    self.__log.verbose( "Deleting task %d from waiting queues" % taskId )
    self.__lock.acquire()
    try:
      try:
        exName = self.__taskInQueue[ taskId ]
        del( self.__taskInQueue[ taskId ] )
      except KeyError:
        return
      try:
        iPos = self.__queues[ exName ].index( taskId )
      except ValueError:
        return
      del( self.__queues[ exName ][ iPos ] )
    finally:
      self.__lock.release()

  def waitingTasks( self, exName ):
    self.__lock.acquire()
    try:
      try:
        return len( self.__queues[ exName ] )
      except KeyError:
        return 0
    finally:
      self.__lock.release()

class ExecutorMind:

  def __init__( self ):
    self.__executorsLock = threading.Lock()
    self.__executors = {}
    self.__tasksLock = threading.Lock()
    self.__tasks = {}
    self.__log = gLogger.getSubLogger( "ExecMind" )
    self.__dispatchCallback = self.__defaultCallback
    self.__taskFreezer = []
    self.__queues = ExecutorQueues( self.__log )

  def __defaultCallback( self, *args ):
    return S_ERROR( "No callback defined" )

  def addExecutor( self, trid, identity, executorName ):
    self.__log.info( "Adding new %s executor to the pool [%s][%s]" % ( executorName, identity, trid ) )
    self.__executorsLock.acquire()
    try:
      if executorName not in self.__executors:
        self.__executors[ executorName ] = []
      self.__executors.append( ( trid, identity ) )
    finally:
      self.__executorsLock.release()

  def __addTaskIfNew( self, taskId, taskObj ):
    self.__tasksLock.acquire()
    try:
      if taskId in self.__tasks:
        self.__log.verbose( "Task %s was already known" % taskId )
        return False
      self.__tasks[ taskId ] = taskObj
      self.__log.verbose( "Added task %s" % taskId )
      return True
    finally:
      self.__tasksLock.release()

  def __getNextExecutor( self, taskId ):
    try:
      taskObj = self.__tasks[ taskId ]
    except IndexError:
      msg = "Task %s was deleted prematurely while being dispatched" % taskId
      self.__log.error( msg )
      return S_ERROR( msg )
    try:
      return self.__dispatchCallback( taskId, taskObj )
    except:
      self.__log.exception( "Exception while calling dispatch callback" )
      return S_ERROR( "Exception while calling dispatch callback" )

  def __feezeTask( self, taskId ):
    self.__tasksLock.acquire()
    try:
      if taskId not in self.__tasks:
        return False
      self.__log.info( "Adding task %s to freezer" % taskId )
      self.__taskFreezer.append( ( time.time(), taskId ) )
      return True
    finally:
      self.__tasksLock.release()

  def __removeTask( self, taskId ):
    #TODO:
    return S_OK()


  def addTask( self, taskId, taskObj ):
    if not self.__addTaskIfNew( taskId, taskObj ):
      return S_OK()


  def __dispatchTask( self, taskId ):
    result = self.__getNextExecutor( taskId )

    if not result[ 'OK' ]:
      self.__log.warn( "Error while calling dispatch callback: %s" % result[ 'Message' ] )
      if self.__addTaskToFreezer( taskId ):
        return S_OK()
      return S_ERROR( "Could not add task. Dispatching task failed" )

    execName = result[ 'Value' ]
    if not execName:
      self.__log.info( "No more executors for task %s" % taskId )
      return self.__removeTask( taskId )

    self.__queues.pushTask( execName, taskId )
    return self.__fillExecutors( execName )

  def __fillExecutors( self, execName ):


