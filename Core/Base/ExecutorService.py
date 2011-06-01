
import threading, time
from DIRAC import S_OK, S_ERROR, gLogger

class ExecutorState:

  def __init__( self, log = False ):
    if log:
      self.__log = log
    else:
      self.__log = gLogger
    self.__lock = threading.Lock()
    self.__typeToId = {}
    self.__maxTasks = {}
    self.__execTasks = {}
    self.__taskInExec = {}

  def _internals( self ):
    return { 't2id' : dict( self.__typeToId ),
             'maxT' : dict( self.__maxTasks ),
             'task' : dict( self.__execTasks ),
             'tine' : dict( self.__taskInExec ), }

  def addExecutor( self, execId, eType, maxTasks = 1 ):
    self.__lock.acquire()
    try:
      self.__maxTasks[ execId ] = maxTasks
      if execId not in self.__execTasks:
        self.__execTasks[ execId ] = set()
      if eType not in self.__typeToId:
        self.__typeToId[ eType ] = set()
      self.__typeToId[ eType ].add( execId )
    finally:
      self.__lock.release()

  def removeExecutor( self, execId ):
    self.__lock.acquire()
    try:
      for eType in self.__typeToId:
        if execId in self.__typeToId[ eType ]:
          self.__typeToId[ eType ].remove( execId )
      for taskId in self.__execTasks[ execId ]:
        self.__taskInExec.pop( taskId )
      self.__execTasks.pop( execId )
      self.__maxTasks.pop( execId )
    finally:
      self.__lock.release()

  def full( self, execId ):
    try:
      return len( self.__execTasks[ execId ] ) >= self.__maxTasks[ execId ]
    except KeyError:
      return True

  def freeSlots( self, execId ):
    try:
      return self.__maxTasks[ execId ] - len( self.__execTasks[ execId ] )
    except KeyError:
      return 0

  def getFreeExecutors( self, eType ):
    execs = {}
    try:
      eids = self.__typeToId[ eType ]
    except KeyError:
      return execs
    try:
      for eid in eids:
        freeSlots = self.freeSlots( eid )
        if freeSlots:
          execs[ eid ] = freeSlots
    except RuntimeError:
      pass
    return execs

  def getIdleExecutor( self, eType ):
    idleId = None
    maxFreeSlots = 0
    try:
      for eId in self.__typeToId[ eType ]:
        freeSlots = self.freeSlots( eId )
        if freeSlots > maxFreeSlots:
          maxFreeSlots = freeSlots
          idleId = eId
    except KeyError:
      pass
    return idleId

  def addTask( self, execId, taskId ):
    self.__lock.acquire()
    try:
      try:
        self.__taskInExec[ taskId ] = execId
        self.__execTasks[ execId ].add( taskId )
        return len( self.__execTasks[ execId ] )
      except KeyError:
        return 0
    finally:
      self.__lock.release()

  def removeTask( self, taskId, execId = None ):
    self.__lock.acquire()
    try:
      try:
        if execId == None:
          execId = self.__taskInExec[ taskId ]
        self.__execTasks[ execId ].remove( taskId )
        self.__taskInExec.pop( taskId )
        return len( self.__execTasks[ execId ] )
      except KeyError:
        return 0
    finally:
      self.__lock.release()

  def getTasksForExecutor( self, execId ):
    try:
      return list( self.__execTasks[ execId ] )
    except KeyError:
      return []


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
    return [ eType for eType in self.__queues ]

  def pushTask( self, eType, taskId, ahead = False ):
    self.__log.info( "Pushing task %d into waiting queue for executor %s" % ( jid, eType ) )
    self.__lock.acquire()
    try:
      if taskId in self.__taskInQueue:
        if self.__taskInQueue[ taskId ] != eType:
          raise Exception( "Task %s cannot be queued because it's already queued" % taskId )
        else:
          return
      if eType not in self.__queues:
        self.__queues[ eType ] = []
      self.__lastUse[ eType ] = time.time()
      if ahead:
        self.__queues[ eType ].insert( 0, taskId )
      else:
        self.__queues[ eType ].append( taskId )
      self.__taskInQueue[ taskId ] = eType
    finally:
      self.__lock.release()

  def popTask( self, eType ):
    self.__lock.acquire()
    try:
      try:
        taskId = self.__queues[ eType ].pop( 0 )
        del( self.__taskInQueue[ taskId ] )
      except IndexError:
        return None
      except KeyError:
        return None
    finally:
      self.__lock.release()
    self.__log.info( "Popped task %s from executor %s waiting queue" % ( taskId, eType ) )
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
        eType = self.__taskInQueue[ taskId ]
        del( self.__taskInQueue[ taskId ] )
      except KeyError:
        return
      try:
        iPos = self.__queues[ eType ].index( taskId )
      except ValueError:
        return
      del( self.__queues[ eType ][ iPos ] )
    finally:
      self.__lock.release()

  def waitingTasks( self, eType ):
    self.__lock.acquire()
    try:
      try:
        return len( self.__queues[ eType ] )
      except KeyError:
        return 0
    finally:
      self.__lock.release()

class ExecutorMind:

  def __init__( self ):
    self.__executorsLock = threading.Lock()
    self.__executors = {}
    self.__idMap = {}
    self.__tasksLock = threading.Lock()
    self.__tasks = {}
    self.__log = gLogger.getSubLogger( "ExecMind" )
    self.__dispatchCallback = self.__defaultCallback
    self.__taskFreezer = []
    self.__queues = ExecutorQueues( self.__log )
    self.__states = ExecutorStates( self.__log )

  def __defaultCallback( self, *args ):
    return S_ERROR( "No callback defined" )

  def addExecutor( self, eType, execId ):
    self.__log.info( "Adding new %s executor to the pool [%s][%s]" % ( eType, execId ) )
    self.__executorsLock.acquire()
    try:
      if eType not in self.__executors:
        self.__executors[ eType ] = []
      if exId in self.__idMap:
        return
      self.__idMap[ execId ] = eType
      self.__executors[ eType ].append( execId )
      self.__states.add( execId )
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

  def __dispatchTask( self, taskId ):
    result = self.__getNextExecutor( taskId )

    if not result[ 'OK' ]:
      self.__log.warn( "Error while calling dispatch callback: %s" % result[ 'Message' ] )
      if self.__addTaskToFreezer( taskId ):
        return S_OK()
      return S_ERROR( "Could not add task. Dispatching task failed" )

    eType = result[ 'Value' ]
    if not eType:
      self.__log.info( "No more executors for task %s" % taskId )
      return self.__removeTask( taskId )

    self.__queues.pushTask( eType, taskId )
    return self.__fillExecutors( eType )

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
    result = self.__dispatchTask( taskId )


  def __fillExecutors( self, eType ):
    eId = self.__states.getIdleExecutor( eType )
    while idleId:
      taskId = self.__queues.popTask( eType )
      if taskId == None:
        break
      self.__log.info( "Sending task %s to %s=%s" % ( taskId, eType, eId ) )
      result = self.__sendTaskToExecutor( eId, taskId )
      if not result[ 'OK' ]:
        self.__log.error( "Could not send task", "to %s: %s" % ( eId, result[ 'Message' ] ) )
        self.__queues.pushTask( eType, taskId, ahead = True )
      else:
        accepted = result[ 'Value' ]
        if accepted:
          self.__log.info( "Task %s was accepted by %s" % ( taskId, eId ) )
          self.__states.addTask( eId, taskId )
        else:
          self.__log.info( "Task %s was NOT accepted by %s" % ( taskId, eId ) )
          self.__queues.pushTask( eType, taskId, ahead = True )
      eId = self.__states.getIdleExecutor( eType )

  def __sendTaskToExecutor( self, eId, taskId ):



if __name__ == "__main__":
  def testExecState():
    execState = ExecutorState()
    execState.addExecutor( 1, "type1", 2 )
    print execState.freeSlots( 1 ) == 2
    print execState.addTask( 1, "t1" ) == 1
    print execState.addTask( 1, "t1" ) == 1
    print execState.addTask( 1, "t2" ) == 2
    print execState.freeSlots( 1 ) == 0
    print execState.full( 1 ) == True
    print execState.removeTask( "t1" ) == 1
    print execState.freeSlots( 1 ) == 1
    print execState.getFreeExecutors( "type1" ) == {1:1}
    print execState.getTasksForExecutor( 1 ) == [ "t2" ]
    print execState.removeExecutor( 1 )
    print execState._internals()
