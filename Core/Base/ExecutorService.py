
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

  def addExecutor( self, eId, eType, maxTasks = 1 ):
    self.__lock.acquire()
    try:
      self.__maxTasks[ eId ] = maxTasks
      if eId not in self.__execTasks:
        self.__execTasks[ eId ] = set()
      if eType not in self.__typeToId:
        self.__typeToId[ eType ] = set()
      self.__typeToId[ eType ].add( eId )
    finally:
      self.__lock.release()

  def removeExecutor( self, eId ):
    self.__lock.acquire()
    try:
      tasks = []
      for eType in self.__typeToId:
        if eId in self.__typeToId[ eType ]:
          self.__typeToId[ eType ].remove( eId )
      for taskId in self.__execTasks[ eId ]:
        self.__taskInExec.pop( taskId )
        tasks.append( taskId )
      self.__execTasks.pop( eId )
      self.__maxTasks.pop( eId )
      return tasks
    finally:
      self.__lock.release()

  def getTasksForExecutor( self, eId ):
    try:
      return set( self.__execTasks[ eId ] )
    except KeyError:
      return set()

  def full( self, eId ):
    try:
      return len( self.__execTasks[ eId ] ) >= self.__maxTasks[ eId ]
    except KeyError:
      return True

  def freeSlots( self, eId ):
    try:
      return self.__maxTasks[ eId ] - len( self.__execTasks[ eId ] )
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

  def addTask( self, eId, taskId ):
    self.__lock.acquire()
    try:
      try:
        self.__taskInExec[ taskId ] = eId
        self.__execTasks[ eId ].add( taskId )
        return len( self.__execTasks[ eId ] )
      except KeyError:
        return 0
    finally:
      self.__lock.release()

  def getExecutorOfTask( self, taskId ):
    try:
      return self.__taskInExec[ taskId ]
    except KeyError:
      return None

  def removeTask( self, taskId, eId = None ):
    self.__lock.acquire()
    try:
      try:
        if eId == None:
          eId = self.__taskInExec[ taskId ]
        self.__execTasks[ eId ].remove( taskId )
        self.__taskInExec.pop( taskId )
        return True
      except KeyError:
        return False
    finally:
      self.__lock.release()

  def getTasksForExecutor( self, eId ):
    try:
      return list( self.__execTasks[ eId ] )
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
    self.__log.info( "Pushing task %s into waiting queue for executor %s" % ( taskId, eType ) )
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

class ExecutorMindCallbacks:

  def cbDispatch( self, taskId, taskObj ):
    return S_ERROR( "No dispatch callback defined" )

  def cbSendTask( self, eId, taskId, taskObj ):
    return S_ERROR( "No send task callback defined" )

  def cbDisconectExecutor( self, eId ):
    return S_ERROR( "No disconnect callback defined" )

class ExecutorMind:

  def __init__( self ):
    self.__idMap = {}
    self.__execTypes = {}
    self.__executorsLock = threading.Lock()
    self.__tasksLock = threading.Lock()
    self.__freezerLock = threading.Lock()
    self.__tasks = {}
    self.__log = gLogger.getSubLogger( "ExecMind" )
    self.__taskFreezer = []
    self.__queues = ExecutorQueues( self.__log )
    self.__states = ExecutorState( self.__log )
    self.__cbHolder = ExecutorMindCallbacks()

  def setCallbacks( self, callbacksObj ):
    if not isinstance( callbacksObj, ExecutorMindCallbacks ):
      return S_ERROR( "Callbacks object does not inherit from ExecutorMindCallbacks" )
    self.__cbHolder = callbacksObj
    return S_OK()

  def addExecutor( self, eType, eId ):
    self.__log.info( "Adding new %s executor to the pool %s" % ( eId, eType ) )
    self.__executorsLock.acquire()
    try:
      if eId in self.__idMap:
        return
      self.__idMap[ eId ] = eType
      if eType not in self.__execTypes:
        self.__execTypes[ eType ] = 0
      self.__execTypes[ eType ] += 1
      self.__states.addExecutor( eId, eType )
    finally:
      self.__executorsLock.release()
    self.__fillExecutors( eType )

  def removeExecutor( self, eId ):
    self.__log.info( "Removing executor %s" % eId )
    self.__executorsLock.acquire()
    try:
      if eId not in self.__idMap:
        return
      eType = self.__idMap.pop( eId )
      self.__execTypes[ eType ] -= 1
      tasksInExec = self.__states.removeExecutor( eId )
      for taskId in tasksInExec:
        self.__queues.pushTask( eType, taskId, ahead = True )
    finally:
      self.__executorsLock.release()
    try:
      self.__cbHolder.cbDisconectExecutor( eId )
    except:
      self.__log.exception( "Exception while disconnecting agent %s" % eId )
    self.__fillExecutors( eType )

  def __freezeTask( self, taskId, count = 0 ):
    self.__log.info( "Freezing task %s" % taskId )
    self.__freezerLock.acquire()
    try:
      for tf in self.__taskFreezer:
        if taskId == tf[1]:
          return
      self.__taskFreezer.append( ( time.time(), taskId, count ) )
    finally:
      self.__freezerLock.release()

  def __unfreezeTasks( self, timeLimit = 0 ):
    while True:
      self.__freezerLock.acquire()
      try:
        try:
          frozenTask = self.__taskFreezer.pop()
        except IndexError:
          return
        if timeLimit and time.time() - frozenTask[0] < abs( timeLimit ):
          self.__taskFreezer.insert( 0, frozenTask )
          return
      finally:
        self.__freezerLock.release()
      taskId = frozenTask[1]
      self.__log.info( "Unfreezed task %s" % taskId )
      result = self.__dispatchTask( taskId, defrozeIfNeeded = False )
      if not result[ 'OK' ]:
        self.__freezeTask( taskId, frozenTask[2] + 1 )

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

  def __dispatchTask( self, taskId, defrozeIfNeeded = True ):
    result = self.__getNextExecutor( taskId )

    if not result[ 'OK' ]:
      self.__log.warn( "Error while calling dispatch callback: %s" % result[ 'Message' ] )
      if self.__freezeTask( taskId ):
        return S_OK()
      return S_ERROR( "Could not add task. Dispatching task failed" )

    eType = result[ 'Value' ]
    if not eType:
      self.__log.info( "No more executors for task %s" % taskId )
      return self.removeTask( taskId )

    self.__log.info( "Next executor type is %s for task %s" % ( eType, taskId ) )
    if eType not in self.__execTypes:
      self.__log.info( "Executor type %s is unknown. Freezing task %s" % ( eType, taskId ) )
      return self.__freezeTask( taskId )


    self.__queues.pushTask( eType, taskId )
    self.__fillExecutors( eType, defrozeIfNeeded = defrozeIfNeeded )
    return S_OK()

  def __getNextExecutor( self, taskId ):
    try:
      taskObj = self.__tasks[ taskId ]
    except IndexError:
      msg = "Task %s was deleted prematurely while being dispatched" % taskId
      self.__log.error( msg )
      return S_ERROR( msg )
    try:
      return self.__cbHolder.cbDispatch( taskId, taskObj )
    except:
      self.__log.exception( "Exception while calling dispatch callback" )
      return S_ERROR( "Exception while calling dispatch callback" )

  def __feezeTask( self, taskId ):
    self.__tasksLock.acquire()
    try:
      if taskId not in self.__tasks:
        return False
      self.__log.info( "Adding task %s to freezer" % taskId )
      self.__freezeTask( taskId )
      return True
    finally:
      self.__tasksLock.release()

  def addTask( self, taskId, taskObj ):
    if not self.__addTaskIfNew( taskId, taskObj ):
      self.__unfreezeTasks()
      return S_OK()
    return self.__dispatchTask( taskId )

  def removeTask( self, taskId ):
    try:
      self.__tasks.pop( taskId )
    except KeyError:
      return S_OK()
    self.__queues.popTask( taskId )
    self.__states.removeTask( taskId )
    return S_OK()

  def taskProcessed( self, eId, taskId, taskObj = False ):
    if taskId not in self.__tasks:
      errMsg = "Task %s is not known" % taskId
      self.__log.error( errMsg )
      return S_ERROR( errMsg )
    if not self.__states.removeTask( taskId, eId ):
      errMsg = "Executor %s says it's processed task but it was not sent to it" % eId
      self.__log.error( errMsg )
      return S_ERROR( errMsg )
    if taskObj:
      self.__tasks[ taskId ] = taskObj
    self.__log.info( "Executor %s processed task %s" % ( eId, taskId ) )
    return self.__dispatchTask( taskId )

  def __fillExecutors( self, eType, defrozeIfNeeded = True ):
    self.__log.verbose( "Filling %s executors" % eType )
    eId = self.__states.getIdleExecutor( eType )
    processedTasks = set()
    while eId:
      taskId = self.__queues.popTask( eType )
      if taskId == None:
        self.__log.verbose( "No more tasks for %s" % eType )
        break
      if taskId in processedTasks:
        self.__queues.pushTask( eType, taskId, ahead = True )
        self.__log.info( "All tasks in the queue have been gone through" )
        return
      processedTasks.add( taskId )
      self.__log.info( "Sending task %s to %s=%s" % ( taskId, eType, eId ) )
      self.__states.addTask( eId, taskId )
      result = self.__sendTaskToExecutor( eId, taskId )
      if not result[ 'OK' ]:
        self.__log.error( "Could not send task", "to %s: %s" % ( eId, result[ 'Message' ] ) )
        self.__queues.pushTask( eType, taskId, ahead = True )
        self.__states.removeTask( taskId )
      else:
        self.__log.info( "Task %s was sent to %s" % ( taskId, eId ) )
      eId = self.__states.getIdleExecutor( eType )
    self.__log.verbose( "No more idle executors for %s" % eType )
    if defrozeIfNeeded:
      self.__unfreezeTasks()

  def __sendTaskToExecutor( self, eId, taskId ):
    try:
      return self.__cbHolder.cbSendTask( eId, taskId, self.__tasks[ taskId ] )
    except:
      self.__log.exception( "Exception while sending task to executor" )
    self.__log.info( "Disconnecting executor" )
    self.removeExecutor( eId )
    return S_ERROR( "Exception while sending task to executor" )



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

