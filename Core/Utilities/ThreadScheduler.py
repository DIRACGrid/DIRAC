
from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.Core.Utilities import ThreadSafe
import md5
import threading
import time

gSchedulerLock = ThreadSafe.Synchronizer()

class ThreadScheduler:

  def __init__(self):
    self.__thId = False
    self.__minPeriod = 1
    self.__taskDict = {}
    self.__hood = []
    self.__event = threading.Event()
    self.__log = gLogger.getSubLogger( "ThreadScheduler" )

  def addPeriodicTask( self, period, taskFunc, taskArgs = (), executions = 0, elapsedTime = 0 ):
    if not callable( taskFunc ):
      return S_ERROR( "%s is not callable" % str( taskFunc ) )
    period = max( period, self.__minPeriod )
    elapsedTime = min( elapsedTime, period )
    md = md5.new()
    task = { 'period' : period,
             'func' : taskFunc,
             'args' : taskArgs,
            }
    md.update( str( task ) )
    taskId = md.hexdigest()
    if taskId in self.__taskDict:
      return S_ERROR( "Task %s is already added" % taskId )
    if executions:
      task[ 'executions' ] = executions
    self.__taskDict[ taskId ] = task
    self.__log.debug( "Added task %s\n%s" % ( taskId, str( task ) ) )
    executeInSecs = period
    if elapsedTime:
      executeInSecs -= elapsedTime
    retVal = self.__scheduleTask( taskId, executeInSecs )
    if not retVal[ 'OK' ]:
      return retVal
    self.__createExecutorIfNeeded()
    return S_OK( taskId )

  def addSingleTask( self, taskFunc, taskArgs = () ):
    return self.addPeriodicTask( self.__minPeriod,
                                 taskFunc,
                                 taskArgs,
                                 executions = 1,
                                 elapsedTime = self.__minPeriod )

  @gSchedulerLock
  def __scheduleTask( self, taskId, executeInSecs = 0 ):
    if not executeInSecs:
      executeInSecs = self.__taskDict[ taskId ][ 'period' ]

    now = time.time()
    for i in range( len( self.__hood ) ):
      if taskId == self.__hood[i][0]:
        tD = self.__hood[i][1] - now
        if abs( tD - executeInSecs ) < 30:
          return S_OK()
        else:
          del( self.__hood[ i ] )
          break

    executionTime = now + executeInSecs
    inserted = False
    for i in range( len( self.__hood ) ):
      if executionTime < self.__hood[i][1]:
        self.__hood.insert( i, ( taskId, executionTime ) )
        inserted = True
        break
    if not inserted:
      self.__hood.append( ( taskId, executionTime ) )

    self.__event.set()

    return S_OK()

  def __executorThread(self):
    self.__log.debug( "Starting executor thread" )
    while len( self.__hood ) > 0:
      timeToWait = self.__timeToNextTask()
      self.__log.debug( "Need to wait %s secs until next task" % timeToWait )
      while timeToWait and timeToWait > 0:
        self.__event.clear()
        self.__event.wait( timeToWait )
        self.__log.debug( "Woke up after event" )
        timeToWait = self.__timeToNextTask()
        self.__log.debug( "After wake up, need to wait for %s secs more" % timeToWait )
      if timeToWait == None:
        break
      taskId = self.__extractNextTask()
      self.__executeTask( taskId )
      self.__schedueIfNeeded( taskId )
    self.__log.debug( "Exiting executor thread" )
    #If we are leaving
    self.__destroyExecutor()

  @gSchedulerLock
  def __createExecutorIfNeeded(self):
    if self.__thId:
      return
    self.__log.debug( "Creating executor thread" )
    self.__thId = threading.Thread( target = self.__executorThread )
    self.__thId.setDaemon( True )
    self.__thId.start()

  @gSchedulerLock
  def __destroyExecutor(self):
    self.__log.debug( "Destroying executor thread" )
    self.__thId = False

  @gSchedulerLock
  def __timeToNextTask( self ):
    if len( self.__hood ) == 0:
      return None
    return self.__hood[0][1] - time.time()

  @gSchedulerLock
  def __extractNextTask( self ):
    if len( self.__hood ) == 0:
      return None
    return self.__hood.pop( 0 )[0]

  def __executeTask( self, taskId ):
    if taskId not in self.__taskDict:
      return False
    task = self.__taskDict[ taskId ]
    if 'executions' in task:
      task[ 'executions' ] -= 1
    try:
      task[ 'func' ]( *task[ 'args' ] )
    except Exception, e:
      #TODO: Something with e
      gLogger.exception( "Exception while executing scheduled task" )
      return False
    return True

  def __schedueIfNeeded( self, taskId ):
    if 'executions' in self.__taskDict[ taskId ]:
      if self.__taskDict[ taskId ][ 'executions' ] == 0:
        del( self.__taskDict[ taskId ] )
        return True
    return self.__scheduleTask( taskId )


gThreadScheduler = ThreadScheduler()