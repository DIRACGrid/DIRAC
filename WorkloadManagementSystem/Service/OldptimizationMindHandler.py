__RCSID__ = "$Id$"

import types
import threading
import time
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.WorkloadManagementSystem.API.Job import Job

class JobOptimizationQueues:

  def __init__( self ):
    self.__lock = threading.Lock()
    self.__queues = {}
    self.__lastUse = {}
    self.__jobInQueue = {}

  def getOptimizerList( self ):
    return [ opName for opName in self.__queues ]

  def pushJob( self, opName, jid, ahead = False ):
    gLogger.info( "Pushing job %d into waiting queue for optimizer %s" % ( jid, opName ) )
    self.__lock.acquire()
    try:
      if jid in self.__jobInQueue and self.__jobInQueue[ jid ] != opName:
        raise Exception( "Job %s cannot be queued because it's already queued" % jid )
      if opName not in self.__queues:
        self.__queues[ opName ] = []
        self.__lastUse[ opName ] = time.time()
      if ahead:
        self.__queues[ opName ].insert( 0, jid )
      else:
        self.__queues[ opName ].append( jid )
      self.__jobInQueue[ jid ] = opName
    finally:
      self.__lock.release()

  def popJob( self, opName ):
    self.__lock.acquire()
    try:
      try:
        jid = self.__queues[ opName ].pop( 0 )
        del( self.__jobInQueue[ jid ] )
      except IndexError:
        return None
      except KeyError:
        return None
    finally:
      self.__lock.release()
    gLogger.info( "Popped job %s from optimizer %s waiting queue" % ( jid, opName ) )
    return jid

  def getState( self ):
    self.__lock.acquire()
    try:
      qInfo = {}
      for qName in self.__queues:
        qInfo[ qName ] = list( self.__queues[ qName ] )
    finally:
      self.__lock.release()
    return qInfo


  def deleteJob( self, jid ):
    gLogger.verbose( "Deleting job %d from waiting queues" % jid )
    self.__lock.acquire()
    try:
      try:
        opName = self.__jobInQueue[ jid ]
        del( self.__jobInQueue[ jid ] )
      except KeyError:
        return
      try:
        iPos = self.__queues[ opName ].index( jid )
      except ValueError:
        return
      del( self.__queues[ opName ][ iPos ] )
    finally:
      self.__lock.release()

  def waitingJobs( self, opName ):
    self.__lock.acquire()
    try:
      try:
        return len( self.__queues[ opName ] )
      except KeyError:
        return 0
    finally:
      self.__lock.release()

class JobsState:

  def __init__( self ):
    self.__jobData = {}
    self.__lock = threading.Lock()

  def loadJob( self, jid ):
    gLogger.verbose( "Loading job data for jid %s" % jid )
    self.__lock.acquire()
    try:
      if jid in self.__jobData:
        return S_OK( self.__jobData[ jid ] )
      return self.__innerLoadJob( jid )
    finally:
      self.__lock.release()

  def __innerLoadJob( self, jid ):
    j = Job( jid = jid )
    self.__jobData[ jid ] = j
    return S_OK( j )

  def forgetJob( self, jid ):
    gLogger.verbose( "Forgetting about jid %s" % jid )
    self.__lock.acquire()
    try:
      return self.__innerForgetJob( jid )
    finally:
      self.__lock.release()

  def __innerForgetJob( self, jid ):
    try:
      del( self.__jobData[ jid ] )
    except KeyError:
      pass
    return S_OK()

  def getJob( self, jid ):
    try:
      return S_OK( self.__jobData[ jid ] )
    except KeyError:
      return self.loadJob( jid )

  def getNextOptimizer( self, jid ):
    gLogger.verbose( "Getting next optimizer for job %d" % jid )
    result = self.getJob( jid )
    if not result[ 'OK' ]:
      return S_ERROR( "%s while getting next optimizer" % result[ 'Message' ] )
    job = result[ 'Value' ]

    #TODO:
    return S_OK( "WorkloadManagement/JobPath" )

  def applyChanges( self, jid, job ):
    #TODO:
    return S_OK()


class OptimizerState:

  def __init__( self, opName, trid, opid, msgSender ):
    self.__trid = trid
    self.__lock = threading.Lock()
    self.__msgSender = msgSender
    self.__name = opName
    self.__bookingEnabled = False
    self.__frozen = False
    self.__opid = opid
    self.__maxSlots = 1
    self.__optimizedJobs = 0
    self.__jobsInOptimizer = set()
    self.__bookings = {}
    self.__maxBookingTime = 5

  def getTrid( self ):
    return self.__trid

  def freeze( self ):
    self.__frozen = True

  def isFrozen( self ):
    return self.__frozen

  def setSlots( self, slots ):
    try:
      self.__maxSlots = max( 1, int( slots ) )
    except:
      pass

  def enableBookings( self ):
    self.__bookingEnabled = True

  def isBookingEnabled( self ):
    return self.__bookingEnabled

  def getName( self ):
    return self.__name

  def bookOptimization( self, jid ):
    if not self.__bookingEnabled:
      return False
    self.__lock.acquire()
    try:
      if self.__frozen:
        return False
      now = time.time()
      for jid in set( self.__bookings ):
        if now - self.__bookings[ jid ] > self.__maxBookingTime:
          del( self.__bookings[ jid ] )
      if self.getFreeSlots() > 0:
        self.__bookings[ jid ] = now
        return True
      return False
    finally:
      self.__lock.release()

  def getBookings( self ):
    return dict( self.__bookings )

  def requestOptimization( self, job ):
    jid = job.getJobId()
    gLogger.info( "Sending job %d to optimizer %s" % ( jid, self.__name ) )
    self.__lock.acquire()
    try:
      if self.__frozen:
        return S_ERROR( "Optimizer is frozen" )
      if jid in self.__jobsInOptimizer:
        gLogger.warn( "Job %d already in optimizer %s" % ( jid, self.__name ) )
        return S_OK()
      if jid in self.__bookings:
        del( self.__bookings[ jid ] )
      self.__jobsInOptimizer.add( jid )
    finally:
      self.__lock.release()

    result = job.dumpToStub()
    if not result[ 'OK' ]:
      return result
    jobStub = result[ 'Value' ]
    gLogger.always( "Sending job %s to %s [%s]" % ( jid, self.__name, self.__trid ) )
    result = self.__msgSender.sendMessage( self.__trid, 'optimizeJob', jid, jobStub )
    if not result[ 'OK' ]:
      self.jobExitedOptimizer( jid )
    return result

  def jobExitedOptimizer( self, jid ):
    gLogger.verbose( "Removing job %s from optimizer %s state" % ( jid, self.__name ) )
    self.__lock.acquire()
    try:
      if self.__frozen:
        return S_ERROR( "Optimizer is frozen" )
      if jid not in self.__jobsInOptimizer:
        return S_ERROR( "Job %d wasn't being optimized by this optimizer" % jid )
      self.__jobsInOptimizer.remove( jid )
      self.__optimizedJobs += 1
      return S_OK()
    finally:
      self.__lock.release()

  def getJobs( self ):
    return set( self.__jobsInOptimizer )

  def getNumOptimizedJobs( self ):
    return self.__optimizedJobs

  def setMaxSlots( self, maxSlots ):
    if self.__frozen:
      return 0
    self.__maxSlots = maxSlots

  def getMaxSlots( self ):
    return self.__maxSlots

  def getFreeSlots( self ):
    if self.__frozen:
      return 0
    return max( 0, self.__maxSlots - len( self.__jobsInOptimizer ) - len( self.__bookings ) )

class OptimizationMind:

  def __init__( self, messageSender ):
    self.__jobsState = JobsState()
    self.__opAdRemLock = threading.Lock()
    self.__optimizers = {}
    self.__optimizersByTrid = {}
    self.__jobsLock = threading.Lock()
    self.__jobs = set()
    self.__jobOpQueue = JobOptimizationQueues()
    self.__msgSender = messageSender

  def getOptimizersStatus( self ):
    self.__opAdRemLock.acquire()
    try:
      opInfo = {}
      for opName in self.__optimizers:
        opInfo[ opName ] = {}
        for opState in self.__optimizers[ opName ]:
          opInfo[ opName ][ opState.getTrid() ] = { 'frozen' : opState.isFrozen(),
                                                    'bookingEnabled' : opState.isBookingEnabled(),
                                                    'jobs' : opState.getJobs(),
                                                    'maxSlots' : opState.getMaxSlots(),
                                                    'bookings' : opState.getBookings()
                                                   }
      return opInfo
    finally:
      self.__opAdRemLock.release()

  def getQueuesStatus( self ):
    return self.__jobOpQueue.getState()

  # Connect & disconnect optimizers

  def optimizerConnected( self, opid, trid, opName, extraArgs ):
    self.__opAdRemLock.acquire()
    try:
      gLogger.info( "New %s optimizer connected [%s]" % ( opName, trid ) )
      if opName not in self.__optimizers:
        self.__optimizers[ opName ] = []
      opState = OptimizerState( opName, trid, opid, self.__msgSender )
      if 'slots' in extraArgs:
        opState.setSlots( extraArgs[ 'slots' ] )
      self.__optimizers[ opName ].append( opState )
      self.__optimizersByTrid[ trid ] = opState
    finally:
      self.__opAdRemLock.release()
    return S_OK()

  def optimizerDisconnected( self, disconnectedTrid ):
    self.__opAdRemLock.acquire()
    try:
      if disconnectedTrid not in self.__optimizersByTrid:
        return S_ERROR( "OOps, trid %s is unknown" % disconnectedTrid )
      gLogger.info( "Optimizer disconnected [%s]" % disconnectedTrid )
      opState = self.__optimizersByTrid[ disconnectedTrid ]
      opState.freeze()
      opName = opState.getName()
      foundPos = 0
      for testOpState in self.__optimizers[ opName ]:
        if disconnectedTrid == testOpState.getTrid():
          break
        foundPos += 1
      del( self.__optimizersByTrid[ disconnectedTrid ] )
      if foundPos < len( self.__optimizers[ opName ] ):
        del( self.__optimizers[ opName ][ foundPos ] )
    finally:
      self.__opAdRemLock.release()
    #Reoptimize jobs in opState
    gLogger.info( "Reoptimizing jobs that were in optimizer %s" % disconnectedTrid )
    for jid in opState.getJobs():
      self.__dispatch( jid )
    return S_OK()

  def startOptimizer( self, trid ):
    try:
      opState = self.__optimizersByTrid[ trid ]
    except KeyError:
      return
    opState.enableBookings()
    self.__fillOptimizers( opState.getName() )

  #Receive job

  def addJobIfNotKnown( self, jid ):
    self.__jobsLock.acquire()
    try:
      if jid not in self.__jobs:
        self.__jobs.add( jid )
        return True
      return False
    finally:
      self.__jobsLock.release()

  def optimizeJob( self, jid ):
    if not self.addJobIfNotKnown( jid ):
      #Already in the system
      return S_OK()
    result = self.__jobsState.loadJob( jid )
    gLogger.verbose( "Loaded Job %s: %s" % ( jid, result ) )
    if not result[ 'OK' ]:
      return result
    return self.__dispatch( jid )

  def __dispatch( self, jid ):
    result = self.__queueJob( jid )
    if not result[ 'OK' ]:
      return result
    opName = result[ 'Value' ]
    return self.__fillOptimizers( opName )

  def __queueJob( self, jid, opName = False ):
    if not opName:
      result = self.__jobsState.getNextOptimizer( jid )
      if not result[ 'OK' ]:
        gLogger.error( "Could not find next optimizer for job", "%d: %s" % ( jid, result['Message' ] ) )
        return self.__jobsState.forgetJob( jid )
      opName = result[ 'Value' ]
    #Push job into queue
    self.__jobOpQueue.pushJob( opName, jid )
    return S_OK( opName )

  def __fillOptimizers( self, opName ):
    gLogger.verbose( "Filling %s optimizers" % opName )
    while True:
      #Get first job if any
      jid = self.__jobOpQueue.popJob( opName )
      if jid == None:
        gLogger.verbose( "No more jobs for optimizers %s" % opName )
        #No more jobs
        return S_OK()
      result = self.__bookJobInOptimizer( opName, jid )
      if not result[ 'OK' ]:
        gLogger.info( "No empty %s optimizers now" % opName )
        self.__jobOpQueue.pushJob( opName, jid, ahead = True )
        return S_OK()
      opState = result[ 'Value' ]
      result = self.__jobsState.getJob( jid )
      if not result[ 'OK' ]:
        gLogger.warn( "Could not load job data", "for jid %s" % jid )
        self.__jobsState.forgetJob( jid )
        continue
      job = result[ 'Value' ]
      result = opState.requestOptimization( job )
      if not result[ 'OK' ]:
        self.__jobOpQueue.pushJob( opName, jid, ahead = True )
        gLogger.warn( "Could not request optimization", result[ 'Message' ] )
        self.optimizerDisconnected( opState.getTrid() )
    return S_OK()

  def __bookJobInOptimizer( self, opName, jid ):
    self.__opAdRemLock.acquire()
    gLogger.verbose( "Trying to book job %d" % jid )
    try:
      if opName not in self.__optimizers:
        return S_ERROR( "No optimizer with name %s" % opName )
      selOpState = False
      selFreeSlots = 0
      selJobs = 0
      for opState in self.__optimizers[ opName ]:
        if not opState.isBookingEnabled():
          continue
        opFree = opState.getFreeSlots()
        opJobs = opState.getNumOptimizedJobs()
        gLogger.verbose( "Checking opState %s [%d Free / %d Done]" % ( opState.getTrid(), opFree, opJobs ) )
        if selFreeSlots == 0 :
          selJobs = opJobs + 1
        if selFreeSlots < opFree and selJobs > opJobs:
          selFreeSlots = opFree
          selJobs = opJobs
          selOpState = opState
    finally:
      self.__opAdRemLock.release()
    if not selOpState or not selOpState.bookOptimization( jid ):
      return S_ERROR( "No idle optimizer %s" % opName )
    gLogger.verbose( "Booked job %d to opState %s" % ( jid, opState.getTrid() ) )
    return S_OK( selOpState )


  def optimizationOK( self, trid, job ):
    jid = job.getJobId()
    try:
      opState = self.__optimizersByTrid[ trid ]
    except KeyError:
      #Optimizer died!
      return S_ERROR( "Optimizer is not connected" )

    result = opState.jobExitedOptimizer( jid )
    if not result[ 'OK' ]:
      self.__checkJobState( jid )
      return result

    self.__jobsState.applyChanges( jid, job )
    return self.__dispatch( jid )

  def optimizationFailed( self, trid, jid, msg ):
    #TODO:
    pass

  def __checkJobState( self, jid ):
    #TODO: Check if the job has been idle in any queue for more than X minutes
    return


gOptimizationMind = False

def initializeOptimizationMindHandler( servInfo ):
  global gOptimizationMind
  gOptimizationMind = OptimizationMind( servInfo[ 'messageSender' ] )
  return S_OK()


class OptimizationMindHandler( RequestHandler ):


  MSG_DEFINITIONS = { 'optimizeJob' :  { 'jid' : ( types.LongType, types.IntType ) },
                      'jobOptimizationOK' : { 'jid' : ( types.LongType, types.IntType ),
                                              'jobStub' : types.StringType  }
                    }

  def conn_new( self, trid, identity, kwargs ):
    gLogger.info( "[CML] NEW CLLBCK %s" % trid )
    if 'optimizerName' not in kwargs or not kwargs[ 'optimizerName']:
      return S_ERROR( "Only optimizers are allowed to connect" )
    return gOptimizationMind.optimizerConnected( identity, trid, kwargs[ 'optimizerName' ], kwargs )

  def conn_connected( self, trid, identity, kwargs ):
    gLogger.info( "[CML] CONN %s" % trid )
    gOptimizationMind.startOptimizer( trid )
    return S_OK()

  def conn_drop( self, trid ):
    return gOptimizationMind.optimizerDisconnected( trid )

  types_msg_optimizeJob = [ ( types.LongType, types.IntType ) ]
  auth_msg_optimizeJob = [ 'All' ]
  def msg_optimizeJob( self, jid ):
    return gOptimizationMind.optimizeJob( jid )

  types_msg_jobOptimizationOK = [ ( types.LongType, types.IntType ), types.StringType ]
  auth_msg_jobOptimizationOK = [ 'All' ]
  def msg_jobOptimizationOK( self, jid, jobStub ):
    trid = self.srv_getTransportID()
    result = Job.loadFromStub( jobStub )
    if not result[ 'OK' ]:
      return gOptimizationMind.optimizationFailed( trid, jid, result[ 'Message' ] )
    job = result[ 'Value' ]
    if jid != job.getJobId():
      return gOptimizationMind.optimizationFailed( trid, jid, "Received job id and defined job id differ" )
    gOptimizationMind.optimizationOK( trid, job )
    return S_OK()
