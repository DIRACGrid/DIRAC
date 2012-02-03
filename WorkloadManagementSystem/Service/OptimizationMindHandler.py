
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode, ThreadScheduler
from DIRAC.Core.Base.ExecutorMindHandler import ExecutorMindHandler
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.Client.Job.JobState import JobState
from DIRAC.WorkloadManagementSystem.Client.Job.CachedJobState import CachedJobState

class OptimizationMindHandler( ExecutorMindHandler ):

  __jobDB = False
  __optimizationStates = [ 'Received', 'Checking' ]
  __loadTaskId = False

  @classmethod
  def __loadJobs( cls ):
    log = cls.srv_log()
    if cls.__loadTaskId:
      period = cls.srv_getCSOption( "LoadJobPeriod", 300 )
      ThreadScheduler.gThreadScheduler.setTaskPeriod( cls.__loadTaskId, period )
    eConn = cls.getExecutorsConnected()
    if not eConn:
      log.info( "No optimizer connected. Skipping load" )
      return S_OK()
    jobTypeCondition = cls.srv_getCSOption( "JobTypeRestriction", [] )
    jobCond = { 'Status': cls.__optimizationStates, 'MinorStatus' : eConn.keys()  }
    if jobTypeCondition:
      jobCond[ 'JobType' ] = jobTypeCondition
    result = cls.__jobDB.selectJobs( jobCond, limit = cls.srv_getCSOption( "JobQueryLimit", 1000 ) )
    if not result[ 'OK' ]:
      return result
    jidList = result[ 'Value' ]
    knownJids = cls.getTaskIds()
    for jid in jidList:
      if jid not in knownJids:
        cls.executeTask( jid, CachedJobState( jid ) )
    return S_OK()

  @classmethod
  def initializeHandler( cls, serviceInfoDict ):
    try:
      cls.__jobDB = JobDB()
    except Exception, excp:
      return S_ERROR( "Could not connect to JobDB" )
    cls.setFailedOnTooFrozen( False )
    cls.setFreezeOnFailedDispatch( False )
    cls.setFreezeOnUnknownExecutor( False )

  @classmethod
  def setFreezeOnUnknownExecutor( self, value ):
    period = cls.srv_getCSOption( "LoadJobPeriod", 60 )
    cls.__loadTaskId = ThreadScheduler.gThreadScheduler.addPeriodicTask( period, cls.__loadJobs )
    return cls.__loadJobs()

  @classmethod
  def exec_dispatch( cls, jid, cjs, pathExecuted ):
    log = cls.srv_log()
    log.info( "Saving changes for job %s" % jid )
    result = cjs.commitChanges()
    if not result[ 'OK' ]:
      log.error( "Could not save changes for job", "%s: %s" % ( jid, result[ 'Message' ] ) )
      return result
    result = cjs.getStatus()
    if not result[ 'OK' ]:
      log.error( "Could not get status for job", "%s: %s" % ( jid, result[ 'Message' ] ) )
      return S_ERROR( "Could not retrieve status: %s" % result[ 'Message' ] )
    status, minorStatus = result[ 'Value' ]
    #If not in proper state then end chain
    if status not in cls.__optimizationStates:
      log.error( "Dispatching job %s out of optimization" % jid )
      return S_OK()
    #If received send to JobPath
    if status == "Received":
      log.error( "Dispatching job %s to JobPath" % jid )
      return S_OK( "JobPath" )
    result = cjs.getOptParameter( 'OptimizerChain' )
    if not result[ 'OK' ]:
      log.error( "Could not get optimizer chain for job", "%s: %s" % ( jid, result[ 'Message' ] ) )
      return S_ERROR( "Couldn't get OptimizerChain: %s" % result[ 'Message' ] )
    optChain = result[ 'Value' ]
    if minorStatus not in optChain:
      log.error( "Next optimizer is not in the chain for job", "%s: %s not in %s" % ( jid, minorStatus, optChain ) )
      return S_ERROR( "Next optimizer %s not in chain %s" % ( minorStatus, optChain ) )
    log.error( "Dispatching job %s to %s" % ( jid, minorStatus ) )
    return S_OK( minorStatus )

  @classmethod
  def exec_serializeTask( cls, cjs ):
    return S_OK( cjs.serialize() )

  @classmethod
  def exec_deserializeTask( cls, taskStub ):
    return CachedJobState.deserialize( taskStub )

  @classmethod
  def exec_taskError( cls, jid, errorMsg ):
    js = JobState( jid )
    result = jd.getStatus()
    if result[ 'OK' ]:
      if result[ 'Value' ][0].lower() == "failed":
        return S_OK()
    else:
      cls.srv_log().error( "Could not get status of job %s: %s" % ( jid, result[ 'Message ' ] ) )
    return jd.setStatus( "Failed", errorMsg )

