
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode, ThreadScheduler
from DIRAC.Core.Base.ExecutorMindHandler import ExecutorMindHandler
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.Client.JobState.CachedJobState import CachedJobState

class OptimizationMindHandler( ExecutorMindHandler ):

  __jobDB = False
  __optimizationStates = [ 'Received', 'Checking' ]
  __loadTaskId = False

  @classmethod
  def __loadJobs( cls, eType = None ):
    log = cls.log
    if cls.__loadTaskId:
      period = cls.srv_getCSOption( "LoadJobPeriod", 300 )
      ThreadScheduler.gThreadScheduler.setTaskPeriod( cls.__loadTaskId, period )
    if not eType:
      eConn = cls.getExecutorsConnected()
      eTypes = [ eType for eType in eConn if eConn[ eType ] > 0 ]
    else:
      eTypes = [ eType ]
    if not eTypes:
      log.info( "No optimizer connected. Skipping load" )
      return S_OK()
    log.info( "Getting jobs for %s" % ",".join( eTypes ) )
    for opState in cls.__optimizationStates:
      #For Received states
      if opState == "Received":
          if 'JobPath' not in eTypes:
            continue
          jobCond = { 'Status' : opState }
      #For checking states
      if opState == "Checking":
        eCheckingTypes = eTypes
        if 'JobPath' in eCheckingTypes:
          eCheckingTypes.remove( 'JobPath' )
        if not eCheckingTypes:
          continue
        jobCond = { 'Status': opState, 'MinorStatus' : eCheckingTypes }
      #Do the magic
      jobTypeCondition = cls.srv_getCSOption( "JobTypeRestriction", [] )
      if jobTypeCondition:
        jobCond[ 'JobType' ] = jobTypeCondition
      result = cls.__jobDB.selectJobs( jobCond, limit = cls.srv_getCSOption( "JobQueryLimit", 10000 ) )
      if not result[ 'OK' ]:
        return result
      jidList = result[ 'Value' ]
      knownJids = cls.getTaskIds()
      added = 0
      for jid in jidList:
        jid = long( jid )
        if jid not in knownJids:
          cls.executeTask( jid, CachedJobState( jid ) )
          added += 1
      log.info( "Added %s/%s jobs for %s state" % ( added, len( jidList ), opState ) )
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
    period = cls.srv_getCSOption( "LoadJobPeriod", 60 )
    result = ThreadScheduler.gThreadScheduler.addPeriodicTask( period, cls.__loadJobs )
    if not result[ 'OK' ]:
      return result
    cls.__loadTaskId = result[ 'Value' ]
    return cls.__loadJobs()

  @classmethod
  def exec_executorConnected( cls, name, trid ):
    return cls.__loadJobs( name )

  @classmethod
  def exec_taskProcessed( cls, jid, jobState, eType ):
    cls.log.info( "Saving changes for job %s after %s" % ( jid, eType ) )
    result = jobState.commitChanges()
    if not result[ 'OK' ]:
      cls.log.error( "Could not save changes for job", "%s: %s" % ( jid, result[ 'Message' ] ) )
    return result

  @classmethod
  def exec_dispatch( cls, jid, jobState, pathExecuted ):
    result = jobState.getStatus()
    if not result[ 'OK' ]:
      cls.log.error( "Could not get status for job", "%s: %s" % ( jid, result[ 'Message' ] ) )
      return S_ERROR( "Could not retrieve status: %s" % result[ 'Message' ] )
    status, minorStatus = result[ 'Value' ]
    #If not in proper state then end chain
    if status not in cls.__optimizationStates:
      cls.log.error( "Dispatching job %s out of optimization" % jid )
      return S_OK()
    #If received send to JobPath
    if status == "Received":
      cls.log.error( "Dispatching job %s to JobPath" % jid )
      return S_OK( "JobPath" )
    result = jobState.getOptParameter( 'OptimizerChain' )
    if not result[ 'OK' ]:
      cls.log.error( "Could not get optimizer chain for job", "%s: %s" % ( jid, result[ 'Message' ] ) )
      return S_ERROR( "Couldn't get OptimizerChain: %s" % result[ 'Message' ] )
    optChain = result[ 'Value' ]
    if minorStatus not in optChain:
      cls.log.error( "Next optimizer is not in the chain for job", "%s: %s not in %s" % ( jid, minorStatus, optChain ) )
      return S_ERROR( "Next optimizer %s not in chain %s" % ( minorStatus, optChain ) )
    cls.log.info( "Dispatching job %s to %s" % ( jid, minorStatus ) )
    return S_OK( minorStatus )

  @classmethod
  def exec_serializeTask( cls, jobState ):
    return S_OK( jobState.serialize() )

  @classmethod
  def exec_deserializeTask( cls, taskStub ):
    return CachedJobState.deserialize( taskStub )

  @classmethod
  def exec_taskError( cls, jid, errorMsg ):
    jobState = JobState( jid )
    result = jobState.getStatus()
    if result[ 'OK' ]:
      if result[ 'Value' ][0].lower() == "failed":
        return S_OK()
    else:
      cls.log.error( "Could not get status of job %s: %s" % ( jid, result[ 'Message ' ] ) )
    cls.log.notice( "Job %s: Setting to Failed|%s" % ( jid, errorMsg ) )
    return jobState.setStatus( "Failed", errorMsg )

