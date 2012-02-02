
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Base.ExecutorMindHandler import ExecutorMindHandler
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.Client.Job.JobState import JobState
from DIRAC.WorkloadManagementSystem.Client.Job.CachedJobState import CachedJobState

class OptimizationMindHandler( ExecutorMindHandler ):

  __jobDB = False
  __optimizationStates = [ 'Received', 'Checking' ]

  def __loadJobs(self):
    jobTypeCondition = self.srv_getCSOption( "JobTypeRestriction", [] )
    jobCond = { 'Status': self.__optimizationStates  }
    if jobTypeCondition:
      jobCond[ 'JobType' ] = jobTypeCondition
    result = self.__jobDB.selectJobs( jobCond, limit = self.srv_getCSOption( "JobQueryLimit",  1000 ) )
    if not result[ 'OK' ]:
      return result
    jidList = result[ 'Value' ]
    knownJids = self.getTaskIds()
    for jid in jidList:
      if jid not in knownJids:
        self.executeTask( jid, CachedJobState( jid ) )
    return S_OK()

  @classmethod
  def initializeHandler( cls, serviceInfoDict ):
    try:
      cls.__jobDB = JobDB()
    except Exception, excp:
      return S_ERROR( "Could not connect to JobDB" )
    return self.__loadJobs()

  @classmethod
  def exec_dispatch( self, taskId, cjs, pathExecuted ):
    result = cjs.commitChanges()
    if not result[ 'OK' ]:
      return result
    result = cjs.getStatus()
    if not result[ 'OK' ]:
      return S_ERROR( "Could not retrieve status: %s" % result[ 'Message' ] )
    status, minorStatus = result[ 'Value' ]
    #If not in proper state then end chain
    if status not in self.__optimizationStates:
      return S_OK()
    #If received send to JobPath
    if status == "Received":
      return S_OK( "JobPath" )
    result = cjs.getOptParameter( 'OptimizerChain' )
    if not result[ 'OK' ]:
      return S_ERROR( "Couldn't get OptimizerChain: %s" % result[ 'Message' ] )
    optChain = result[ 'Value' ]
    if minorStatus not in optChain:
      return S_ERROR( "Next optimizer %s not in chain %s" % ( minorStatus, optChain ) )
    return S_OK( minorStatus )

  @classmethod
  def exec_serializeTask( cls, cjs ):
    return S_OK( cjs.serialize() )

  @classmethod
  def exec_deserializeTask( self, taskStub ):
    return CachedJobState.deserialize( taskStub )

  @classmethod
  def exec_taskError( self, taskId, errorMsg ):
    js = JobState( taskId )
    result = jd.getStatus()
    #TODO: Set status to failed if not already failed

