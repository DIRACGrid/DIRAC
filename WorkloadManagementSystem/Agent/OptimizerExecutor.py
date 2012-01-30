
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Base.Executor import Executor
from DIRAC.WorkloadManagementSystem.DB.JobDB         import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB  import JobLoggingDB

class OptimizerExecutor( Executor ):

  def initialize( self ):
    result = self.connect( "WorkloadManagement/OptimizationMind" )
    self.am_setOption( "ReconnectRetries", 10 )
    self.am_setOption( "ReconnectWaitTime", 10 )
    self.am_setModuleParam( 'optimizerName', self.am_getModuleParam( 'fullName' ) )
    self.jobDB = JobDB()
    self.logDB = JobLoggingDB()
    return result

  def processTask( self, taskId, taskObj ):
    return self.optimizeJob( taskId, taskObj )

  def optimizeJob( self, jobId, jobObj ):
    raise Exception( "You need to overwrite this method to optimize the job!" )

  def deserializeTask( self, taskStub ):
    #TODO: Serialize and deserialize jobs once decide how that plays
    return S_OK( result[0] )

  def serializeTask( self, taskObj ):
    #TODO: Serialize and deserialize jobs once decide how that plays
    return S_OK( DEncode.encode( taskObj ) )
