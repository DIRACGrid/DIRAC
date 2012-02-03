
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import DEncode, List
from DIRAC.Core.Base.Executor import Executor
from DIRAC.WorkloadManagementSystem.Client.Job.CachedJobState import CachedJobState

class OptimizerExecutor( Executor ):

  def initialize( self ):
    self.__optimizerName = self.am_getModuleParam( 'fullName' )
    self.__optimizerName = "/".join( agentName.split( "/" )[1:] )
    result = self.connect( "WorkloadManagement/OptimizationMind", name = self.__optimizerName )
    if not result[ 'OK' ]:
      return result
    self.am_setOption( "ReconnectRetries", 10 )
    self.am_setOption( "ReconnectWaitTime", 10 )
    self.am_setModuleParam( 'optimizerName', self.__optimizerName )
    return initializeOptimizer()

  def am_optimizerName( self ):
    return self.__optimizerName

  def initializeOptimizer( self ):
    return S_OK()

  def processTask( self, jid, jobState ):
    result = self.optimizeJob( jid, jobState )
    #If the manifest is dirty, update it!
    manifest = jobState.getManifest()
    if manifest.isDirty():
      jobState.setManifest( manifest )
    #Did it go as expected? If not Failed!
    if not result[ 'OK' ]:
      return jobState.setStatus( "Failed", result[ 'Message' ] )
    return self.__setNextOptimizer()


  def optimizeJob( self, jid, jobState ):
    raise Exception( "You need to overwrite this method to optimize the job!" )

  def __setNextOptimizer( self ):
    result = jobState.getOptParameter( 'OptimizerChain' )
    if not result['OK']:
      return result
    opChain = List.fromChar( result[ 'Value' ], "," )
    opName = self.__optimizerName
    try:
      opIndex = opChain.find( opName )
    except ValueError:
      return S_ERROR( "Optimizer %s is not in the chain!" % opName )
    chainLength = len( opChain )
    if chainLength - 1 == opIndex:
      #This is the last optimizer in the chain!
      jobState.setState( self.am_getOption( 'WaitingStatus', 'Waiting' ),
                         self.am_getOption( 'WaitingMinorStatus', 'Pilot Agent Submission' ) )
      return S_OK()
    nextOp = opChain[ opIndex + 1 ]
    return jobState.setState( "Checking", nextOp )


  def deserializeTask( self, taskStub ):
    return CachedJobState.deserialize( taskStub )

  def serializeTask( self, cjs ):
    return S_OK( cjs.serialize() )
