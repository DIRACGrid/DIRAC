########################################################################
# $HeadURL$
# File :    JobPathAgent.py
# Author :  Stuart Paterson
########################################################################
""" 
  The Job Path Agent determines the chain of Optimizing Agents that must
  work on the job prior to the scheduling decision.

  Initially this takes jobs in the received state and starts the jobs on the
  optimizer chain.  The next development will be to explicitly specify the
  path through the optimizers.

"""
__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.Agent.OptimizerModule  import OptimizerModule
from DIRAC.Core.Utilities.ModuleFactory                    import ModuleFactory
from DIRAC.Core.Utilities                                  import List 
from DIRAC.WorkloadManagementSystem.Client.JobDescription  import JobDescription
from DIRAC                                                 import S_OK, S_ERROR

OPTIMIZER_NAME = 'JobPath'

class JobPathAgent( OptimizerModule ):
  """
      The specific Optimizer must provide the following methods:
      - checkJob() - the main method called for each job
      and it can provide:
      - initializeOptimizer() before each execution cycle      
  """

  #############################################################################
  def initializeOptimizer( self ):
    """Initialize specific parameters for JobPathAgent.
    """
    self.startingMajorStatus = "Received"
    self.startingMinorStatus = False
    #self.requiredJobInfo = "jdlOriginal"
    return S_OK()

  def beginExecution( self ):
    """Called before each Agent execution cycle
    """
    self.basePath = self.am_getOption( 'BasePath', ['JobPath', 'JobSanity'] )
    self.inputData = self.am_getOption( 'InputData', ['InputData'] )
    self.endPath = self.am_getOption( 'EndPath', ['JobScheduling', 'TaskQueue'] )
    self.voPlugin = self.am_getOption( 'VOPlugin', '' )

    return S_OK()

  def __syncJobDesc( self, jobId, jobDesc, classAdJob ):
    """ ???
    """
    if not jobDesc.isDirty():
      return
    for op in jobDesc.getOptions():
      classAdJob.insertAttributeString( op, jobDesc.getVar( op ) )
    self.jobDB.setJobJDL( jobId, jobDesc.dumpDescriptionAsJDL() )

  #############################################################################
  def checkJob( self, job, classAdJob ):
    """This method controls the checking of the job.
    """
    jobDesc = JobDescription()
    result = jobDesc.loadDescription( classAdJob.asJDL() )
    if not result[ 'OK' ]:
      self.setFailedJob( job, result['Message'], classAdJob )
      return result
    self.__syncJobDesc( job, jobDesc, classAdJob )

    #Check if job defines a path itself
    # FIXME: only some group might be able to overwrite the jobPath
    jobPath = classAdJob.get_expression( 'JobPath' ).replace( '"', '' ).replace( 'Unknown', '' )
    #jobPath = jobDesc.getVarWithDefault( 'JobPath' ).replace( 'Unknown', '' )
    if jobPath:
      # HACK: Remove the { and } to ensure we have a simple string
      jobPath = jobPath.replace( "{", "" ).replace( "}", "" )
      self.log.info( 'Job %s defines its own optimizer chain %s' % ( job, jobPath ) )
      return self.processJob( job, List.fromChar( jobPath ) )

    #If no path, construct based on JDL and VO path module if present
    path = list( self.basePath )
    if self.voPlugin:
      argumentsDict = {'JobID':job, 'ClassAd':classAdJob, 'ConfigPath':self.am_getModuleParam( "section" )}
      moduleFactory = ModuleFactory()
      moduleInstance = moduleFactory.getModule( self.voPlugin, argumentsDict )
      if not moduleInstance['OK']:
        self.log.error( 'Could not instantiate module:', '%s' % ( self.voPlugin ) )
        self.setFailedJob( job, 'Could not instantiate module: %s' % ( self.voPlugin ), classAdJob )
        return S_ERROR( 'Holding pending jobs' )

      module = moduleInstance['Value']
      result = module.execute()
      if not result['OK']:
        self.log.warn( 'Execution of %s failed' % ( self.voPlugin ) )
        return result
      extraPath = List.fromChar( result['Value'] )
      if extraPath:
        path.extend( extraPath )
        self.log.verbose( 'Adding extra VO specific optimizers to path: %s' % ( extraPath ) )
    else:
      self.log.verbose( 'No VO specific plugin module specified' )
      #Should only rely on an input data setting in absence of VO plugin
      result = self.jobDB.getInputData( job )
      if not result['OK']:
        self.log.error( 'Failed to get input data from JobDB', job )
        self.log.warn( result['Message'] )
        return result

      if result['Value']:
        # if the returned tuple is not empty it will evaluate true
        self.log.info( 'Job %s has an input data requirement' % ( job ) )
        path.extend( self.inputData )
      else:
        self.log.info( 'Job %s has no input data requirement' % ( job ) )

    path.extend( self.endPath )
    self.log.info( 'Constructed path for job %s is: %s' % ( job, path ) )
    return self.processJob( job, path )

  #############################################################################
  def processJob( self, job, chain ):
    """Set job path and send to next optimizer
    """
    result = self.setOptimizerChain( job, chain )
    if not result['OK']:
      self.log.warn( result['Message'] )
    result = self.setJobParam( job, 'JobPath', ','.join( chain ) )
    if not result['OK']:
      self.log.warn( result['Message'] )
    return self.setNextOptimizer( job )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
