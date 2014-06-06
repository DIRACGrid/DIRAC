########################################################################
# $HeadURL$
########################################################################
"""
  The Job Sanity Agent accepts all jobs from the Job
  receiver and screens them for the following problems:
   - Output data already exists
   - Problematic JDL
   - Jobs with too much input data e.g. > 100 files
   - Jobs with input data incorrectly specified e.g. castor:/
   - Input sandbox not correctly uploaded.
"""

__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor  import OptimizerExecutor
from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers import Resources
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient   import SandboxStoreClient
import re

class JobSanity( OptimizerExecutor ):
  """
      The specific Optimizer must provide the following methods:
      - checkJob() - the main method called for each job
      and it can provide:
      - initializeOptimizer() before each execution cycle
  """

  @classmethod
  def initializeOptimizer( cls ):
    """Initialize specific parameters for JobSanityAgent.
    """
    cls.sandboxClient = SandboxStoreClient( useCertificates = True )
    cls.ex_setOption( "FailedStatus", "Insane job" )
    return S_OK()

  #############################################################################
  def optimizeJob( self, jid, jobState ):
    """ This method controls the order and presence of
        each sanity check for submitted jobs. This should
        be easily extended in the future to accommodate
        any other potential checks.
    """
    #Job JDL check
    result = jobState.getAttribute( 'JobType' )
    if not result['OK']:
      return result
    jobType = result['Value'].lower()

    result = jobState.getManifest()
    if not result[ 'OK' ]:
      return result
    manifest = result[ 'Value' ]

    finalMsg = []

    #Input data check
    if self.ex_getOption( 'InputDataCheck', True ):
      voName = manifest.getOption( "VirtualOrganization", "" )
      if not voName:
        return S_ERROR( "No VirtualOrganization defined in manifest" )
      result = self.checkInputData( manifest, jobType, voName )
      if not result[ 'OK' ]:
        return result
      finalMsg.append( "%s LFNs" % result[ 'Value' ] )
      self.jobLog.info( "%s LFNs" % result[ 'Value' ] )

    #Platform check # disabled
    if self.ex_getOption( 'PlatformCheck', False ):
      result = self.checkPlatformSupported( jobState )
      if not result[ 'OK' ]:
        return result
      finalMsg.append( "Platform OK" )
      self.jobLog.info( "Platform OK" )

    #Output data exists check # disabled
    if self.ex_getOption( 'OutputDataCheck', False ):
      if jobType != 'user':
        result = self.checkOutputDataExists( jobState )
        if not result[ 'OK' ]:
          return result
        finalMsg.append( "Output data OK" )
        self.jobLog.info( "Output data OK" )

    #Input Sandbox uploaded check
    if self.ex_getOption( 'InputSandboxCheck', True ):
      result = self.checkInputSandbox( jobState, manifest )
      if not result[ 'OK' ]:
        return result
      finalMsg.append( "Assigned %s ISBs" % result[ 'Value' ] )
      self.jobLog.info( "Assigned %s ISBs" % result[ 'Value' ] )

    #Check site
    if self.ex_getOption( 'RequirementsCheck', True ):
      result = self.checkRequirementsSanity( jobState, manifest )
      if not result[ 'OK' ]:
        return result
      finalMsg.append( "Requirements OK" )
      self.jobLog.info( "Requirements OK" )

    jobState.setParameter( 'JobSanityCheck', " | ".join( finalMsg ) )
    return self.setNextOptimizer( jobState )

  #############################################################################
  def checkInputData( self, manifest, jobType, voName ):
    """This method checks both the amount of input
       datasets for the job and whether the LFN conventions
       are correct.
    """
    inputData = manifest.getOption( "InputData", [] )
    if not inputData:
      return S_OK( 0 )

    self.jobLog.debug( 'Input data requirement will be checked' )
    self.jobLog.debug( 'Data is:\n\t%s' % "\n\t".join( inputData ) )

    voRE = re.compile( "^(LFN:)?/%s/" % voName )

    for lfn in inputData:
      if not voRE.match( lfn ):
        return S_ERROR( "Input data not correctly specified" )
      #if lfn.find( "//" ) > -1:
      #  return S_ERROR( "Input data contains //" )

    #only check limit for user jobs
    if jobType == 'user':
      maxLFNs = self.ex_getOption( 'MaxInputDataPerJob', 10000 )
      if len( inputData ) > maxLFNs:
        message = '%s datasets selected. Max limit is %s.' % ( len( data ), maxLFNs )
        jobState.setParameter( "DatasetCheck", message )
        return S_ERROR( "Exceeded Maximum Dataset Limit (%s)" % maxLFNs )

    return S_OK( len( inputData ) )

  #############################################################################
  def  checkOutputDataExists( self, jobState ):
    """If the job output data is already in the LFC, this
       method will fail the job for the attention of the
       data manager. To be tidied for DIRAC3...
    """
    # FIXME: To implement checkOutputDataExists
    return S_OK()

  #############################################################################
  def checkPlatformSupported( self, jobState ):
    """This method queries the CS for available platforms
       supported by DIRAC and will check these against what
       the job requests.
    """
    # FIXME: To implement checkPlatformSupported
    return S_OK()

  #############################################################################
  def checkInputSandbox( self, jobState, manifest ):
    """The number of input sandbox files, as specified in the job
       JDL are checked in the JobDB.
    """
    result = jobState.getAttributes( [ 'Owner', 'OwnerDN', 'OwnerGroup', 'DIRACSetup' ] )
    if not result[ 'OK' ]:
      return result
    attDict = result[ 'Value' ]
    ownerName = attDict[ 'Owner' ]
    if not ownerName:
      ownerDN = attDict[ 'OwnerDN' ]
      if not ownerDN:
        return S_ERROR( "Missing OwnerDN" )
      result = Registry.getUsernameForDN( ownerDN )
      if not result[ 'OK' ]:
        return result
      ownerName = result[ 'Value' ]
    ownerGroup = attDict[ 'OwnerGroup' ]
    if not ownerGroup:
      return S_ERROR( "Missing OwnerGroup" )
    jobSetup = attDict[ 'DIRACSetup' ]
    if not jobSetup:
      return S_ERROR( "Missing DIRACSetup" )

    isbList = manifest.getOption( 'InputSandbox', [] )
    sbsToAssign = []
    for isb in isbList:
      if isb.find( "SB:" ) == 0:
        self.jobLog.info( "Found a sandbox", isb )
        sbsToAssign.append( ( isb, "Input" ) )
    numSBsToAssign = len( sbsToAssign )
    if not numSBsToAssign:
      return S_OK( 0 )
    self.jobLog.info( "Assigning %s sandboxes on behalf of %s@%s" % ( numSBsToAssign, ownerName, ownerGroup ) )
    result = self.sandboxClient.assignSandboxesToJob( jobState.jid, sbsToAssign, ownerName, ownerGroup, jobSetup )
    if not result[ 'OK' ]:
      self.jobLog.error( "Could not assign sandboxes in the SandboxStore" )
      return S_ERROR( "Cannot assign sandbox to job" )
    assigned = result[ 'Value' ]
    if assigned != numSBsToAssign:
      self.jobLog.error( "Could not assign all sandboxes (%s). Only assigned %s" % ( numSBsToAssign, assigned ) )
    return S_OK( numSBsToAssign )

  def checkRequirementsSanity( self, jobState, manifest ):
    #Check if sites exist
    sites = manifest.getOption( "Site", [] )
    if sites:
      allSites = Resources.getSites()
      for site in sites:
        if site not in allSites:
          return S_ERROR( "Unknown site %s" % site )
    ses = manifest.getOption( "TargetSE", [] )
    if ses:
      resources = Resources.Resources( vo = manifest.getOption( "VirtualOrganization", "" ) )
      result = resources.getEligibleResources('Storage')
      if not result[ 'OK' ]:
        #TODO: Raise excp to retry job?
        return S_ERROR( "Can't retrieve SEs" )
      validSEs = result[ 'Value' ]
      for se in ses:
        if se not in validSEs:
          return S_ERROR( "Unknown Target SE %s" % se )

    return S_OK()


