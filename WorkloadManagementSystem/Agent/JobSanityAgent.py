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

from DIRAC.WorkloadManagementSystem.Agent.Base.OptimizerExecutor  import OptimizerExecutor
from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient   import SandboxStoreClient
import re

class JobSanityAgent( OptimizerExecutor ):
  """
      The specific Optimizer must provide the following methods:
      - checkJob() - the main method called for each job
      and it can provide:
      - initializeOptimizer() before each execution cycle      
  """

  #############################################################################
  def initializeOptimizer( self ):
    """Initialize specific parameters for JobSanityAgent.
    """
    self.sandboxClient = SandboxStoreClient( useCertificates = True )
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
    if self.am_getOption( 'InputDataCheck', True ):
      voName = manifest.getOption( "VirtualOrganization", "" )
      if not voName:
        return S_ERROR( "No VirtualOrganization defined in manifest" )
      result = self.checkInputData( jobState, jobType, voName )
      if not result[ 'OK' ]:
        return result
      finalMsg.append( "%s LFNs" % result[ 'Value' ] )
      self.log.info( "Job %s: %s LFNs" % ( jid, result[ 'Value' ] ) )

    #Platform check # disabled
    if self.am_getOption( 'PlatformCheck', False ):
      result = self.checkPlatformSupported( jobState )
      if not result[ 'OK' ]:
        return result
      finalMsg.append( "Platform OK" )
      self.log.info( "Job %s: Platform OK" % jid )

    #Output data exists check # disabled
    if self.am_getOption( 'OutputDataCheck', False ):
      if jobType != 'user':
        result = self.checkOutputDataExists( jobState )
        if not result[ 'OK' ]:
          return result
        finalMsg.append( "Output data OK" )
        self.log.info( "Job %s: Output data OK" % jid )

    #Input Sandbox uploaded check
    if self.am_getOption( 'InputSandboxCheck', True ):
      result = self.checkInputSandbox( jobState, manifest )
      if not result[ 'OK' ]:
        return result
      finalMsg.append( "Assigned %s ISBs" % result[ 'Value' ] )
      self.log.info( "Job %s: Assigned %s ISBs" % ( jid, result[ 'Value' ] ) )

    jobState.setParameter( 'JobSanityCheck', " | ".join( finalMsg ) )
    return S_OK()

  #############################################################################
  def checkInputData( self, jobState, jobType, voName ):
    """This method checks both the amount of input
       datasets for the job and whether the LFN conventions
       are correct.
    """

    result = jobState.getInputData()
    if not result[ 'OK' ]:
      self.log.warn( 'Failed to get input data from JobDB for %s' % ( jobState.jid ) )
      self.log.warn( result['Message'] )
      return S_ERROR( "Input Data Specification" )

    data = result[ 'Value' ] # seems to be [''] when null, which isn't an empty list ;)
    data = [ lfn.strip() for lfn in data if lfn.strip() ]
    if not data:
      return S_OK( 0 )

    self.log.debug( 'Job %s has an input data requirement and will be checked' % ( jobState.jid ) )
    self.log.debug( 'Data is:\n\t%s' % "\n\t".join( data ) )

    voRE = re.compile( "^(LFN:)?/%s/" % voName )

    for lfn in data:
      if not voRE.match( lfn ):
        return S_ERROR( "Input data not correctly specified" )
      if lfn.find( "//" ) > -1:
        return S_ERROR( "Input data contains //" )

    #only check limit for user jobs
    if jobType == 'user':
      maxLFNs = self.am_getOption( 'MaxInputDataPerJob', 100 )
      if len( data ) > maxLFNs:
        message = '%s datasets selected. Max limit is %s.' % ( len( data ), maxLFNs )
        jobState.setParam( "DatasetCheck", message )
        return S_ERROR( "Exceeded Maximum Dataset Limit (%s)" % maxLFNs )

    return S_OK( len( data ) )

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
        self.log.info( "Found a sandbox", isb )
        sbsToAssign.append( ( isb, "Input" ) )
    numSBsToAssign = len( sbsToAssign )
    if not numSBsToAssign:
      return S_OK( 0 )
    self.log.info( "Assigning %s sandboxes on behalf of %s@%s" % ( numSBsToAssign, ownerName, ownerGroup ) )
    result = self.sandboxClient.assignSandboxesToJob( jobState.jid, sbsToAssign, ownerName, ownerGroup, jobSetup )
    if not result[ 'OK' ]:
      self.log.error( "Could not assign sandboxes in the SandboxStore", "assigned to job %s" % jobState.jid )
      return S_ERROR( "Cannot assign sandbox to job" )
    assigned = result[ 'Value' ]
    if assigned != numSBsToAssign:
      self.log.error( "Could not assign all sandboxes (%s). Only assigned %s" % ( numSBsToAssign, assigned ) )
    return S_OK( numSBsToAssign )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
