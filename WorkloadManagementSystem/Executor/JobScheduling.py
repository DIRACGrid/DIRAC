"""   The Job Scheduling Agent takes the information gained from all previous
      optimizers and makes a scheduling decision for the jobs.  Subsequent to this
      jobs are added into a Task Queue by the next optimizer and pilot agents can
      be submitted.

      All issues preventing the successful resolution of a site candidate are discovered
      here where all information is available.  This Agent will fail affected jobs
      meaningfully.

"""

import types
import random

from DIRAC                                                          import S_OK, S_ERROR
from DIRAC.Core.Utilities.SiteSEMapping                             import getSEsForSite
from DIRAC.Core.Utilities.Time                                      import fromString, toEpoch
from DIRAC.Core.Utilities                                           import List
from DIRAC.Core.Security                                            import Properties
from DIRAC.ConfigurationSystem.Client.Helpers.Resources             import getSiteTier
from DIRAC.ConfigurationSystem.Client.Helpers                       import Registry
from DIRAC.Resources.Storage.StorageElement                         import StorageElement
from DIRAC.StorageManagementSystem.Client.StorageManagerClient      import StorageManagerClient
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor import OptimizerExecutor


class JobScheduling( OptimizerExecutor ):
  """
      The specific Optimizer must provide the following methods:
      - checkJob() - the main method called for each job
      and it can provide:
      - initializeOptimizer() before each execution cycle
  """

  @classmethod
  def initializeOptimizer( cls ):
    """ Initialization of the Agent.
    """
    try:
      from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
    except ImportError, excp :
      return S_ERROR( "Could not import JobDB: %s" % str( excp ) )

    try:
      cls.__jobDB = JobDB()
    except RuntimeError:
      return S_ERROR( "Cannot connect to JobDB" )

    cls.ex_setOption( "FailedStatus", "Cannot schedule" )
    return S_OK()

  def __checkReschedules( self, jobState ):
    # Reschedule delay
    result = jobState.getAttributes( [ 'RescheduleCounter', 'RescheduleTime', 'ApplicationStatus' ] )
    if not result[ 'OK' ]:
      return result
    attDict = result[ 'Value' ]
    try:
      reschedules = int( attDict[ 'RescheduleCounter' ] )
    except ValueError:
      return S_ERROR( "RescheduleCounter has to be an integer" )
    if reschedules != 0:
      delays = self.ex_getOption( 'RescheduleDelays', [60, 180, 300, 600] )
      delay = delays[ min( reschedules, len( delays ) - 1 ) ]
      reschTime = attDict[ 'RescheduleTime' ]
      if type( reschTime ) == types.StringType:
        reschTime = fromString( reschTime )
      waited = toEpoch() - toEpoch( reschTime )
      if waited < delay:
        raise OptimizerExecutor.FreezeTask( 'On Hold: after rescheduling %s' % reschedules, delay )
    return result

  def _getSitesRequired( self, jobState ):
    """Returns any candidate sites specified by the job or sites that have been
       banned and could affect the scheduling decision.
    """

    result = jobState.getManifest()
    if not result[ 'OK' ]:
      return S_ERROR( "Could not retrieve manifest: %s" % result[ 'Message' ] )
    manifest = result[ 'Value' ]

    banned = set( manifest.getOption( "BannedSites", [] ) )
    if banned:
      self.jobLog.info( "Banned %s sites" % ", ".join( banned ) )

    sites = manifest.getOption( "Site", [] )
    sites = set( [ site for site in sites if site.strip().lower() not in ( "any", "" ) ] )

    if len( sites ) == 1:
      self.jobLog.info( 'Single chosen site %s specified' % ( list(sites)[0] ) )

    if sites and banned:
      sites = set.difference( banned )
      if not sites:
        return S_ERROR( "Impossible site requirement" )

    return S_OK( ( sites, banned ) )

  def optimizeJob( self, jid, jobState ):
    result = self.__checkReschedules( jobState )
    if not result[ 'OK' ]:
      return result
    # Get site requirements
    result = self._getSitesRequired( jobState )
    if not result[ 'OK' ]:
      return result
    userSites, userBannedSites = result[ 'Value' ]

    # Get active and banned sites from DIRAC
    result = self.__jobDB.getSiteMask( 'Active' )
    if not result[ 'OK' ]:
      return S_ERROR( "Cannot retrieve active sites from JobDB" )
    wmsActiveSites = set( result[ 'Value' ] )
    result = self.__jobDB.getSiteMask( 'Banned' )
    if not result[ 'OK' ]:
      return S_ERROR( "Cannot retrieve banned sites from JobDB" )
    wmsBannedSites = set( result[ 'Value' ] )

    # If the user has selected any site, filter them and hold the job if not able to run
    if userSites:
      result = jobState.getAttribute( "JobType" )
      if not result[ 'OK' ]:
        return S_ERROR( "Could not retrieve job type" )
      jobType = result[ 'Value' ]
      if jobType not in self.ex_getOption( 'ExcludedOnHoldJobTypes', [] ):
        sites = userSites.intersection( wmsActiveSites ).difference( wmsBannedSites )
        if not sites:
          raise OptimizerExecutor.FreezeTask( "Sites %s are inactive or banned" % ", ".join( userSites ) )

    result = jobState.getOptParameters()
    if not result[ 'OK' ]:
      self.jobLog.error( "Can't retrieve optimizer parameters: %s" % result[ 'Value' ] )
      return S_ERROR( "Cant retrieve optimizer parameters" )

    dataSites = result[ 'Value' ].get( "DataSites", "" )
    if dataSites:
      dataSites = set( List.fromChar( dataSites ) )
      if userSites:
        userSites.intersection( dataSites )
      else:
        userSites = dataSites

    #HERE

    #Fill in the JobRequirements section
    result = jobState.getManifest()
    if not result[ 'OK' ]:
      return S_ERROR( "Could not retrieve manifest: %s" % result[ 'Message' ] )
    manifest = result[ 'Value' ]

    reqSection = "JobRequirements"

    if reqSection in manifest:
      result = manifest.getSection( reqSection )
    else:
      result = manifest.createSection( reqSection )
    if not result[ 'OK' ]:
      self.jobLog.error( "Cannot create %s: %s" % ( reqSection, result[ 'Message' ] ) )
      return S_ERROR( "Cannot create %s in the manifest" % reqSection )
    reqCfg = result[ 'Value' ]

    targetSEs = manifest.getOption( "TargetSEs", "" )
    if targetSEs:
      reqCfg.setOption( "SEs", targetSEs )
    elif userSites:
      reqCfg.setOption( "Sites", ", ".join( userSites ) )
    if userBannedSites:
      reqCfg.setOption( "BannedSites", ", ".join( userBannedSites ) )

    for key in ( 'SubmitPools', "GridMiddleware", "PilotTypes", "JobType", "GridRequiredCEs",
                 "OwnerDN", "OwnerGroup", "VirtualOrganization", 'Priority', 'DIRACSetup',
                 'CPUTime' ):
      if key == "JobType":
        reqKey = "JobTypes"
      elif key == "GridRequiredCEs":
        reqKey = "GridCEs"
      elif key == 'Priority':
        reqkey = 'UserPriority'
      elif key == "DIRACSetup":
        reqKey = 'Setup'
      else:
        reqKey = key

      if key in manifest:
        reqCfg.setOption( reqKey, manifest.getOption( key, "" ) )

    #Platform
    userPlatform = manifest.getOption( "Platform" )
    if userPlatform and userPlatform != 'any':
      preqs = [ userPlatform ]
      result = gConfig.getOptionsDict( "/Resources/Computing/OSCompatibility" )
      if result[ 'OK' ]:
        compatDict = result[ 'Value' ]
        for compatPlatform in compatDict:
          if compatPlatform != userPlatform:
            if userPlatform in List.fromChar( compatDict[ compatPlatform ] ):
              preqs.append( compatPlatform )
      reqCfg.setOption( "Platforms", ", ".join( preqs ) )

    #TODO: FIX THIS CRAP!
    self._setJobSite( jobState, userSites )

    result = jobState.setStatus( self.ex_getOption( 'WaitingStatus', 'Waiting' ),
                                 minorStatus = self.ex_getOption( 'WaitingMinorStatus',
                                                                  'Pilot Agent Submission' ),
                                 appStatus = "Unknown",
                                 source = self.ex_optimizerName() )
    if not result[ 'OK' ]:
      return result

    self.jobLog.info( "Done" )
    return self.sendJobToTQ()

  def _setJobSite( self, jobState, siteList ):
    """ Set the site attribute
    """
    site = self._getJobSite( siteList )
    return jobState.setAttribute( "Site", site )

  def _getJobSite( self, siteList ):
    """ get the Job site
    """
    siteList = list( siteList )
    numSites = len( siteList )
    if numSites == 0 or numSites == 1 and not siteList[0]:
      self.jobLog.info( "Any site is candidate" )
      return "ANY"
    elif numSites == 1:
      self.jobLog.info( "Only site %s is candidate" % siteList[0] )
      return siteList[0]
    else:
      tierSite = []
      siteTierDict = self._getSiteTiers( siteList )
      for site, tier in siteTierDict.iteritems():
        if tier == min( siteTierDict.values() ):
          tierSite.append( site )

      if len( tierSite ) == 1:
        siteName = "Group.%s" % ".".join( tierSite[0].split( "." )[1:] )
        self.jobLog.info( "Group %s is candidate" % siteName )
      else:
        siteName = "Multiple"
        self.jobLog.info( "Multiple sites are candidate" )

      return siteName

  def _getSiteTiers( self, siteList ):
    """ retun dict {'Site':Tier}
    """
    siteTierDict = {}
    for siteName in siteList:
      result = getSiteTier( siteName )
      if not result[ 'OK' ]:
        self.jobLog.error( "Cannot get tier for site %s" % ( siteName ) )
        siteTier = 2
      else:
        siteTier = int( result[ 'Value' ] )
      siteTierDict.setdefault( siteName, siteTier )

    return siteTierDict



