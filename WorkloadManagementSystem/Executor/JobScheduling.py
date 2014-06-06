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
from DIRAC.ResourceStatusSystem.Client.SiteStatus                   import SiteStatus


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
    siteStatus = SiteStatus()
    result = siteStatus.getUsableSites( 'ComputingAccess' )
    if not result[ 'OK' ]:
      return S_ERROR( "Cannot retrieve active sites from JobDB" )
    usableSites = result[ 'Value' ]
    result = siteStatus.getUnusableSites( 'ComputingAccess' )
    if not result[ 'OK' ]:
      return S_ERROR( "Cannot retrieve banned sites from JobDB" )
    unusableSites = result[ 'Value' ]

    # If the user has selected any site, filter them and hold the job if not able to run
    if userSites:
      result = jobState.getAttribute( "JobType" )
      if not result[ 'OK' ]:
        return S_ERROR( "Could not retrieve job type" )
      jobType = result[ 'Value' ]
      if jobType not in self.ex_getOption( 'ExcludedOnHoldJobTypes', [] ):
        sites = userSites.intersection( usableSites ).difference( unusableSites )
        if not sites:
          if len( userSites ) > 1:
            raise OptimizerExecutor.FreezeTask( "Requested sites %s are inactive" % ", ".join( userSites ) )
          else:
            raise OptimizerExecutor.FreezeTask( "Requested site %s is inactive" % userSites[0] )
          

    #Get the Input data
    # Third, check if there is input data
    result = jobState.getInputData()
    if not result['OK']:
      self.jobLog.error( "Cannot get input data %s" % ( result['Message'] ) )
      return S_ERROR( 'Failed to get input data from JobDB' )

    if not result['Value']:
      #No input data? Generate requirements and next
      return self.__sendToTQ( jobState, userSites, userBannedSites )

    inputData = result[ 'Value' ]

    self.jobLog.verbose( 'Has an input data requirement' )
    idAgent = self.ex_getOption( 'InputDataAgent', 'InputData' )
    result = self.retrieveOptimizerParam( idAgent )
    if not result['OK']:
      self.jobLog.error( "Could not retrieve input data info: %s" % result[ 'Message' ] )
      return S_ERROR( "File Catalog Access Failure" )
    opData = result[ 'Value' ]
    if 'SiteCandidates' not in opData:
      return S_ERROR( "No possible site candidates" )

    #Filter input data sites with user requirement
    siteCandidates = list( opData[ 'SiteCandidates' ] )
    self.jobLog.info( "Site candidates are %s" % siteCandidates )

    siteCandidates = self.__applySiteFilter( siteCandidates, userSites, userBannedSites )
    if not siteCandidates:
      return S_ERROR( "Impossible InputData * Site requirements" )

    idSites = {}
    for site in siteCandidates:
      idSites[ site ] = opData[ 'SiteCandidates' ][ site ]

    #Check if sites have correct count of disk+tape replicas
    numData = len( inputData )
    errorSites = set()
    for site in idSites:
      if numData != idSites[ site ][ 'disk' ] + idSites[ site ][ 'tape' ]:
        self.jobLog.error( "Site candidate %s does not have all the input data" % site )
        errorSites.add( site )
    for site in errorSites:
      idSites.pop( site )
    if not idSites:
      return S_ERROR( "Site candidates do not have all the input data" )

    #Check if staging is required
    stageRequired, siteCandidates = self.__resolveStaging( jobState, inputData, idSites )
    if not siteCandidates:
      return S_ERROR( "No destination sites available" )

    #Is any site active?
    stageSites = self.__applySiteFilter( siteCandidates, wmsActiveSites, wmsBannedSites )
    if not stageSites:
      return self.__holdJob( jobState, "Sites %s are inactive or banned" % ", ".join( siteCandidates ) )

    #If no staging is required send to TQ
    if not stageRequired:
      #Use siteCandidates and not stageSites because active and banned sites
      #will be taken into account on matching time
      return self.__sendToTQ( jobState, siteCandidates, userBannedSites )

    #Check if the user is allowed to stage
    if self.ex_getOption( "RestrictDataStage", False ):
      if not self.__checkStageAllowed( jobState ):
        return S_ERROR( "Stage not allowed" )

    #Get stageSites[0] because it has already been randomized and it's as good as any in stageSites
    stageSite = stageSites[0]
    self.jobLog.verbose( " Staging site will be %s" % ( stageSite ) )
    stageData = idSites[ stageSite ]
    #Set as if everything has already been staged
    stageData[ 'disk' ] += stageData[ 'tape' ]
    stageData[ 'tape' ] = 0
    #Set the site info back to the original dict to save afterwards
    opData[ 'SiteCandidates' ][ stageSite ] = stageData

    result = self.__requestStaging( jobState, stageSite, opData )
    if not result[ 'OK' ]:
      return result
    stageLFNs = result[ 'Value' ]
    self.__updateSharedSESites( jobState, stageSite, stageLFNs, opData )
    #Save the optimizer data again
    self.jobLog.verbose( 'Updating %s Optimizer Info:' % ( idAgent ), opData )
    result = self.storeOptimizerParam( idAgent, opData )
    if not result[ 'OK' ]:
      return result

    return self.__setJobSite( jobState, stageSites )

  def __applySiteFilter( self, sites, active = False, banned = False ):
    filtered = list( sites )
    if active:
      for site in sites:
        if site not in active:
          filtered.remove( site )
    if banned:
      for site in banned:
        if site in filtered:
          filtered.remove( site )
    return filtered

  def __holdJob( self, jobState, holdMsg, delay = 0 ):
    if delay:
      self.freezeTask( delay )
    else:
      self.freezeTask( self.ex_getOption( "HoldTime", 300 ) )
    jid = jobState.jid
    self.jobLog.info( "On hold -> %s" % holdMsg )
    return jobState.setAppStatus( holdMsg, source = self.ex_optimizerName() )

  def __getSitesRequired( self, jobState ):
    """Returns any candidate sites specified by the job or sites that have been
       banned and could affect the scheduling decision.
    """
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
                 'CPUTime', 'Tags' ):
      if key == "JobType":
        reqKey = "JobTypes"
      elif key == "GridRequiredCEs" or key == "GridCE":
        reqKey = "GridCEs"
      elif key == 'Priority':
        reqkey = 'UserPriority'
      elif key == "DIRACSetup":
        reqKey = 'Setup'
      elif key == "SubmitPools" or key == "SubmitPool":
        reqKey = "SubmitPools"    
      elif key == "PilotTypes" or key == "PilotType":
        reqKey = "PilotTypes"    
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
    return self.setNextOptimizer( jobState )

  def __resolveStaging( self, jobState, inputData, idSites ):
    diskSites = []
    maxOnDisk = 0
    bestSites = []

    for site in idSites:
      nTape = idSites[ site ][ 'tape' ]
      nDisk = idSites[ site ][ 'disk' ]
      if nTape > 0:
        self.jobLog.verbose( "%s tape replicas on site %s" % ( nTape, site ) )
      if nDisk > 0:
        self.jobLog.verbose( "%s disk replicas on site %s" % ( nDisk, site ) )
        if nDisk == len( inputData ):
          diskSites.append( site )
      if nDisk > maxOnDisk:
        maxOnDisk = nDisk
        bestSites = [ site ]
      elif nDisk == maxOnDisk:
       bestSites.append( site )

    #If there are selected sites, those are disk only sites
    if diskSites:
      self.jobLog.info( "No staging required" )
      return ( False, diskSites )

    self.jobLog.info( "Staging required" )
    if len( bestSites ) > 1:
      random.shuffle( bestSites )
    return ( True, bestSites )

  def __requestStaging( self, jobState, stageSite, opData ):
    result = getSEsForSite( stageSite )
    if not result['OK']:
      return S_ERROR( 'Could not determine SEs for site %s' % stageSite )
    siteSEs = result['Value']

    tapeSEs = []
    diskSEs = []
    for seName in siteSEs:
      se = StorageElement( seName )
      result = se.getStatus()
      if not result[ 'OK' ]:
        self.jobLog.error( "Cannot retrieve SE %s status: %s" % ( seName, result[ 'Message' ] ) )
        return S_ERROR( "Cannot retrieve SE status" )
      seStatus = result[ 'Value' ]
      if seStatus[ 'Read' ] and seStatus[ 'TapeSE' ]:
        tapeSEs.append( seName )
      if seStatus[ 'Read' ] and seStatus[ 'DiskSE' ]:
        diskSEs.append( seName )

    if not tapeSEs:
      return S_ERROR( "No Local SEs for site %s" % stageSite )

    self.jobLog.verbose( "Tape SEs are %s" % ( ", ".join( tapeSEs ) ) )

    #I swear this is horrible DM code it's not mine.
    #Eternity of hell to the inventor of the Value of Value of Success of...
    inputData = opData['Value']['Value']['Successful']
    stageLFNs = {}
    lfnToStage = []
    for lfn in inputData:
      replicas = inputData[ lfn ]
      #Check SEs
      seStage = []
      for seName in replicas:
        surl = replicas[ seName ]
        if seName in diskSEs:
          #This lfn is in disk. Skip it
          seStage = []
          break
        if seName not in tapeSEs:
          #This lfn is not in this tape SE. Check next SE
          continue
        seStage.append( seName )
      for seName in seStage:
        if seName not in stageLFNs:
          stageLFNs[ seName ] = []
        stageLFNs[ seName ].append( lfn )
        if lfn not in lfnToStage:
          lfnToStage.append( lfn )

    if not stageLFNs:
      return S_ERROR( "Cannot find tape replicas" )

    #Check if any LFN is in more than one SE
    #If that's the case, try to stage from the SE that has more LFNs to stage to group the request
    #1.- Get the SEs ordered by ascending replicas
    sortedSEs = reversed( sorted( [ ( len( stageLFNs[ seName ] ), seName ) for seName in stageLFNs.keys() ] ) )
    for lfn in lfnToStage:
      found = False
      #2.- Traverse the SEs
      for stageCount, seName in sortedSEs:
        if lfn in stageLFNs[ seName ]:
          #3.- If first time found, just mark as found. Next time delete the replica from the request
          if found:
            stageLFNs[ seName ].remove( lfn )
          else:
            found = True
        #4.-If empty SE, remove
        if len( stageLFNs[ seName ] ) == 0:
          stageLFNs.pop( seName )

    self.jobLog.verbose( "Stage request will be \n\t%s" % "\n\t".join( [ "%s:%s" % ( lfn, stageLFNs[ lfn ] ) for lfn in stageLFNs ] ) )

    stagerClient = StorageManagerClient()
    result = jobState.setStatus( self.ex_getOption( 'StagingStatus', 'Staging' ),
                                 self.ex_getOption( 'StagingMinorStatus', 'Request To Be Sent' ),
                                 appStatus = "",
                                 source = self.ex_optimizerName() )
    if not result[ 'OK' ]:
      return result

    result = stagerClient.setRequest( stageLFNs, 'WorkloadManagement',
                                      'updateJobFromStager@WorkloadManagement/JobStateUpdate',
                                      int( jobState.jid ) )
    if not result[ 'OK' ]:
      self.jobLog.error( "Could not send stage request: %s" %  result[ 'Message' ] )
      return S_ERROR( "Problem sending staging request" )

    rid = str( result[ 'Value' ] )
    self.jobLog.info( "Stage request %s sent" % rid )
    jobState.setParameter( "StageRequest", rid )

    result = jobState.setStatus( self.ex_getOption( 'StagingStatus', 'Staging' ),
                                 self.ex_getOption( 'StagingMinorStatus', 'Request Sent' ),
                                 appStatus = "",
                                 source = self.ex_optimizerName() )
    if not result[ 'OK' ]:
      return result

    return S_OK( stageLFNs )


  def __updateSharedSESites( self, jobState, stageSite, stagedLFNs, opData ):
    siteCandidates = opData[ 'SiteCandidates' ]

    seStatus = {}
    for siteName in siteCandidates:
      if siteName == stageSite:
        continue
      self.jobLog.verbose( "Checking %s for shared SEs" % siteName )
      siteData = siteCandidates[ siteName ]
      result = getSEsForSite( siteName )
      if not result[ 'OK' ]:
        continue
      closeSEs = result[ 'Value' ]
      diskSEs = []
      for seName in closeSEs:
        #If we don't have the SE status get it and store it
        if seName not in seStatus:
          seObj = StorageElement( seName )
          result = seObj.getStatus()
          if not result['OK' ]:
            self.jobLog.error( "Cannot retrieve SE %s status: %s" % ( seName, result[ 'Message' ] ) )
            continue
          seStatus[ seName ] = result[ 'Value' ]
        #get the SE status from mem and add it if its disk
        status = seStatus[ seName ]
        if status['Read'] and status['DiskSE']:
          diskSEs.append( seName )
      self.jobLog.verbose( "Disk SEs for %s are %s" % ( siteName, ", ".join( diskSEs ) ) )

      #Hell again to the dev of this crappy value of value of successful of ...
      lfnData = opData['Value']['Value']['Successful']
      for seName in stagedLFNs:
        #If the SE is not close then skip it
        if seName not in closeSEs:
          continue
        for lfn in stagedLFNs[ seName ]:
          self.jobLog.verbose( "Checking %s for %s" % ( seName, lfn ) )
          #I'm pretty sure that this cannot happen :P
          if lfn not in lfnData:
            continue
          #Check if it's already on disk at the site
          onDisk = False
          for siteSE in lfnData[ lfn ]:
            if siteSE in diskSEs:
              self.jobLog.verbose( "%s on disk for %s" % ( lfn, siteSE ) )
              onDisk = True
          #If not on disk, then update!
          if not onDisk:
            self.jobLog.verbose( "Setting LFN to disk for %s" % ( seName ) )
            siteData[ 'disk' ] += 1
            siteData[ 'tape' ] -= 1

    return S_OK()

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



