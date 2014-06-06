########################################################################
# $HeadURL: $
# File :   JobSchedulingAgent.py
# Author : Stuart Paterson
########################################################################

"""   The Job Scheduling Agent takes the information gained from all previous
      optimizers and makes a scheduling decision for the jobs.  Subsequent to this
      jobs are added into a Task Queue by the next optimizer and pilot agents can
      be submitted.

      All issues preventing the successful resolution of a site candidate are discovered
      here where all information is available.  This Agent will fail affected jobs
      meaningfully.

"""
__RCSID__ = "$Id: $"

from DIRAC.WorkloadManagementSystem.Agent.OptimizerModule      import OptimizerModule
from DIRAC.Core.Utilities.ClassAd.ClassAdLight                 import ClassAd
from DIRAC.Core.Utilities.SiteSEMapping                        import getSEsForSite
from DIRAC.Core.Utilities.Time                                 import fromString, toEpoch
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient
from DIRAC.Resources.Storage.StorageElement                    import StorageElement
from DIRAC.ConfigurationSystem.Client.Helpers.Resources        import getSiteTier
from DIRAC.Core.Utilities                                      import List  
from DIRAC.ResourceStatusSystem.Client.SiteStatus              import SiteStatus               

from DIRAC                                                     import S_OK, S_ERROR

import random

class JobSchedulingAgent( OptimizerModule ):
  """
      The specific Optimizer must provide the following methods:
      - checkJob() - the main method called for each job
      and it can provide:
      - initializeOptimizer() before each execution cycle
  """

  #############################################################################
  def initializeOptimizer( self ):
    """ Initialization of the Agent.
    """

    self.dataAgentName = self.am_getOption( 'InputDataAgent', 'InputData' )
    self.stagingStatus = self.am_getOption( 'StagingStatus', 'Staging' )
    self.stagingMinorStatus = self.am_getOption( 'StagingMinorStatus', 'Request Sent' )
    delays = self.am_getOption( 'RescheduleDelays', [60, 180, 300, 600] )
    self.rescheduleDelaysList = [ int( x ) for x in delays ]
    self.maxRescheduleDelay = self.rescheduleDelaysList[-1]
    self.excludedOnHoldJobTypes = self.am_getOption( 'ExcludedOnHoldJobTypes', [] )

    return S_OK()

  #############################################################################
  def checkJob( self, job, classAdJob ):
    """This method controls the checking of the job.
    """
    self.log.verbose( 'Job %s will be processed' % ( job ) )

    # Check if the job was recently rescheduled
    result = self.jobDB.getJobAttributes( job, ['RescheduleCounter', 'RescheduleTime', 'ApplicationStatus'] )
    if not result['OK']:
      self.log.error( result['Message'] )
      return S_ERROR( 'Can not get job attributes from JobDB' )
    jobDict = result['Value']
    reCounter = int( jobDict['RescheduleCounter'] )
    if reCounter != 0 :
      reTime = fromString( jobDict['RescheduleTime'] )
      delta = toEpoch() - toEpoch( reTime )
      delay = self.maxRescheduleDelay
      if reCounter <= len( self.rescheduleDelaysList ):
        delay = self.rescheduleDelaysList[reCounter - 1]
      if delta < delay:
        if jobDict['ApplicationStatus'].find( 'On Hold: after rescheduling' ) == -1:
          result = self.jobDB.setJobStatus( job, application = 'On Hold: after rescheduling #%d' % reCounter )
        return S_OK()

    # First, get Site and BannedSites from the Job

    result = self.__getJobSiteRequirement( job, classAdJob )
    userBannedSites = result['BannedSites']
    userSites = result['Sites']

    if userSites:
      userSites = applySiteRequirements( userSites, [], userBannedSites )
      if not userSites:
        msg = 'Impossible Site Requirement'
        return S_ERROR( msg )

    # Second, get the Active and Banned sites from the RSS

    siteStatus = SiteStatus()
    
    usableSites   = siteStatus.getUsableSites( 'ComputingAccess' )
    unusableSites = siteStatus.getUnusableSites( 'ComputingAccess' )
    
    if not ( usableSites['OK'] and unusableSites['OK'] ):
      if not usableSites['OK']:
        self.log.error( usableSites['Message'] )
      if not unusableSites['OK']:
        self.log.error( unusableSites['Message'] )
      return S_ERROR( 'Can not get Active and Banned Sites from JobDB' )

    usableSites   = usableSites['Value']
    unusableSites = unusableSites['Value']

    if userSites:
      sites = applySiteRequirements( userSites, usableSites, unusableSites )
      if not sites:
        # Put on Hold only non-excluded job types
        jobType = classAdJob.getAttributeString( 'JobType' )
        if not jobType in self.excludedOnHoldJobTypes:
          msg = 'On Hold: Requested site is Banned or not Active'
          self.log.info( msg )
          result = self.jobDB.setJobStatus( job, application = msg )
          return S_OK()


    # Third, check if there is input data
    result = self.jobDB.getInputData( job )
    if not result['OK']:
      self.log.warn( 'Failed to get input data from JobDB for %s' % ( job ) )
      self.log.error( result['Message'] )
      return S_ERROR( 'Failed to get input data from JobDB' )

    if not result['Value']:
      return self.__sendJobToTaskQueue( job, classAdJob, userSites, userBannedSites )

    hasInputData = False
    inputData = []
    for lfn in result['Value']:
      if lfn:
        inputData.append( lfn )
        hasInputData = True

    if not hasInputData:
      #With no input data requirement, job can proceed directly to task queue
      self.log.verbose( 'Job %s has no input data requirement' % ( job ) )
      return self.__sendJobToTaskQueue( job, classAdJob, userSites, userBannedSites )

    self.log.verbose( 'Job %s has an input data requirement ' % ( job ) )

    # Fourth, Check all optimizer information
    result = self.__checkOptimizerInfo( job )
    if not result['OK']:
      return result

    optInfo = result['Value']

    #Compare site candidates with current mask
    optSites = optInfo['SiteCandidates'].keys()
    self.log.info( 'Input Data Site Candidates: %s' % ( ', '.join( optSites ) ) )
    # Check that it is compatible with user requirements
    optSites = applySiteRequirements( optSites, userSites, userBannedSites )
    if not optSites:
      msg = 'Impossible Site + InputData Requirement'
      return S_ERROR( msg )

    sites = applySiteRequirements( optSites, usableSites, unusableSites )
    if not sites:
      msg = 'On Hold: InputData Site is Banned or not Active'
      self.log.info( msg )
      result = self.jobDB.setJobStatus( job, application = msg )
      return S_OK()

    #Set stager request as necessary, optimize for smallest #files on tape if
    #more than one site candidate left at this point
    checkStaging = self.__resolveSitesForStaging( job, sites, inputData, optInfo['SiteCandidates'] )
    if not checkStaging['OK']:
      return checkStaging

    destinationSites = checkStaging['SiteCandidates']
    if not destinationSites:
      return S_ERROR( 'No destination sites available' )

    stagingFlag = checkStaging['Value']
    if stagingFlag:
      #Single site candidate chosen and staging required
      self.log.verbose( 'Job %s requires staging of input data' % ( job ) )
      # set all LFN to disk for the selected site
      stagingSite = destinationSites[0]
      siteDict = optInfo['SiteCandidates'][stagingSite]
      siteDict['disk'] = siteDict['disk'] + siteDict['tape']
      siteDict['tape'] = 0

      optInfo['SiteCandidates'][stagingSite] = siteDict
      self.log.verbose( 'Updating %s Optimizer Info for Job %s:' % ( self.dataAgentName, job ), optInfo )
      result = self.setOptimizerJobInfo( job, self.dataAgentName, optInfo )
      if not result['OK']:
        return result

      # Site is selected for staging, report it
      self.log.verbose( 'Staging site candidate for job %s is %s' % ( job, stagingSite ) )

      result = self.__getStagingSites( stagingSite, destinationSites )
      if not result['OK']:
        stagingSites = [stagingSite]
      else:
        stagingSites = result['Value']

      if len( stagingSites ) == 1:
        self.jobDB.setJobAttribute( job, 'Site', stagingSite )
      else:
        # Get the name of the site group
        result = self.__getSiteGroup( stagingSites )
        if result['OK']:
          groupName = result['Value']
          if groupName:
            self.jobDB.setJobAttribute( job, 'Site', groupName )
          else:
            self.jobDB.setJobAttribute( job, 'Site', 'Multiple' )
        else:
          self.jobDB.setJobAttribute( job, 'Site', 'Multiple' )

      stagerDict = self.__setStagingRequest( job, stagingSite, optInfo )
      if not stagerDict['OK']:
        return stagerDict
      self.__updateOtherSites( job, stagingSite, stagerDict['Value'], optInfo )
      return S_OK()
    else:
      #No staging required, can proceed to task queue agent and then waiting status
      self.log.verbose( 'Job %s does not require staging of input data' % ( job ) )
    #Finally send job to TaskQueueAgent
    return self.__sendJobToTaskQueue( job, classAdJob, destinationSites, userBannedSites )

  def __getStagingSites( self, stagingSite, destinationSites ):
    """ Get a list of sites where the staged data will be available
    """

    result = getSEsForSite( stagingSite )
    if not result['OK']:
      return result
    stagingSEs = result['Value']
    stagingSites = [stagingSite]
    for s in destinationSites:
      if s != stagingSite:
        result = getSEsForSite( s )
        if not result['OK']:
          continue
        for se in result['Value']:
          if se in stagingSEs:
            stagingSites.append( s )
            break

    stagingSites.sort()
    return S_OK( stagingSites )


  def __getSiteGroup( self, stagingSites ):
    """ Get the name of the site group if applicable. Later can be replaced by site groups defined in the CS
    """
    tier1 = ''
    groupName = ''
    for site in stagingSites:
      result = getSiteTier( site )
      if not result['OK']:
        self.log.error( result['Message'] )
        continue
      tier = result['Value']
      if tier in [0, 1]:
        tier1 = site
        if tier == 0:
          break

    if tier1:
      grid, sname, ccode = tier1.split( '.' )
      groupName = '.'.join( ['Group', sname, ccode] )

    return S_OK( groupName )

  #############################################################################
  def __updateOtherSites( self, job, stagingSite, stagedLFNsPerSE, optInfo ):
    """
      Update Optimizer Info for other sites for which the SE on which we have staged
      Files are declared local
    """
    updated = False
    seDict = {}
    for site, siteDict in optInfo['SiteCandidates'].items():
      if stagingSite == site:
        continue
      closeSEs = getSEsForSite( site )
      if not closeSEs['OK']:
        continue
      closeSEs = closeSEs['Value']
      siteDiskSEs = []
      for se in closeSEs:
        if se not in seDict:
          try:
            storageElement = StorageElement( se )
            seDict[se] = storageElement.getStatus()['Value']
          except Exception:
            self.log.exception( 'Failed to instantiate StorageElement( %s )' % se )
            continue
        seStatus = seDict[se]
        if seStatus['Read'] and seStatus['DiskSE']:
          siteDiskSEs.append( se )

      for lfn, replicas in optInfo['Value']['Value']['Successful'].items():
        for stageSE, stageLFNs in stagedLFNsPerSE.items():
          if lfn in stageLFNs and stageSE in closeSEs:
            # The LFN has been staged, we need to check now if this SE is close
            # to the Site and if the LFN was not already on a Disk SE at the Site
            isOnDisk = False
            for se in replicas:
              if se in siteDiskSEs:
                isOnDisk = True
            if not isOnDisk:
              # This is updating optInfo
              updated = True
              siteDict['disk'] += 1
              siteDict['tape'] -= 1
            break

    if updated:
      self.log.verbose( 'Updating %s Optimizer Info for Job %s:' % ( self.dataAgentName, job ), optInfo )
      self.setOptimizerJobInfo( job, self.dataAgentName, optInfo )

  #############################################################################
  def __checkOptimizerInfo( self, job ):
    """This method aggregates information from optimizers to return a list of
       site candidates and all information regarding input data.
    """
    #Check input data agent result and limit site candidates accordingly
    dataResult = self.getOptimizerJobInfo( job, self.dataAgentName )
    if dataResult['OK'] and len( dataResult['Value'] ):
      self.log.verbose( 'Retrieved from %s Optimizer Info for Job %s:' % ( self.dataAgentName, job ), dataResult )
      if 'SiteCandidates' in dataResult['Value']:
        return S_OK( dataResult['Value'] )

      msg = 'No possible site candidates'
      self.log.info( msg )
      return S_ERROR( msg )

    msg = 'File Catalog Access Failure'
    self.log.info( msg )
    return S_ERROR( msg )

  #############################################################################
  def __resolveSitesForStaging( self, job, siteCandidates, inputData, inputDataDict ):
    """Site candidates are resolved from potential candidates and any job site
       requirement is compared at this point.
    """
    self.log.verbose( 'InputData', inputData )
    self.log.verbose( 'InputDataDict', inputDataDict )
    finalSiteCandidates = []
    stageSiteCandidates = {}
    # Number of sites with all files on Disk
    diskCount = 0
    # List with the number of files on Tape
    tapeList = []
    # List with the number of files on Disk
    diskList = []
    stagingFlag = 0
    numberOfCandidates = len( siteCandidates )
    numberOfFiles = len( inputData )
    self.log.verbose( 'Job %s has %s candidate sites' % ( job, numberOfCandidates ) )
    for site in siteCandidates:
      tape = inputDataDict[site]['tape']
      disk = inputDataDict[site]['disk']
      tapeList.append( tape )
      diskList.append( disk )
      if disk not in stageSiteCandidates:
        stageSiteCandidates[disk] = []
      if tape > 0:
        self.log.verbose( '%s replicas on tape storage for %s' % ( tape, site ) )
      if disk > 0:
        self.log.verbose( '%s replicas on disk storage for %s' % ( disk, site ) )
        stageSiteCandidates[disk].append( site )
        if disk == numberOfFiles:
          diskCount += 1
          finalSiteCandidates.append( site )

    if diskCount:
      self.log.verbose( 'All replicas on disk, no staging required' )
      result = S_OK( stagingFlag )
      result['SiteCandidates'] = finalSiteCandidates
      return result

    # If not all files are available on Disk at a single site, select those with
    # a larger number of files on disk
    self.log.verbose( 'Staging is required for job' )
    stagingFlag = 1
    maxDiskValue = sorted( diskList )[-1]
    if maxDiskValue:
      self.log.verbose( 'The following sites have %s disk replicas: %s'
                        % ( maxDiskValue, stageSiteCandidates[maxDiskValue] ) )
      finalSiteCandidates.extend( stageSiteCandidates[maxDiskValue] )
    else:
      # there is no site with a replica on disk, select any of the sites
      finalSiteCandidates = list( siteCandidates )

    random.shuffle( finalSiteCandidates )
    if len( finalSiteCandidates ) > 1:
      self.log.verbose( 'Site %s has been randomly chosen for job' % ( finalSiteCandidates[0] ) )
    else:
      self.log.verbose( '%s is the site with highest number of disk replicas (=%s)' %
                        ( finalSiteCandidates[0], maxDiskValue ) )

    result = S_OK( stagingFlag )
    result['SiteCandidates'] = finalSiteCandidates
    return result

  #############################################################################
  def __setStagingRequest( self, job, destination, inputDataDict ):
    """A Staging request is formulated and saved as a job optimizer parameter.
    """

    self.log.verbose( 'Destination site %s' % ( destination ) )
    self.log.verbose( 'Input Data: %s' % ( inputDataDict ) )

    destinationSEs = getSEsForSite( destination )
    if not destinationSEs['OK']:
      return S_ERROR( 'Could not determine SEs for site %s' % destination )
    destinationSEs = destinationSEs['Value']

    siteTapeSEs = []
    siteDiskSEs = []
    for se in destinationSEs:
      storageElement = StorageElement( se )
      seStatus = storageElement.getStatus()['Value']
      if seStatus['Read'] and seStatus['TapeSE']:
        siteTapeSEs.append( se )
      if seStatus['Read'] and seStatus['DiskSE']:
        siteDiskSEs.append( se )

    if not siteTapeSEs:
      return S_ERROR( 'No LocalSEs For Site' )

    self.log.verbose( 'Site tape SEs: %s' % ( ', '.join( siteTapeSEs ) ) )
    stageSURLs = {} # OLD WAY
    stageLfns = {} # NEW WAY

    inputData = inputDataDict['Value']['Value']['Successful']
    for lfn, reps in inputData.items():
      for se, surl in reps.items():
        if se in siteDiskSEs:
          # this File is on Disk, we can ignore it
          break
        if se not in siteTapeSEs:
          # this File is not being staged
          continue
        if not lfn in stageSURLs.keys():
          stageSURLs[lfn] = {}
          stageSURLs[lfn].update( {se:surl} )
          if not stageLfns.has_key( se ): # NEW WAY
            stageLfns[se] = []          # NEW WAY
          stageLfns[se].append( lfn )     # NEW WAY

    # Now we need to check is any LFN is in more than one SE
    if len( stageLfns ) > 1:
      stageSEs = sorted( [ ( len( stageLfns[se] ), se ) for se in stageLfns.keys() ] )
      for lfn in stageSURLs:
        lfnFound = False
        for se in [ item[1] for item in reversed( stageSEs ) ]:
        # for ( numberOfLfns, se ) in reversed( stageSEs ):
          if lfnFound and lfn in stageLfns[se]:
            stageLfns[se].remove( lfn )
          if lfn in stageLfns[se]:
            lfnFound = True

    stagerClient = StorageManagerClient()
    request = stagerClient.setRequest( stageLfns, 'WorkloadManagement',
                                       'updateJobFromStager@WorkloadManagement/JobStateUpdate', job )
    if request['OK']:
      self.jobDB.setJobParameter( int( job ), 'StageRequest', str( request['Value'] ) )

    if not request['OK']:
      self.log.error( 'Problem sending Staging request:' )
      self.log.error( request )
      return S_ERROR( 'Error Sending Staging Request' )
    else:
      self.log.info( 'Staging request successfully sent' )

    result = self.updateJobStatus( job, self.stagingStatus, self.stagingMinorStatus, "Unknown" )
    if not result['OK']:
      return result
    return S_OK( stageLfns )

  #############################################################################
  def __getJobSiteRequirement( self, job, classAdJob ):
    """Returns any candidate sites specified by the job or sites that have been
       banned and could affect the scheduling decision.
    """

    result = self.jobDB.getJobAttribute( job, 'Site' )
    if not result['OK']:
      site = []
    else:
      site = List.fromChar( result['Value'] )

    result = S_OK()

    bannedSites = classAdJob.getAttributeString( 'BannedSite' )
    if not bannedSites:
      # Just try out the legacy option variant
      bannedSites = classAdJob.getAttributeString( 'BannedSites' )
    bannedSites = bannedSites.replace( '{', '' ).replace( '}', '' )
    bannedSites = List.fromChar( bannedSites )

    groupFlag = False
    for s in site:
      if "Group" in s:
        groupFlag = True

    if not 'ANY' in site and not 'Unknown' in site and not 'Multiple' in site and not groupFlag:
      if len( site ) == 1:
        self.log.info( 'Job %s has single chosen site %s specified in JDL' % ( job, site[0] ) )
      result['Sites'] = site
    elif 'Multiple' in site or groupFlag:
      result['Sites'] = classAdJob.getListFromExpression( 'Site' )
      # We might also be here after a Staging Request where several Sites are allowed
      if 'ANY' in result['Sites'] or '' in result['Sites']:
        result['Sites'] = []
    else:
      result['Sites'] = []

    if bannedSites:
      self.log.info( 'Job %s has JDL requirement to ban %s' % ( job, bannedSites ) )
      result['BannedSites'] = bannedSites
    else:
      result['BannedSites'] = []

    return result

  #############################################################################
  def __checkSitesInMask( self, job, siteCandidates ):
    """Returns list of site candidates that are in current mask.
    """

    siteStatus = SiteStatus()
    result     = siteStatus.getUsableSites( 'ComputingAccess' )  
    if not result['OK']:
      return S_ERROR( 'Could not get site mask' )

    sites = []
    usableSites = result['Value']
    for candidate in siteCandidates:
      if not candidate in usableSites:
        self.log.verbose( '%s is a candidate site for job %s but not in mask' % ( candidate, job ) )
      else:
        sites.append( candidate )

    self.log.info( 'Candidate sites in Mask are %s' % ( sites ) )

    return S_OK( sites )

  #############################################################################
  def __sendJobToTaskQueue( self, job, classAdJob, siteCandidates, bannedSites ):
    """This method sends jobs to the task queue agent and if candidate sites
       are defined, updates job JDL accordingly.
    """

    reqJDL = classAdJob.get_expression( 'JobRequirements' )
    classAddReq = ClassAd( reqJDL )

    if siteCandidates:
      classAddReq.insertAttributeVectorString( 'Sites', siteCandidates )
    if bannedSites:
      classAddReq.insertAttributeVectorString( 'BannedSites', bannedSites )

    if classAdJob.lookupAttribute( "SubmitPools" ):
      classAddReq.set_expression( 'SubmitPools', classAdJob.get_expression( 'SubmitPools' ) )
    # Hack for backward compatibility  
    elif classAdJob.lookupAttribute( "SubmitPool" ):
      classAddReq.set_expression( 'SubmitPools', classAdJob.get_expression( 'SubmitPool' ) )  

    if classAdJob.lookupAttribute( "GridMiddleware" ):
      classAddReq.set_expression( 'GridMiddleware', classAdJob.get_expression( 'GridMiddleware' ) )

    if classAdJob.lookupAttribute( "PilotType" ):
      classAddReq.set_expression( 'PilotTypes', classAdJob.get_expression( 'PilotType' ) )

    if classAdJob.lookupAttribute( "JobType" ):
      jobTypes = [ jt for jt in classAdJob.getListFromExpression( 'JobType' ) if jt ]
      classAddReq.insertAttributeVectorString( 'JobTypes', jobTypes )

    #Required CE's requirements
    gridCEs = [ ce for ce in classAdJob.getListFromExpression( 'GridCE' ) if ce ]
    if gridCEs:
      classAddReq.insertAttributeVectorString( 'GridCEs', gridCEs )
    # Hack for backward compatibility  
    else:
      gridCEs = [ ce for ce in classAdJob.getListFromExpression( 'GridRequiredCEs' ) if ce ]  
      if gridCEs:
        classAddReq.insertAttributeVectorString( 'GridCEs', gridCEs )

    if siteCandidates:
      sites = ','.join( siteCandidates )
      classAdJob.insertAttributeString( "Site", sites )

    reqJDL = classAddReq.asJDL()
    classAdJob.insertAttributeInt( 'JobRequirements', reqJDL )

    jdl = classAdJob.asJDL()
    result = self.jobDB.setJobJDL( job, jdl )
    if not result['OK']:
      return result

    if siteCandidates:
      if len( siteCandidates ) == 1:
        self.log.verbose( 'Individual site candidate for job %s is %s' % ( job, siteCandidates[0] ) )
        self.jobDB.setJobAttribute( job, 'Site', siteCandidates[0] )
      elif bannedSites:
        remainingSites = []
        for site in siteCandidates:
          if not site in bannedSites:
            remainingSites.append( site )
        if remainingSites:
          if len( remainingSites ) == 1:
            self.log.verbose( 'Individual site candidate for job %s is %s' % ( job, remainingSites[0] ) )
            self.jobDB.setJobAttribute( job, 'Site', remainingSites[0] )
          else:
            self.log.verbose( 'Site candidates for job %s are %s' % ( job, str( remainingSites ) ) )
            result = self.jobDB.getJobAttribute( job, 'Site' )
            siteGroup = "Multiple"
            if result['OK']:
              if result['Value'].startswith( 'Group' ):
                siteGroup = result['Value']
            self.jobDB.setJobAttribute( job, 'Site', siteGroup )
      else:
        self.log.verbose( 'Site candidates for job %s are %s' % ( job, str( siteCandidates ) ) )
        result = self.jobDB.getJobAttribute( job, 'Site' )
        siteGroup = "Multiple"
        if result['OK']:
          if result['Value'].startswith( 'Group' ):
            siteGroup = result['Value']
        self.jobDB.setJobAttribute( job, 'Site', siteGroup )
    else:
      self.log.verbose( 'All sites are eligible for job %s' % job )
      self.jobDB.setJobAttribute( job, 'Site', 'ANY' )

    return self.setNextOptimizer( job )

def applySiteRequirements( sites, activeSites = None, bannedSites = None ):
  """ Return site list after applying
  """
  siteList = list( sites )
  if activeSites:
    for site in sites:
      if site not in activeSites:
        siteList.remove( site )
  if bannedSites:
    for site in bannedSites:
      if site in siteList:
        siteList.remove( site )

  return siteList



#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
