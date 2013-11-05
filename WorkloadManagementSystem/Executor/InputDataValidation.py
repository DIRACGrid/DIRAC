########################################################################
# $HeadURL$
# File :    InputDataAgent.py
########################################################################
"""
  The Input Data Agent queries the file catalog for specified job input data and adds the
  relevant information to the job optimizer parameters to be used during the
  scheduling decision.
"""
__RCSID__ = "$Id$"

import time
import random
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor  import OptimizerExecutor
from DIRAC.Resources.Storage.StorageElement                          import StorageElement
from DIRAC.Core.Utilities.SiteSEMapping                              import getSitesForSE, getSEsForSite
from DIRAC.Core.Utilities                                            import DictCache
from DIRAC.Core.Security                                             import Properties
from DIRAC.ConfigurationSystem.Client.Helpers                        import Registry
from DIRAC.ResourceStatusSystem.Client.SiteStatus                    import SiteStatus
from DIRAC.StorageManagementSystem.Client.StorageManagerClient       import StorageManagerClient
from DIRAC                                                           import S_OK, S_ERROR


class InputDataValidation( OptimizerExecutor ):
  """
      The specific Optimizer must provide the following methods:
      - initializeOptimizer() before each execution cycle
      - checkJob() - the main method called for each job
  """

  @classmethod
  def initializeOptimizer( cls ):
    """ Initialization of the Agent.
    """
    random.seed()
    cls.__SEStatus = DictCache.DictCache()
    cls.__sitesForSE = DictCache.DictCache()
    try:
      from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
    except ImportError, excp :
      return S_ERROR( "Could not import JobDB: %s" % str( excp ) )

    try:
      cls.__jobDB = JobDB()
    except RuntimeError:
      return S_ERROR( "Cannot connect to JobDB" )

    cls.__siteStatus = SiteStatus()
    cls.ex_setOption( "FailedStatus", "Input Data Not Available" )
    return S_OK()

  def optimizeJob( self, jid, jobState ):
    result = self.doTheThing( jid, jobState )
    if not result[ 'OK' ]:
      jobState.setAppStatus( result[ 'Message' ] )
      return result
    return S_OK()

  def doTheThing( self, jid, jobState ):
    result = jobState.getInputData()
    if not result[ 'OK' ]:
      self.jobLog.error( "Can't retrieve input data: %s" % result[ 'Message' ] )
      return result
    lfnData = result[ 'Value' ]

    result = jobState.getManifest()
    if not result[ 'OK' ]:
      return result
    manifest = result[ 'Value' ]

    result = self.freezeByBannedSE( manifest, lfnData )
    if not result[ 'OK' ]:
      return result

    result = self.selectSiteStage( manifest, lfnData )
    if not result[ 'OK' ]:
      return result
    candidates, lfn2Stage = result[ 'Value' ]

    if not lfn2Stage:
      jobState.setOptParameter( "DataSites", ",".join( candidates ) )
      self.jobLog.notice( "No need to stage. Sending to next optimizer" )
      return self.setNextOptimizer()

    if self.ex_getOption( "RestrictDataStage", False ):
      if not self.__checkStageAllowed( jobState ):
        return S_ERROR( "Stage not allowed" )

    result = jobState.getOptParameter( "StageRequestedForSites" )
    if not result[ 'OK' ]:
      raise RuntimeError( "Can't retrieve optimizer parameter! (%s)" % result[ 'Message' ] )
    stageRequested = result[ 'Value' ]
    if stageRequested:
      jobState.setOptParameter( "DataSites", stageRequested )
      self.jobLog.info( "Stage already requested. Sending to next optimizer" )
      return self.setNextOptimizer()

    result = self.requestStage( jobState, candidates, lfnData )
    if not result[ 'OK' ]:
      return result

    stageCandidates = result[ 'Value' ]
    self.jobLog.notice( "Requested stage at sites %s" % ",".join( stageCandidates ) )
    result = jobState.setOptParameter( "StageRequestedForSites", ",".join( stageCandidates ) )
    if not result[ 'OK' ]:
      return result

    #TODO: What if more than one stage site?
    jobState.setAttribute( 'Site', list( stageCandidates )[0] )

    return S_OK()

  def __checkStageAllowed( self, jobState ):
    """Check if the job credentials allow to stage date """
    result = jobState.getAttribute( "OwnerGroup" )
    if not result[ 'OK' ]:
      self.jobLog.error( "Cannot retrieve OwnerGroup from DB: %s" % result[ 'Message' ] )
      return S_ERROR( "Cannot get OwnerGroup" )
    group = result[ 'Value' ]
    return Properties.STAGE_ALLOWED in Registry.getPropertiesForGroup( group )

  def freezeByBannedSE( self, manifest, lfnData ):
    targetSEs = manifest.getOption( "TargetSEs", [] )
    if targetSEs:
      self.jobLog.info( "TargetSEs defined: %s" % ", ".join( targetSEs ) )
    for lfn in lfnData:
      replicas = lfnData[ lfn ][ 'Replicas' ]
      anyBanned = False
      for seName in list( replicas.keys() ):
        if targetSEs and seName not in targetSEs:
          self.jobLog.info( "Ignoring replica in %s (not in TargetSEs)" % seName )
          replicas.pop( seName )
          continue
        result = self.__getSEStatus( seName )
        if not result[ 'OK' ]:
          self.jobLog.error( "Can't retrieve status for SE %s" % seName )
          replicas.pop( seName )
          anyBanned = True
          continue
        seStatus = result[ 'Value' ]
        if not seStatus[ 'Read' ]:
          self.jobLog.info( "Ignoring replica in %s (SE is not readable)" % seName )
          replicas.pop( seName )
          anyBanned = True
          continue
      if anyBanned:
        raise OptimizerExecutor.FreezeTask( "Banned SE makes access to Input Data impossible" )
      if not replicas:
        return S_ERROR( "%s has no replicas in any target SE" % lfn )

    return S_OK()

  def selectSiteStage( self, manifest, lfnData ):
    lfnSite = {}
    tapelfn = {}
    disklfn = set()
    for lfn in lfnData:
      replicas = lfnData[ lfn ][ 'Replicas' ]
      lfnSite[ lfn ] = set()
      for seName in replicas:
        result = self.__getSitesForSE( seName )
        if not result[ 'OK' ]:
          return result
        sites = result[ 'Value' ]
        lfnSite[ lfn ].update( sites )
        if lfn not in disklfn:
          if replicas[ seName ][ 'Disk' ]:
            disklfn.add( lfn )
            try:
              tapelfn.pop( lfn )
            except KeyError:
              pass
          else:
            if lfn not in tapelfn:
              tapelfn[ lfn ] = set()
            for site in sites:
              tapelfn[ lfn ].add( site )

    candidates = set.intersection( *[ lfnSite[ lfn ] for lfn in lfnSite ] )
    userSites = manifest.getOption( "Site", [] )
    if userSites:
      candidates = set.intersection( candidates, userSites )
    bannedSites = manifest.getOption( "BannedSites", [] )
    candidates = candidates.difference( bannedSites )
    if not candidates:
      return S_ERROR( "No candidate sites available" )

    self.jobLog.info( "Sites with access to input data are %s" % ",".join( candidates ) )

    if not tapelfn:
      self.jobLog.info( "No need to stage. Candidates are %s" % ", ".join( candidates ) )
      return S_OK( ( candidates, False ) )

    self.jobLog.info( "Need to stage %s files at most" % len( tapelfn ) )

    tapeCandidates = {}
    for lfn in tapelfn:
      for site in set.intersection( candidates, tapelfn[lfn] ):
        if site not in tapeCandidates:
          tapeCandidates[ site ] = 0
        tapeCandidates[ site ] += 1

    if len( tapeCandidates ) == 1:
      stageSite = tapeCandidates.keys()[0]
      minStage = tapeCandidates[ stageSite ]
      tapeCandidates = set( [ stageSite ] )
    else:
      minStage = min( *[ tapeCandidates[ site ] for site in tapeCandidates ] )
      tapeCandidates = set( [ site for site in tapeCandidates if tapeCandidates[ site ] == minStage ] )

    self.jobLog.info( "Sites %s need to stage %d files" % ( ",".join( tapeCandidates ), minStage ) )

    result = self.__siteStatus.getUnusableSites( 'ComputingAccess' )
    if result[ 'OK' ]:
      for site in result[ 'Value' ]:
        tapeCandidates.discard( site )
      if not tapeCandidates:
        raise OptimizerExecutor.FreezeTask( "All stageable sites are banned" )

    finalCandidates = set.intersection( tapeCandidates, candidates )
    if not finalCandidates:
      self.jobLog.error( "No site that can stage is allowed to run" )
      return S_ERROR( "No site can fullfill requirements" )

    self.jobLog.info( "Candidate sites for staging are %s" % ", ".join( finalCandidates ) )

    return S_OK( ( finalCandidates, set( tapelfn ) ) )


  def __getSitesForSE( self, seName ):
    result = self.__sitesForSE.get( seName )
    if result == False:
      result = getSitesForSE( seName )
      if not result['OK']:
        return result
      self.__sitesForSE.add( seName, 600, result )
    return result

  def __getSEStatus( self, seName ):
    result = self.__SEStatus.get( seName )
    if result == False:
      seObj = StorageElement( seName )
      result = seObj.getStatus()
      if not result[ 'OK' ]:
        return result
      self.__SEStatus.add( seName, 600, result )
    return result

  def requestStage( self, jobState, candidates, lfnData ):
    #Any site is as good as any so random time!
    stageSite = random.sample( candidates, 1 )[0]
    self.jobLog.info( "Site selected %s for staging" % stageSite )
    result = getSEsForSite( stageSite )
    if not result['OK']:
      return S_ERROR( 'Could not determine SEs for site %s' % stageSite )
    siteSEs = result['Value']

    tapeSEs = []
    diskSEs = []
    for seName in siteSEs:
      result = self.__getSEStatus( seName )
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

    stageLFNs = {}
    lfnToStage = []
    for lfn in lfnData:
      replicas = lfnData[ lfn ][ 'Replicas' ]
      # Check SEs
      seStage = []
      for seName in replicas:
        _surl = replicas[ seName ][ 'SURL' ]
        if seName in diskSEs:
          # This lfn is in disk. Skip it
          seStage = []
          break
        if seName not in tapeSEs:
          # This lfn is not in this tape SE. Check next SE
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

    # Check if any LFN is in more than one SE
    # If that's the case, try to stage from the SE that has more LFNs to stage to group the request
    # 1.- Get the SEs ordered by ascending replicas
    sortedSEs = reversed( sorted( [ ( len( stageLFNs[ seName ] ), seName ) for seName in stageLFNs.keys() ] ) )
    for lfn in lfnToStage:
      found = False
      # 2.- Traverse the SEs
      for _stageCount, seName in sortedSEs:
        if lfn in stageLFNs[ seName ]:
          # 3.- If first time found, just mark as found. Next time delete the replica from the request
          if found:
            stageLFNs[ seName ].remove( lfn )
          else:
            found = True
        # 4.-If empty SE, remove
        if len( stageLFNs[ seName ] ) == 0:
          stageLFNs.pop( seName )

    self.jobLog.info( "Stage request will be \n\t%s" % "\n\t".join( [ "%s:%s" % ( lfn, stageLFNs[ lfn ] ) for lfn in stageLFNs ] ) )

    stagerClient = StorageManagerClient()
    result = stagerClient.setRequest( stageLFNs, 'WorkloadManagement',
                                      'stageCallback@WorkloadManagement/OptimizationMind',
                                      int( jobState.jid ) )
    if not result[ 'OK' ]:
      self.jobLog.error( "Could not send stage request: %s" % result[ 'Message' ] )
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

    stageCandidates = []
    for seName in stageLFNs:
      result = self.__getSitesForSE( seName )
      if result[ 'OK' ]:
        stageCandidates.append( result[ 'Value' ] )

    stageCandidates = candidates.intersection( *[ sC for sC in stageCandidates ] ).union( [ stageSite ] )
    return S_OK( stageCandidates )

