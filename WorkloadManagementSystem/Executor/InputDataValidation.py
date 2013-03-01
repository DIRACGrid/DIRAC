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
    cls.__SEStatus = DictCache()
    cls.__sitesForSE = DictCache()
    try:
      from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
    except ImportError, excp :
      return S_ERROR( "Could not import JobDB: %s" % str( excp ) )

    try:
      cls.__jobDB = JobDB()
    except RuntimeError:
      return S_ERROR( "Cannot connect to JobDB" )
    return S_OK()


  def optimizeJob( self, jid, jobState ):
    result = self.doTheThing( jobState )
    if not result[ 'OK' ]:
      jobState.setAppStatus( result[ 'Message' ] )
      return S_ERROR( cls.ex_getOption( "FailedJobStatus", "Input Data Not Available" ) )
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
    if result[ 'Value' ]:
      self.freezeTask( 600 )
      self.jobLog.info( "On hold -> Banned SE make access to lfn impossible" )
      return S_OK()

    result = self.selectSiteStage( lfnData )
    if not result[ 'OK' ]:
      return result
    candidates, lfn2Stage = result[ 'Value' ]

    if not lfn2Stage:
      jobState.setOptimizerParameter( "SiteCandidates", ",".join( candidates ) )
      return self.setNextOptimizer()

    if self.ex_getOption( "RestrictDataStage", False ):
      if not self.__checkStageAllowed( jobState ):
        return S_ERROR( "Stage not allowed" )

    result = jobState.getOptimizerParameter( "StageRequestedForSite" )
    if not result[ 'OK' ]:
      raise RuntimeError( "Can't retrieve optimizer parameter! (%s)" % result[ 'Message' ] )
    stageRequested = result[ 'Value' ]
    if stageRequested:
      jobState.setOptimizerParameter( "SiteCandidates", stageRequested )
      return self.setNextOptimizer()

    result = self.requestStage( jobState, candidates, lfnData )
    if not result[ 'OK' ]:
      return result

    jobState.setOptimizerParameter( "StageRequestedForSite", result[ 'Value' ] )
    return self.setNextOptimizer()

  def __checkStageAllowed( self, jobState ):
    """Check if the job credentials allow to stage date """
    result = jobState.getAttribute( "OwnerGroup" )
    if not result[ 'OK' ]:
      self.jobLog.error( "Cannot retrieve OwnerGroup from DB: %s" % result[ 'Message' ] )
      return S_ERROR( "Cannot get OwnerGroup" )
    group = result[ 'Value' ]
    return Properties.STAGE_ALLOWED in Registry.getPropertiesForGroup( group )

  def freezeByBannedSE( self, manifest, lfnData ):
    targetSEs = manigest.getOption( "TargetSEs", [] )
    if not targetSEs:
      return S_OK( False )
    for lfn in lfnData:
      replicas = lfnData[ lfn ][ 'Replicas' ]
      anyBanned = False
      for seName in list( replicas.keys() ):
        if seName not in targetSEs:
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
          replicas.pop( seName )
          anyBanned = True
          continue
      if anyBanned:
        return S_OK( True )
      if not replicas:
        return S_ERROR( "%s has no replicas in any target SE" % lfn )

    return S_OK( False )

  def selectSiteStage( self, manifest, lfnData ):
    lfnSite = {}
    tapelfn = {}
    disklfn = set()
    for lfn in lfnData:
      replicas = lfnData[ lfn ][ 'Replicas' ]
      lfnSite[ lfn ] = set()
      for seName in replicas:
        result = self.__getSiteForSE( seName )
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
            tapelfn[ lfn ].add( sites )

    candidates = set.intersection( *[ lfnSite[ lfn ] for lfn in lfnSite ] )
    userSites = manifest.getOption( "Site", [] )
    if userSites:
      candidates = set.intersection( candidates, userSites )
    if not candidates:
      return S_ERROR( "No candidate sites available" )

    if not tapelfn:
      return S_OK( ( candidates, False ) )

    tapeInSite = {}
    for lfn in tapelfn:
      for site in tapelfn[ lfn ]:
        if site not in tapeInSite:
          tapeInSite[ site ] = 0
        tapeInSite[ site ] += 1

    result = self.__jobDB.getSiteMask( 'Banned' )
    if not result[ 'OK' ]:
      bannedSites = []
    else:
      bannedSites = result[ 'Value' ]

    for site in bannedSites:
      try:
        tapeInSite.pop( site )
      except KeyError:
        pass

    minTape = None
    for site in tapeInSite:
      if minTape == None:
        minTape = tapeInSite[ site ]
      else:
        minTape = min( minTape, tapeInSite[ site ] )

    #Sites with min stage
    stageSites = [ site for site in tapeInSite if tapeInSite[ site ] == minTape ]

    return S_OK( stageSites, set( tapelfn ) )


  def __getSiteForSE( self, seName ):
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
    if len( candidates ) > 1:
      random.shuffle( candidates )
    stageSite = candidate[0]
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
      replicas = inputData[ lfn ][ 'Replicas' ]
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

    self.jobLog.verbose( "Stage request will be \n\t%s" % "\n\t".join( [ "%s:%s" % ( lfn, stageLFNs[ lfn ] ) for lfn in stageLFNs ] ) )

    stagerClient = StorageManagerClient()
    result = stagerClient.setRequest( stageLFNs, 'WorkloadManagement',
                                      'updateJobFromStager@WorkloadManagement/JobStateUpdate',
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
    return S_OK( stageSite )


