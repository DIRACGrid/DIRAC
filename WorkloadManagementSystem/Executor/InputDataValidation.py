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
import pprint
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor  import OptimizerExecutor
from DIRAC.Resources.Storage.StorageElement                          import StorageElement
from DIRAC.Core.Utilities.SiteSEMapping                              import getSitesForSE
from DIRAC.Core.Utilities                                            import DictCache
from DIRAC                                                           import S_OK, S_ERROR


class InputDataValidation( OptimizerExecutor ):
  """
      The specific Optimizer must provide the following methods:
      - initializeOptimizer() before each execution cycle
      - checkJob() - the main method called for each job
  """

  @classmethod
  def initializeOptimizer( cls ):
    cls.__SEStatus = DictCache()

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

    result = self.getCandidateSEs( lfnData )

    #Now check if banned SE's might prevent jobs to be scheduled
    result = self.__checkActiveSEs( jobState, resolvedData['Value']['Value'] )
    if not result['OK']:
      # if after checking SE's input data can not be resolved any more
      # then keep the job in the same status and update the application status
      self.freezeTask( 600 )
      return jobState.setAppStatus( result['Message'] )

    return self.setNextOptimizer()

  def __getSEStatus( self, seName ):
    result = self.__SEStatus.get( seName )
    if result == False:
      seObj = StorageElement( seName )
      result = seObj.getStatus()
      if not result[ 'OK' ]:
        return result
      self.__SEStatus.add( seName, 600, result )
    return result

  def __checkActiveSEs( self, jobState, replicaDict ):
    """
    Check active SE and replicas and identify possible Site candidates for
    the execution of the job
    """
    # Now let's check if some replicas might not be available due to banned SE's
    self.jobLog.info( "Checking active replicas" )
    startTime = time.time()
    result = self.__replicaMan.checkActiveReplicas( replicaDict )
    self.jobLog.info( "Active replica check took %.2f secs" % ( time.time() - startTime ) )
    if not result['OK']:
      # due to banned SE's input data might no be available
      msg = "On Hold: Input data not Available for SE"
      self.jobLog.warn( result['Message'] )
      return S_ERROR( result['Message'] )

    activeReplicaDict = result['Value']

    result = self.__checkReplicas( jobState, activeReplicaDict )

    if not result['OK']:
      # due to a banned SE's input data is not available at a single site
      msg = "On Hold: Input data not Available due to banned SE"
      self.jobLog.warn( result['Message'] )
      return S_ERROR( msg )

    resolvedData = {}
    #THIS IS ONE OF THE MOST HORRIBLE HACKS. I hate the creator of the Value of Value of Successful of crap...
    resolvedData['Value'] = S_OK( activeReplicaDict )
    resolvedData['SiteCandidates'] = result['Value']
    result = self.storeOptimizerParam( self.ex_getProperty( 'optimizerName' ), resolvedData )
    if not result['OK']:
      self.log.warn( result['Message'] )
      return result
    return S_OK( resolvedData )


  #############################################################################
  def __getSitesForSE( self, seName ):
    """ Returns a list of sites having the given SE as a local one.
        Uses the local cache of the site-se information
    """

    # Empty the cache if too old
    now = time.time()
    if ( now - self.__lastCacheUpdate ) > self.__cacheLifeTime:
      self.log.verbose( 'Resetting the SE to site mapping cache' )
      self.__SEToSiteMap = {}
      self.__lastCacheUpdate = now

    if seName not in self.__SEToSiteMap:
      result = getSitesForSE( seName )
      if not result['OK']:
        return result
      self.__SEToSiteMap[ seName ] = list( result['Value'] )
    return S_OK( self.__SEToSiteMap[ seName ] )

  #############################################################################
  def __getSiteCandidates( self, lfnData ):
    """This method returns a list of possible site candidates based on the
       job input data requirement.  For each site candidate, the number of files
       on disk and tape is resolved.
    """

    lfnSites = {}
    for lfn in okReplicas:
      replicas = okReplicas[ lfn ]
      diskSiteSet = set()
      tapeSiteSet = set()
      for seName in replicas:
        result = self.__getSitesForSE( seName )
        if result['OK']:
          if replicas[ seName ][ 'Disk' ]:
            diskSiteSet.update( result['Value'] )
          else:
            tapeSiteSet.update( result['Value'] )
      lfnSites[ lfn ] = { 'Disk' : diskSiteSet, 'Tape' : tapeSiteSet }

    #First

    #This makes an intersection of all sets in the dictionary and returns a set with it
    siteCandidates = set.intersection( *[ lfnSEs[ lfn ] for lfn in lfnSEs ] )

    if not siteCandidates:
      return S_ERROR( 'No candidate sites available' )

    #In addition, check number of files on tape and disk for each site
    #for optimizations during scheduling
    sitesData = {}
    for siteName in siteCandidates:
      sitesData[ siteName ] = { 'disk': set(), 'tape': set() }

    #Loop time!
    seDict = {}
    for lfn in okReplicas:
      replicas = okReplicas[ lfn ]
      #Check each SE in the replicas
      for seName in replicas:
        #If not already "loaded" the add it to the dict
        if seName not in seDict:
          result = self.__getSitesForSE( seName )
          if not result['OK']:
            self.jobLog.warn( "Could not get sites for SE %s: %s" % ( seName, result[ 'Message' ] ) )
            continue
          siteList = result[ 'Value' ]
          seObj = StorageElement( seName )
          result = seObj.getStatus()
          if not result[ 'OK' ]:
            self.jobLog.error( "Could not retrieve status for SE %s: %s" % ( seName, result[ 'Message' ] ) )
            continue
          seStatus = result[ 'Value' ]
          seDict[ seName ] = { 'Sites': siteList, 'Status': seStatus }
        #Get SE info from the dict
        seData = seDict[ seName ]
        siteList = seData[ 'Sites' ]
        seStatus = seData[ 'Status' ]
        for siteName in siteList:
          #If not a candidate site then skip it
          if siteName not in siteCandidates:
            continue
          #Add the LFNs to the disk/tape lists
          diskLFNs = sitesData[ siteName ][ 'disk' ]
          tapeLFNs = sitesData[ siteName ][ 'tape' ]
          if seStatus[ 'Read' ] and seStatus[ 'DiskSE' ]:
            #Sets contain only unique elements, no need to check if it's there
            diskLFNs.add( lfn )
            if lfn in tapeLFNs:
              tapeLFNs.remove( lfn )
          if seStatus[ 'Read' ] and seStatus[ 'TapeSE' ]:
            if lfn not in diskLFNs:
              tapeLFNs.add( lfn )

    for siteName in sitesData:
      sitesData[siteName]['disk'] = len( sitesData[siteName]['disk'] )
      sitesData[siteName]['tape'] = len( sitesData[siteName]['tape'] )
    return S_OK( sitesData )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
