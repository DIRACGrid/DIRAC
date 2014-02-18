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
from DIRAC.Resources.Catalog.FileCatalog                             import FileCatalog
from DIRAC.Core.Utilities.SiteSEMapping                              import getSitesForSE
from DIRAC.Core.Utilities.List                                       import uniqueElements
from DIRAC                                                           import S_OK, S_ERROR
from DIRAC.DataManagementSystem.Client.DataManager                   import DataManager


class InputData( OptimizerExecutor ):
  """
      The specific Optimizer must provide the following methods:
      - initializeOptimizer() before each execution cycle
      - checkJob() - the main method called for each job
  """

  @classmethod
  def initializeOptimizer( cls ):
    """Initialize specific parameters for JobSanityAgent.
    """
    cls.failedMinorStatus = cls.ex_getOption( '/FailedJobStatus', 'Input Data Not Available' )
    #this will ignore failover SE files
    cls.checkFileMetadata = cls.ex_getOption( 'CheckFileMetadata', True )

    #Define the shifter proxy needed
    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/ProductionManager
    # the shifterProxy option in the Configuration can be used to change this default.
    cls.ex_setProperty( 'shifterProxy', 'DataManager' )

    try:
      cls.__dataMan = DataManager()
    except Exception, e:
      msg = 'Failed to create DataManager'
      cls.log.exception( msg )
      return S_ERROR( msg + str( e ) )

    try:
      cls.__fc = FileCatalog()
    except Exception, e:
      msg = 'Failed to create FileCatalog'
      cls.log.exception( msg )
      return S_ERROR( msg + str( e ) )

    cls.__SEToSiteMap = {}
    cls.__lastCacheUpdate = 0
    cls.__cacheLifeTime = 600

    return S_OK()

  def optimizeJob( self, jid, jobState ):
    result = jobState.getInputData()
    if not result[ 'OK' ]:
      self.jobLog.error( "Cannot retrieve input data: %s" % result[ 'Message' ] )
      return S_ERROR( "Cannot retrieve input data" )
    if not result[ 'Value' ]:
      self.jobLog.notice( "No input data. Skipping." )
      return self.setNextOptimizer()
    inputData = result[ 'Value' ]

    #Check if we already executed this Optimizer and the input data is resolved
    result = self.retrieveOptimizerParam( self.ex_getProperty( 'optimizerName' ) )
    if result['OK'] and result['Value']:
      self.jobLog.info( "Retrieving stored info" )
      resolvedData = result['Value']
    else:
      self.jobLog.info( 'Processing input data' )
      result = self.__resolveInputData( jobState, inputData )
      if not result['OK']:
        self.jobLog.warn( result['Message'] )
        return result
      resolvedData = result['Value']

#    #Now check if banned SE's might prevent jobs to be scheduled
#    result = self.__checkActiveSEs( jobState, resolvedData['Value']['Value'] )
#    if not result['OK']:
#      # if after checking SE's input data can not be resolved any more
#      # then keep the job in the same status and update the application status
#      self.freezeTask( 600 )
#      return jobState.setAppStatus( result['Message'] )

    return self.setNextOptimizer()

  #############################################################################
  def __resolveInputData( self, jobState, inputData ):
    """This method checks the file catalog for replica information.
    """
    lfns = []
    for lfn in inputData:
      if lfn[:4].lower() == "lfn:":
        lfns.append( lfn[4:] )
      else:
        lfns.append( lfn )


    startTime = time.time()

    print "LFNS", lfns

    result = self.__dataMan.getActiveReplicas( lfns )  # This will return already active replicas, excluding banned SEs
    self.jobLog.info( 'Catalog replicas lookup time: %.2f seconds ' % ( time.time() - startTime ) )
    if not result['OK']:
      self.log.warn( result['Message'] )
      return result

    replicaDict = result['Value']

    print "REPLICA DICT", replicaDict

    result = self.__checkReplicas( jobState, replicaDict )

    if not result['OK']:
      self.jobLog.error( result['Message'] )
      return result
    siteCandidates = result[ 'Value' ]

    if self.ex_getOption( 'CheckFileMetadata', True ):
      start = time.time()
      guidDict = self.__fc.getFileMetadata( lfns )
      self.jobLog.info( 'Catalog Metadata Lookup Time: %.2f seconds ' % ( time.time() - startTime ) )

      if not guidDict['OK']:
        self.log.warn( guidDict['Message'] )
        return guidDict

      failed = guidDict['Value']['Failed']
      if failed:
        self.log.warn( 'Failed to establish some GUIDs' )
        self.log.warn( failed )

      for lfn in replicaDict['Successful']:
        replicas = replicaDict['Successful'][ lfn ]
        guidDict['Value']['Successful'][lfn].update( replicas )

    resolvedData = {}
    resolvedData['Value'] = guidDict
    resolvedData['SiteCandidates'] = siteCandidates
    self.jobLog.verbose( "Storing:\n%s" % pprint.pformat( resolvedData ) )
    result = self.storeOptimizerParam( self.ex_getProperty( 'optimizerName' ), resolvedData )
    if not result['OK']:
      self.log.warn( result['Message'] )
      return result
    return S_OK( resolvedData )

  #############################################################################
  def __checkReplicas( self, jobState, replicaDict ):
    """Check that all input lfns have valid replicas and can all be found at least in one single site.
    """
    badLFNs = []

    if 'Successful' not in replicaDict:
      return S_ERROR( 'No replica Info available' )

    okReplicas = replicaDict['Successful']
    for lfn in okReplicas:
      if not okReplicas[ lfn ]:
        badLFNs.append( 'LFN:%s -> No replicas available' % ( lfn ) )

    if 'Failed' in replicaDict:
      errorReplicas = replicaDict[ 'Failed' ]
      for lfn in errorReplicas:
        badLFNs.append( 'LFN:%s -> %s' % ( lfn, errorReplicas[ lfn ] ) )

    if badLFNs:
      errorMsg = "\n".join( badLFNs )
      self.jobLog.info( 'Found %s problematic LFN(s):\n%s' % ( len( badLFNs ), errorMsg ) )
      result = jobState.setParameter( self.ex_getProperty( 'optimizerName' ), errorMsg )
      if not result['OK']:
        self.log.error( result['Message'] )
      return S_ERROR( 'Input data not available' )

    return self.__getSiteCandidates( okReplicas )

  #############################################################################
  def __checkActiveSEs( self, jobState, replicaDict ):
    """
    Check active SE and replicas and identify possible Site candidates for
    the execution of the job
    """
    # Now let's check if some replicas might not be available due to banned SE's
    self.jobLog.info( "Checking active replicas" )
    startTime = time.time()
    result = self.__dataMan.checkActiveReplicas( replicaDict )
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
  def __getSiteCandidates( self, okReplicas ):
    """This method returns a list of possible site candidates based on the
       job input data requirement.  For each site candidate, the number of files
       on disk and tape is resolved.
    """

    lfnSEs = {}
    for lfn in okReplicas:
      replicas = okReplicas[ lfn ]
      siteSet = set()
      for seName in replicas:
        result = self.__getSitesForSE( seName )
        if result['OK']:
          siteSet.update( result['Value'] )
      lfnSEs[ lfn ] = siteSet

    if not lfnSEs:
      return S_ERROR( "No candidate sites available" )

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
          if seStatus[ 'DiskSE' ]:
            #Sets contain only unique elements, no need to check if it's there
            diskLFNs.add( lfn )
            if lfn in tapeLFNs:
              tapeLFNs.remove( lfn )
          if seStatus[ 'TapeSE' ]:
            if lfn not in diskLFNs:
              tapeLFNs.add( lfn )

    for siteName in sitesData:
      sitesData[siteName]['disk'] = len( sitesData[siteName]['disk'] )
      sitesData[siteName]['tape'] = len( sitesData[siteName]['tape'] )
    return S_OK( sitesData )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
