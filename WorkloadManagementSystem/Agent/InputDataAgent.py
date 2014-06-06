"""   
  The Input Data Agent queries the file catalog for specified job input data and adds the
  relevant information to the job optimizer parameters to be used during the
  scheduling decision.
"""
__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.Agent.OptimizerModule  import OptimizerModule

from DIRAC.Core.Utilities.SiteSEMapping                    import getSitesForSE
from DIRAC.Core.Utilities.List                             import uniqueElements
from DIRAC                                                 import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Resources    import Resources
from DIRAC.DataManagementSystem.Client.DataManager         import DataManager
from DIRAC.Resources.Catalog.FileCatalog                   import FileCatalog
from DIRAC.ConfigurationSystem.Client.Helpers.Resources    import getStorageElementOptions
from DIRAC.ResourceStatusSystem.Client.ResourceStatus      import ResourceStatus

import time

class InputDataAgent( OptimizerModule ):
  """
      The specific Optimizer must provide the following methods:
      - initializeOptimizer() before each execution cycle
      - checkJob() - the main method called for each job
  """

  #############################################################################
  def initializeOptimizer( self ):
    """Initialize specific parameters for JobSanityAgent.
    """
    self.failedMinorStatus = self.am_getOption( '/FailedJobStatus', 'Input Data Not Available' )
    #this will ignore failover SE files
    self.checkFileMetadata = self.am_getOption( 'CheckFileMetadata', True )

    #Define the shifter proxy needed
    # This sets the Default Proxy to used as that defined under 
    # /Operations/Shifter/ProductionManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'ProductionManager' )

    try:
      self.dataManager = DataManager()
    except Exception, e:
      msg = 'Failed to create DataManager'
      self.log.exception( msg )
      return S_ERROR( msg + str( e ) )
    
    self.resourceStatus  = ResourceStatus()
    self.resourcesHelper = Resources()
    self.fc = FileCatalog()
    self.seToSiteMapping = {}
    self.lastCScheck = 0
    self.cacheLength = 600
    

    return S_OK()

  #############################################################################
  def checkJob( self, job, classAdJob ):
    """
    This method does the optimization corresponding to this Agent, 
    it is call for each job by the Optimizer framework
    """

    result = self.jobDB.getInputData( job )
    if not result['OK']:
      self.log.warn( 'Failed to get input data from JobdB for %s' % ( job ) )
      self.log.warn( result['Message'] )
      return result
    if not result['Value']:
      self.log.verbose( 'Job %s has no input data requirement' % ( job ) )
      return self.setNextOptimizer( job )

    #Check if we already executed this Optimizer and the input data is resolved
    res = self.getOptimizerJobInfo( job, self.am_getModuleParam( 'optimizerName' ) )
    if res['OK'] and len( res['Value'] ):
      pass
    else:
      self.log.verbose( 'Job %s has an input data requirement and will be processed' % ( job ) )
      inputData = result['Value']
      result = self.__resolveInputData( job, inputData )
      if not result['OK']:
        self.log.warn( result['Message'] )
        return result

    return self.setNextOptimizer( job )

  #############################################################################
  def __resolveInputData( self, job, inputData ):
    """This method checks the file catalog for replica information.
    """
    lfns = [ fname.replace( 'LFN:', '' ) for fname in inputData ]

    start = time.time()
    # In order to place jobs on Hold if a certain SE is banned we need first to check first if
    # if the replicas are really available
    replicas = self.dataManager.getActiveReplicas( lfns )
    timing = time.time() - start
    self.log.verbose( 'Catalog Replicas Lookup Time: %.2f seconds ' % ( timing ) )
    if not replicas['OK']:
      self.log.warn( replicas['Message'] )
      return replicas

    replicaDict = replicas['Value']

    siteCandidates = self.__checkReplicas( job, replicaDict )

    if not siteCandidates['OK']:
      self.log.warn( siteCandidates['Message'] )
      return siteCandidates

    if self.checkFileMetadata:
      guids = True
      start = time.time()
      guidDict = self.fc.getFileMetadata( lfns )
      timing = time.time() - start
      self.log.info( 'Catalog Metadata Lookup Time: %.2f seconds ' % ( timing ) )

      if not guidDict['OK']:
        self.log.warn( guidDict['Message'] )
        guids = False

      failed = guidDict['Value']['Failed']
      if failed:
        self.log.warn( 'Failed to establish some GUIDs' )
        self.log.warn( failed )
        guids = False

      if guids:
        for lfn, reps in replicaDict['Successful'].items():
          guidDict['Value']['Successful'][lfn].update( reps )
        replicas = guidDict

    resolvedData = {}
    resolvedData['Value'] = replicas
    resolvedData['SiteCandidates'] = siteCandidates['Value']
    result = self.setOptimizerJobInfo( job, self.am_getModuleParam( 'optimizerName' ), resolvedData )
    if not result['OK']:
      self.log.warn( result['Message'] )
      return result
    return S_OK( resolvedData )

  #############################################################################
  def __checkReplicas( self, job, replicaDict ):
    """Check that all input lfns have valid replicas and can all be found at least in one single site.
    """
    badLFNs = []

    if replicaDict.has_key( 'Successful' ):
      for lfn, reps in replicaDict['Successful'].items():
        if not reps:
          badLFNs.append( 'LFN:%s Problem: No replicas available' % ( lfn ) )
    else:
      return S_ERROR( 'No replica Info available' )

    if replicaDict.has_key( 'Failed' ):
      for lfn, cause in replicaDict['Failed'].items():
        badLFNs.append( 'LFN:%s Problem: %s' % ( lfn, cause ) )

    if badLFNs:
      self.log.info( 'Found %s problematic LFN(s) for job %s' % ( len( badLFNs ), job ) )
      param = '\n'.join( badLFNs )
      self.log.info( param )
      result = self.setJobParam( job, self.am_getModuleParam( 'optimizerName' ), param )
      if not result['OK']:
        self.log.error( result['Message'] )
      return S_ERROR( 'Input Data Not Available' )

    return self.__getSiteCandidates( replicaDict['Successful'] )

  #############################################################################
  def __checkActiveSEs( self, job, replicaDict ):
    """
    Check active SE and replicas and identify possible Site candidates for 
    the execution of the job
    """
    # Now let's check if some replicas might not be available due to banned SE's
    activeReplicas = self.dataManager.checkActiveReplicas( replicaDict )
    if not activeReplicas['OK']:
      # due to banned SE's input data might no be available
      msg = "On Hold: Missing replicas due to banned SE"
      self.log.info( msg )
      self.log.warn( activeReplicas['Message'] )
      return S_ERROR( msg )

    activeReplicaDict = activeReplicas['Value']

    siteCandidates = self.__checkReplicas( job, activeReplicaDict )

    if not siteCandidates['OK']:
      # due to a banned SE's input data is not available at a single site      
      msg = "On Hold: Input data not Available due to banned SE"
      self.log.info( msg )
      self.log.warn( siteCandidates['Message'] )
      return S_ERROR( msg )

    resolvedData = {}
    resolvedData['Value'] = activeReplicas
    resolvedData['SiteCandidates'] = siteCandidates['Value']
    result = self.setOptimizerJobInfo( job, self.am_getModuleParam( 'optimizerName' ), resolvedData )
    if not result['OK']:
      self.log.warn( result['Message'] )
      return result
    return S_OK( resolvedData )


  #############################################################################
  def __getSitesForSE( self, se ):
    """ Returns a list of sites having the given SE as a local one.
        Uses the local cache of the site-se information
    """

    # Empty the cache if too old
    if ( time.time() - self.lastCScheck ) > self.cacheLength:
      self.log.verbose( 'Resetting the SE to site mapping cache' )
      self.seToSiteMapping = {}
      self.lastCScheck = time.time()

    if se not in self.seToSiteMapping:
      sites = getSitesForSE( se )
      if sites['OK']:
        self.seToSiteMapping[se] = list( sites['Value'] )
      return sites
    else:
      return S_OK( self.seToSiteMapping[se] )

  #############################################################################
  def __getSiteCandidates( self, inputData ):
    """This method returns a list of possible site candidates based on the
       job input data requirement.  For each site candidate, the number of files
       on disk and tape is resolved.
    """

    fileSEs = {}
    for lfn, replicas in inputData.items():
      siteList = []
      for se in replicas.keys():
        sites = self.__getSitesForSE( se )
        if sites['OK']:
          siteList += sites['Value']
      fileSEs[lfn] = uniqueElements( siteList )

    siteCandidates = []
    i = 0
    for _fileName, sites in fileSEs.items():
      if not i:
        siteCandidates = sites
      else:
        tempSite = []
        for site in siteCandidates:
          if site in sites:
            tempSite.append( site )
        siteCandidates = tempSite
      i += 1

    if not len( siteCandidates ):
      return S_ERROR( 'No candidate sites available' )

    #In addition, check number of files on tape and disk for each site
    #for optimizations during scheduling
    siteResult = {}
    for site in siteCandidates:
      siteResult[site] = { 'disk': [], 'tape': [] }

    seDict = {}
    for lfn, replicas in inputData.items():
      for se in replicas.keys():
        if se not in seDict:
          sites = self.__getSitesForSE( se )
          if not sites['OK']:
            continue
          try:
            #storageElement = StorageElement( se )
            result = self.resourceStatus.getStorageStatus( se, statusType = 'ReadAccess' )
            if not result['OK']:
              continue
            seDict[se] = { 'Sites': sites['Value'], 'SEParams': result['Value'][se] }
            result = self.resourcesHelper.getStorageElementOptionsDict( se )
            if not result['OK']:
              continue
            seDict[se]['SEParams'].update(result['Value'])
          except Exception:
            self.log.exception( 'Failed to instantiate StorageElement( %s )' % se )
            continue
        for site in seDict[se]['Sites']:
          if site in siteCandidates:
            if seDict[se]['SEParams']['ReadAccess'] and seDict[se]['SEParams']['DiskSE']:
              if lfn not in siteResult[site]['disk']:
                siteResult[site]['disk'].append( lfn )
                if lfn in siteResult[site]['tape']:
                  siteResult[site]['tape'].remove( lfn )
            if seDict[se]['SEParams']['ReadAccess'] and seDict[se]['SEParams']['TapeSE']:
              if lfn not in siteResult[site]['tape'] and lfn not in siteResult[site]['disk']:
                siteResult[site]['tape'].append( lfn )

    for site in siteResult:
      siteResult[site]['disk'] = len( siteResult[site]['disk'] )
      siteResult[site]['tape'] = len( siteResult[site]['tape'] )
    return S_OK( siteResult )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
