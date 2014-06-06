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
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor  import OptimizerExecutor
from DIRAC.Resources.Storage.StorageElement                          import StorageElement
from DIRAC                                                           import S_OK, S_ERROR
from DIRAC.Core.Utilities                                            import DictCache
from DIRAC.DataManagementSystem.Client.ReplicaManager                import ReplicaManager


class InputDataResolution( OptimizerExecutor ):
  """
      The specific Optimizer must provide the following methods:
      - initializeOptimizer() before each execution cycle
      - checkJob() - the main method called for each job
  """

  @classmethod
  def initializeOptimizer( cls ):
    """Initialize specific parameters
    """
    cls.ex_setProperty( 'shifterProxy', 'DataManager' )
    cls.__SEStatus = DictCache.DictCache()

    try:
      cls.__replicaMan = ReplicaManager()
    except Exception, e:
      msg = 'Failed to create ReplicaManager'
      cls.log.exception( msg )
      return S_ERROR( msg + str( e ) )

    cls.ex_setOption( "FailedStatus", "Input Data Not Available" )
    return S_OK()

  def optimizeJob( self, jid, jobState ):
    result = self.doTheThing( jobState )
    if not result[ 'OK' ]:
      self.jobLog.error( result[ 'Message' ] )
      jobState.setAppStatus( result[ 'Message' ] )
      return S_ERROR( self.ex_getOption( "FailedJobStatus", "Input Data Not Available" ) )
    #Reset the stage info
    return jobState.setOptParameter( "StageRequestedForSites", "" )

  def doTheThing( self, jobState ):
    result = jobState.getManifest()
    if not result[ 'OK' ]:
      self.jobLog.notice( "Can't retrieve manifest. %s" % result[ 'Message' ] )
      return result
    jobManifest = result[ 'Value' ]
    inputData = jobManifest.getOption( 'InputData', [] )
    if not inputData:
      self.jobLog.notice( "No input data. Skipping." )
      return self.setNextOptimizer()

    result = jobState.getInputData()
    if result[ 'OK' ] and result[ 'Value' ]:
      self.jobLog.notice( "Already resolved input data, skipping" )
      return self.setNextOptimizer()

    #Sanitize
    lfns = []
    for lfn in inputData:
      if lfn[:4].lower() == "lfn:":
        lfns.append( lfn[4:] )
      else:
        lfns.append( lfn )


    startTime = time.time()
    result = self.__replicaMan.getReplicas( lfns )
    self.jobLog.info( 'Catalog replicas lookup time: %.2f seconds ' % ( time.time() - startTime ) )
    if not result['OK']:
      self.log.warn( result['Message'] )
      return result

    replicaDict = result['Value']
    result = self.__checkReplicas( replicaDict )
    if not result['OK']:
      self.jobLog.error( result['Message'] )
      return result
    lfnData = result[ 'Value' ]

    if self.ex_getOption( 'CheckFileMetadata', True ):
      result = self.__getMetadata( lfnData )
      if not result[ 'OK' ]:
        return result
      lfnData = result[ 'Value' ]

    result = self.__markDiskReplicas( lfnData )
    if not result[ 'OK' ]:
      return result
    lfnData = result[ 'Value' ]

    result = jobState.setInputData( lfnData )
    if not result[ 'OK' ]:
      return result
    return self.setNextOptimizer()

  def __checkReplicas( self, replicaDict ):
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
      return S_ERROR( 'Input data not available' )

    lfnData = {}
    for lfn in okReplicas:
      replicas = okReplicas[ lfn ]
      lfnData[ lfn ] = { 'Replicas' : {} , 'Metadata' : {} }
      lfnDataReplicas = lfnData[ lfn ][ 'Replicas' ]
      for seName in replicas:
        lfnDataReplicas[ seName ] = { 'SURL' : replicas[ seName ], 'Disk' : False }
    return S_OK( lfnData )


  def __getMetadata( self, lfnData ):
      startTime = time.time()
      result = self.__replicaMan.getCatalogFileMetadata( lfnData.keys() )
      self.jobLog.info( 'Catalog Metadata Lookup Time: %.2f seconds ' % ( time.time() - startTime ) )

      if not result['OK']:
        self.jobLog.error( result['Message'] )
        return S_ERROR( 'Failed to retrieve Input Data metadata' )

      failed = result['Value']['Failed']
      if failed:
        self.jobLog.error( 'Failed to establish some GUIDs' )
        self.jobLog.error( result['Message'] )
        return S_ERROR( 'Failed to retrieve Input Data metadata' )

      metadataDict = result[ 'Value' ][ 'Successful' ]
      for lfn in metadataDict:
        lfnData[ lfn ][ 'Metadata' ] = metadataDict[ lfn ]
      return S_OK( lfnData )

  def __getSEStatus( self, seName ):
    result = self.__SEStatus.get( seName )
    if result == False:
      seObj = StorageElement( seName )
      result = seObj.getStatus()
      if not result[ 'OK' ]:
        return result
      self.__SEStatus.add( seName, 600, result )
    return result

  def __markDiskReplicas( self, lfnData ):
    """This method returns a list of possible site candidates based on the
       job input data requirement.  For each site candidate, the number of files
       on disk and tape is resolved.
    """

    for lfn in lfnData:
      replicas = lfnData[ lfn ][ 'Replicas' ]
      #Check each SE in the replicas
      for seName in replicas:
        result = self.__getSEStatus( seName )
        if not result[ 'OK' ]:
          #TODO: Maybe raise an exception to rerun job?
          return result
        seStatus = result[ 'Value' ]
        if seStatus[ 'Read' ] and seStatus[ 'DiskSE' ]:
          replicas[ seName ][ 'Disk' ] = True

    return S_OK( lfnData )

