__RCSID__ = "$Id$"

from DIRAC import gLogger, gConfig, S_OK

from DIRAC.Core.Base.AgentModule                                  import AgentModule
from DIRAC.StorageManagementSystem.Client.StorageManagerClient    import StorageManagerClient
from DIRAC.Core.Utilities.List                                    import sortList
from DIRAC.Resources.Storage.StorageElement                       import StorageElement
from DIRAC.StorageManagementSystem.DB.StorageManagementDB         import THROTTLING_STEPS, THROTTLING_TIME

import re

AGENT_NAME = 'StorageManagement/StageRequestAgent'

class StageRequestAgent( AgentModule ):

  def initialize( self ):
    self.stagerClient = StorageManagerClient()
    #self.storageDB = StorageManagementDB()
    # pin lifetime = 1 day
    self.pinLifetime = self.am_getOption( 'PinLifetime', THROTTLING_TIME )

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    return S_OK()

  def execute( self ):

    # Get the current submitted stage space and the amount of pinned space for each storage element
    res = self.getStorageUsage()
    if not res['OK']:
      return res

    return self.submitStageRequests()

  def getStorageUsage( self ):
    """ Fill the current Status of the SE Caches from the DB
    """
    self.storageElementCache = {}

    res = self.stagerClient.getSubmittedStagePins()
    if not res['OK']:
      gLogger.fatal( "StageRequest.getStorageUsage: Failed to obtain submitted requests from StorageManagementDB.", res['Message'] )
      return res
    self.storageElementUsage = res['Value']
    if self.storageElementUsage:
      gLogger.info( "StageRequest.getStorageUsage: Active stage/pin requests found at the following sites:" )
      for storageElement in sortList( self.storageElementUsage.keys() ):
        seDict = self.storageElementUsage[storageElement]
        # Convert to GB for printout
        seDict['TotalSize'] = seDict['TotalSize'] / ( 1000 * 1000 * 1000.0 )
        gLogger.info( "StageRequest.getStorageUsage: %s: %s replicas with a size of %.3f GB." %
                      ( storageElement.ljust( 15 ), str( seDict['Replicas'] ).rjust( 6 ), seDict['TotalSize'] ) )
    if not self.storageElementUsage:
      gLogger.info( "StageRequest.getStorageUsage: No active stage/pin requests found." )

    return S_OK()


  def submitStageRequests( self ):
    """ This manages the following transitions of the Replicas
        * Waiting -> Offline (if the file is not found Cached)
        * Waiting -> StageSubmitted (if the file is found Cached)
        * Offline -> StageSubmitted (if there are not more Waiting replicas)
    """
    # Retry Replicas that have not been Staged in a previous attempt 
    res = self._getMissingReplicas()
    if not res['OK']:
      gLogger.fatal( "StageRequest.submitStageRequests: Failed to get replicas from StorageManagementDB.", res['Message'] )
      return res
    seReplicas = res['Value']['SEReplicas']
    allReplicaInfo = res['Value']['AllReplicaInfo']

    if seReplicas:
      gLogger.info( "StageRequest.submitStageRequests: Completing partially Staged Tasks" )
    for storageElement, seReplicaIDs in seReplicas.items():
      gLogger.debug( 'Staging at %s:' % storageElement, seReplicaIDs )
      self._issuePrestageRequests( storageElement, seReplicaIDs, allReplicaInfo )

    # Check Waiting Replicas and select those found Online and all other Replicas from the same Tasks
    res = self._getOnlineReplicas()
    if not res['OK']:
      gLogger.fatal( "StageRequest.submitStageRequests: Failed to get replicas from StorageManagementDB.", res['Message'] )
      return res
    seReplicas = res['Value']['SEReplicas']
    allReplicaInfo = res['Value']['AllReplicaInfo']

    # Check Offline Replicas that fit in the Cache and all other Replicas from the same Tasks
    res = self._getOfflineReplicas()

    if not res['OK']:
      gLogger.fatal( "StageRequest.submitStageRequests: Failed to get replicas from StorageManagementDB.", res['Message'] )
      return res

    # Merge info from both results
    for storageElement, seReplicaIDs in res['Value']['SEReplicas'].items():
      if storageElement not in seReplicas:
        seReplicas[storageElement] = seReplicaIDs
      else:
        for replicaID in seReplicaIDs:
          if replicaID not in seReplicas[storageElement]:
            seReplicas[storageElement].append( replicaID )
    allReplicaInfo.update( res['Value']['AllReplicaInfo'] )

    gLogger.info( "StageRequest.submitStageRequests: Obtained %s replicas for staging." % len( allReplicaInfo ) )
    for storageElement, seReplicaIDs in seReplicas.items():
      gLogger.debug( 'Staging at %s:' % storageElement, seReplicaIDs )
      self._issuePrestageRequests( storageElement, seReplicaIDs, allReplicaInfo )
    return S_OK()

  def _getMissingReplicas( self ):
    """ This recovers Replicas that were not Staged on a previous attempt (the stage request failed or timed out),
        while other Replicas of the same task are already Staged. If left behind they can produce a deadlock.
        All SEs are considered, even if their Cache is full
    """
    # Get Replicas that are in Staged/StageSubmitted 
    gLogger.info( 'StageRequest._getMissingReplicas: Checking Staged Replicas' )

    res = self.__getStagedReplicas()
    if not res['OK']:
      gLogger.fatal( "StageRequest._getMissingReplicas: Failed to get replicas from StorageManagementDB.", res['Message'] )
      return res
    seReplicas = {}

    allReplicaInfo = res['Value']['AllReplicaInfo']
    replicasToStage = []
    for _storageElement, seReplicaIDs in res['Value']['SEReplicas'].items():
      # Consider all SEs
      replicasToStage.extend( seReplicaIDs )

    # Get Replicas from the same Tasks as those selected
    res = self.__addAssociatedReplicas( replicasToStage, seReplicas, allReplicaInfo )
    if not res['OK']:
      gLogger.fatal( "StageRequest._getMissingReplicas: Failed to get associated Replicas.", res['Message'] )

    return res

  def _getOnlineReplicas( self ):
    """ This manages the transition
        * Waiting -> Offline (if the file is not found Cached)
        and returns the list of Cached Replicas for which the pin time has to be extended
        SEs for which the cache is currently full are not considered
    """
    # Get all Replicas in Waiting Status associated to Staging Tasks
    gLogger.verbose( 'StageRequest._getOnlineReplicas: Checking Online Replicas to be handled' )

    res = self.__getWaitingReplicas()
    if not res['OK']:
      gLogger.fatal( "StageRequest._getOnlineReplicas: Failed to get replicas from StorageManagementDB.", res['Message'] )
      return res
    seReplicas = {}
    allReplicaInfo = res['Value']['AllReplicaInfo']
    if not len( allReplicaInfo ):
      gLogger.info( "StageRequest._getOnlineReplicas: There were no Waiting replicas found" )
      return res
    gLogger.info( "StageRequest._getOnlineReplicas: Obtained %s replicas Waiting for staging." % len( allReplicaInfo ) )
    replicasToStage = []
    for storageElement, seReplicaIDs in res['Value']['SEReplicas'].items():
      if not self.__usage( storageElement ) < self.__cache( storageElement ):
        gLogger.info( 'StageRequest._getOnlineReplicas: Skipping %s, current usage above limit ( %s GB )' % ( storageElement, self.__cache( storageElement ) ) )
        # Do not consider those SE that have the Cache full
        continue
      # Check if the Replica Metadata is OK and find out if they are Online or Offline
      res = self.__checkIntegrity( storageElement, seReplicaIDs, allReplicaInfo )
      if not res['OK']:
        gLogger.error( 'StageRequest._getOnlineReplicas: Failed to check Replica Metadata', '(%s): %s' % ( storageElement, res['Message'] ) )
      else:
        # keep only Online Replicas
        seReplicas[storageElement] = res['Value']['Online']
        replicasToStage.extend( res['Value']['Online'] )

    # Get Replicas from the same Tasks as those selected
    res = self.__addAssociatedReplicas( replicasToStage, seReplicas, allReplicaInfo )
    if not res['OK']:
      gLogger.fatal( "StageRequest._getOnlineReplicas: Failed to get associated Replicas.", res['Message'] )

    return res

  def _getOfflineReplicas( self ):
    """ This checks Replicas in Offline status
        and returns the list of Replicas to be Staged
        SEs for which the cache is currently full are not considered
    """
    # Get all Replicas in Waiting Status associated to Staging Tasks
    gLogger.verbose( 'StageRequest._getOfflineReplicas: Checking Offline Replicas to be handled' )

    res = self.__getOfflineReplicas()
    if not res['OK']:
      gLogger.fatal( "StageRequest._getOfflineReplicas: Failed to get replicas from StorageManagementDB.", res['Message'] )
      return res
    seReplicas = {}
    allReplicaInfo = res['Value']['AllReplicaInfo']
    if not len( allReplicaInfo ):
      gLogger.info( "StageRequest._getOfflineReplicas: There were no Offline replicas found" )
      return res
    gLogger.info( "StageRequest._getOfflineReplicas: Obtained %s replicas Offline for staging." % len( allReplicaInfo ) )
    replicasToStage = []

    for storageElement, seReplicaIDs in res['Value']['SEReplicas'].items():
      if not self.__usage( storageElement ) < self.__cache( storageElement ):
        gLogger.info( 'StageRequest._getOfflineReplicas: Skipping %s, current usage above limit ( %s GB )' % ( storageElement, self.__cache( storageElement ) ) )
        # Do not consider those SE that have the Cache full
        continue
      seReplicas[storageElement] = []
      for replicaID in sorted( seReplicaIDs ):
        seReplicas[storageElement].append( replicaID )
        replicasToStage.append( replicaID )
        self.__add( storageElement, allReplicaInfo[replicaID]['Size'] )
        if not self.__usage( storageElement ) < self.__cache( storageElement ):
          # Stop adding Replicas when the cache is full
          break

    # Get Replicas from the same Tasks as those selected
    res = self.__addAssociatedReplicas( replicasToStage, seReplicas, allReplicaInfo )
    if not res['OK']:
      gLogger.fatal( "StageRequest._getOfflineReplicas: Failed to get associated Replicas.", res['Message'] )

    return res

  def __usage( self, storageElement ):
    """ Retrieve current usage of SE
    """
    if not storageElement in self.storageElementUsage:
      self.storageElementUsage[storageElement] = {'TotalSize': 0.}
    return self.storageElementUsage[storageElement]['TotalSize']

  def __cache( self, storageElement ):
    """ Retrieve cache size for SE
    """
    if not storageElement in self.storageElementCache:
      self.storageElementCache[storageElement] = gConfig.getValue( "/Resources/StorageElements/%s/DiskCacheTB" % storageElement, 1. ) * 1000. / THROTTLING_STEPS
    return self.storageElementCache[storageElement]

  def __add( self, storageElement, size ):
    """ Add size (in bytes) to current usage of storageElement (in GB)
    """
    if not storageElement in self.storageElementUsage:
      self.storageElementUsage[storageElement] = {'TotalSize': 0.}
    size = size / ( 1000 * 1000 * 1000.0 )
    self.storageElementUsage[storageElement]['TotalSize'] += size
    return size

  def _issuePrestageRequests( self, storageElement, seReplicaIDs, allReplicaInfo ):
    """ Make the request to the SE and update the DB
    """
    pfnRepIDs = {}
    for replicaID in seReplicaIDs:
      pfn = allReplicaInfo[replicaID]['PFN']
      pfnRepIDs[pfn] = replicaID

    # Now issue the prestage requests for the remaining replicas
    stageRequestMetadata = {}
    updatedPfnIDs = []
    if pfnRepIDs:
      gLogger.info( "StageRequest._issuePrestageRequests: Submitting %s stage requests for %s." % ( len( pfnRepIDs ), storageElement ) )
      res = StorageElement( storageElement ).prestageFile( pfnRepIDs, lifetime = self.pinLifetime )
      gLogger.debug( "StageRequest._issuePrestageRequests: StorageElement.prestageStorageFile: res=", res )
      #Daniela: fishy result from ReplicaManager!!! Should NOT return OK
      #res= {'OK': True, 'Value': {'Successful': {}, 'Failed': {'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/data/2010/RAW/EXPRESS/LHCb/COLLISION10/71476/071476_0000000241.raw': ' SRM2Storage.__gfal_exec: Failed to perform gfal_prestage.[SE][BringOnline][SRM_INVALID_REQUEST] httpg://srm-lhcb.cern.ch:8443/srm/managerv2: User not able to access specified space token\n'}}}
      #res= {'OK': True, 'Value': {'Successful': {'srm://gridka-dCache.fzk.de/pnfs/gridka.de/lhcb/data/2009/RAW/FULL/LHCb/COLLISION09/63495/063495_0000000001.raw': '-2083846379'}, 'Failed': {}}}

      if not res['OK']:
        gLogger.error( "StageRequest._issuePrestageRequests: Completely failed to submit stage requests for replicas.", res['Message'] )
      else:
        for pfn, requestID in res['Value']['Successful'].items():
          if not stageRequestMetadata.has_key( requestID ):
            stageRequestMetadata[requestID] = []
          stageRequestMetadata[requestID].append( pfnRepIDs[pfn] )
          updatedPfnIDs.append( pfnRepIDs[pfn] )
    if stageRequestMetadata:
      gLogger.info( "StageRequest._issuePrestageRequests: %s stage request metadata to be updated." % len( stageRequestMetadata ) )
      res = self.stagerClient.insertStageRequest( stageRequestMetadata, self.pinLifetime )
      if not res['OK']:
        gLogger.error( "StageRequest._issuePrestageRequests: Failed to insert stage request metadata.", res['Message'] )
        return res
      res = self.stagerClient.updateReplicaStatus( updatedPfnIDs, 'StageSubmitted' )
      if not res['OK']:
        gLogger.error( "StageRequest._issuePrestageRequests: Failed to insert replica status.", res['Message'] )
    return

  def __sortBySE( self, replicaDict ):

    seReplicas = {}
    replicaIDs = {}
    for replicaID, info in replicaDict.items():
      lfn = info['LFN']
      storageElement = info['SE']
      size = info['Size']
      pfn = info['PFN']
      replicaIDs[replicaID] = {'LFN':lfn, 'PFN':pfn, 'Size':size, 'StorageElement':storageElement}
      if not seReplicas.has_key( storageElement ):
        seReplicas[storageElement] = []
      seReplicas[storageElement].append( replicaID )
    return S_OK( {'SEReplicas':seReplicas, 'AllReplicaInfo':replicaIDs} )

  def __getStagedReplicas( self ):
    """ This obtains the Staged replicas from the Replicas table and for each LFN the requested storage element """
    # First obtain the Waiting replicas from the Replicas table
    res = self.stagerClient.getStagedReplicas()
    if not res['OK']:
      gLogger.error( "StageRequest.__getStagedReplicas: Failed to get replicas with Waiting status.", res['Message'] )
      return res
    if not res['Value']:
      gLogger.debug( "StageRequest.__getStagedReplicas: No Waiting replicas found to process." )
    else:
      gLogger.debug( "StageRequest.__getStagedReplicas: Obtained %s Waiting replicas(s) to process." % len( res['Value'] ) )

    return self.__sortBySE( res['Value'] )

  def __getWaitingReplicas( self ):
    """ This obtains the Waiting replicas from the Replicas table and for each LFN the requested storage element """
    # First obtain the Waiting replicas from the Replicas table
    res = self.stagerClient.getWaitingReplicas()
    if not res['OK']:
      gLogger.error( "StageRequest.__getWaitingReplicas: Failed to get replicas with Waiting status.", res['Message'] )
      return res
    if not res['Value']:
      gLogger.debug( "StageRequest.__getWaitingReplicas: No Waiting replicas found to process." )
    else:
      gLogger.debug( "StageRequest.__getWaitingReplicas: Obtained %s Waiting replicas(s) to process." % len( res['Value'] ) )

    return self.__sortBySE( res['Value'] )

  def __getOfflineReplicas( self ):
    """ This obtains the Offline replicas from the Replicas table and for each LFN the requested storage element """
    # First obtain the Waiting replicas from the Replicas table
    res = self.stagerClient.getOfflineReplicas()
    if not res['OK']:
      gLogger.error( "StageRequest.__getOfflineReplicas: Failed to get replicas with Waiting status.", res['Message'] )
      return res
    if not res['Value']:
      gLogger.debug( "StageRequest.__getOfflineReplicas: No Waiting replicas found to process." )
    else:
      gLogger.debug( "StageRequest.__getOfflineReplicas: Obtained %s Waiting replicas(s) to process." % len( res['Value'] ) )

    return self.__sortBySE( res['Value'] )

  def __addAssociatedReplicas( self, replicasToStage, seReplicas, allReplicaInfo ):
    """ Retrieve the list of Replicas that belong to the same Tasks as the provided list
    """
    res = self.stagerClient.getAssociatedReplicas( replicasToStage )
    if not res['OK']:
      gLogger.fatal( "StageRequest.__addAssociatedReplicas: Failed to get associated Replicas.", res['Message'] )
      return res
    addReplicas = {'Offline': {}, 'Waiting': {}}
    replicaIDs = {}
    for replicaID, info in res['Value'].items():
      lfn = info['LFN']
      storageElement = info['SE']
      size = info['Size']
      pfn = info['PFN']
      status = info['Status']
      if status not in ['Waiting', 'Offline']:
        continue
      if not addReplicas[status].has_key( storageElement ):
        addReplicas[status][storageElement] = []
      replicaIDs[replicaID] = {'LFN':lfn, 'PFN':pfn, 'Size':size, 'StorageElement':storageElement }
      addReplicas[status][storageElement].append( replicaID )

    waitingReplicas = addReplicas['Waiting']
    offlineReplicas = addReplicas['Offline']
    newReplicaInfo = replicaIDs
    allReplicaInfo.update( newReplicaInfo )

    # First handle Waiting Replicas for which metadata is to be checked
    for storageElement, seReplicaIDs in waitingReplicas.items():
      for replicaID in list( seReplicaIDs ):
        if replicaID in replicasToStage:
          seReplicaIDs.remove( replicaID )
      res = self.__checkIntegrity( storageElement, seReplicaIDs, allReplicaInfo )
      if not res['OK']:
        gLogger.error( 'StageRequest.__addAssociatedReplicas: Failed to check Replica Metadata', '(%s): %s' % ( storageElement, res['Message'] ) )
      else:
        # keep all Replicas (Online and Offline)
        if not storageElement in seReplicas:
          seReplicas[storageElement] = []
        seReplicas[storageElement].extend( res['Value']['Online'] )
        replicasToStage.extend( res['Value']['Online'] )
        seReplicas[storageElement].extend( res['Value']['Offline'] )
        replicasToStage.extend( res['Value']['Offline'] )

    # Then handle Offline Replicas for which metadata is already checked
    for storageElement, seReplicaIDs in offlineReplicas.items():
      if not storageElement in seReplicas:
        seReplicas[storageElement] = []
      for replicaID in sorted( seReplicaIDs ):
        if replicaID in replicasToStage:
          seReplicaIDs.remove( replicaID )
      seReplicas[storageElement].extend( seReplicaIDs )
      replicasToStage.extend( seReplicaIDs )

    for replicaID in allReplicaInfo.keys():
      if replicaID not in replicasToStage:
        del allReplicaInfo[replicaID]

    totalSize = 0
    for storageElement in sorted( seReplicas.keys() ):
      replicaIDs = seReplicas[storageElement]
      size = 0
      for replicaID in replicaIDs:
        size += self.__add( storageElement, allReplicaInfo[replicaID]['Size'] )

      gLogger.info( 'StageRequest.__addAssociatedReplicas:  Considering %s GB to be staged at %s' % ( size, storageElement ) )
      totalSize += size

    gLogger.info( "StageRequest.__addAssociatedReplicas: Obtained %s GB for staging." % totalSize )

    return S_OK( {'SEReplicas':seReplicas, 'AllReplicaInfo':allReplicaInfo} )

  def __checkIntegrity( self, storageElement, seReplicaIDs, allReplicaInfo ):
    """ Check the integrity of the files to ensure they are available
        Updates status of Offline Replicas for a later pass
        Return list of Online replicas to be Stage
    """
    if not seReplicaIDs:
      return S_OK( {'Online': [], 'Offline': []} )

    pfnRepIDs = {}
    for replicaID in seReplicaIDs:
      pfn = allReplicaInfo[replicaID]['PFN']
      pfnRepIDs[pfn] = replicaID

    gLogger.info( "StageRequest.__checkIntegrity: Checking the integrity of %s replicas at %s." % ( len( pfnRepIDs ), storageElement ) )
    res = StorageElement( storageElement ).getFileMetadata( pfnRepIDs )
    if not res['OK']:
      gLogger.error( "StageRequest.__checkIntegrity: Completely failed to obtain metadata for replicas.", res['Message'] )
      return res

    terminalReplicaIDs = {}
    onlineReplicaIDs = []
    offlineReplicaIDs = []
    for pfn, metadata in res['Value']['Successful'].items():

      if metadata['Size'] != allReplicaInfo[pfnRepIDs[pfn]]['Size']:
        gLogger.error( "StageRequest.__checkIntegrity: PFN StorageElement size does not match FileCatalog", pfn )
        terminalReplicaIDs[pfnRepIDs[pfn]] = 'PFN StorageElement size does not match FileCatalog'
        pfnRepIDs.pop( pfn )
      elif metadata['Lost']:
        gLogger.error( "StageRequest.__checkIntegrity: PFN has been Lost by the StorageElement", pfn )
        terminalReplicaIDs[pfnRepIDs[pfn]] = 'PFN has been Lost by the StorageElement'
        pfnRepIDs.pop( pfn )
      elif metadata['Unavailable']:
        gLogger.error( "StageRequest.__checkIntegrity: PFN is declared Unavailable by the StorageElement", pfn )
        terminalReplicaIDs[pfnRepIDs[pfn]] = 'PFN is declared Unavailable by the StorageElement'
        pfnRepIDs.pop( pfn )
      else:
        if metadata['Cached']:
          gLogger.verbose( "StageRequest.__checkIntegrity: Cache hit for file." )
          onlineReplicaIDs.append( pfnRepIDs[pfn] )
        else:
          offlineReplicaIDs.append( pfnRepIDs[pfn] )

    for pfn, reason in res['Value']['Failed'].items():
      if re.search( 'File does not exist', reason ):
        gLogger.error( "StageRequest.__checkIntegrity: PFN does not exist in the StorageElement", pfn )
        terminalReplicaIDs[pfnRepIDs[pfn]] = 'PFN does not exist in the StorageElement'
      pfnRepIDs.pop( pfn )

    # Update the states of the replicas in the database #TODO Sent status to integrity DB
    if terminalReplicaIDs:
      gLogger.info( "StageRequest.__checkIntegrity: %s replicas are terminally failed." % len( terminalReplicaIDs ) )
      res = self.stagerClient.updateReplicaFailure( terminalReplicaIDs )
      if not res['OK']:
        gLogger.error( "StageRequest.__checkIntegrity: Failed to update replica failures.", res['Message'] )
    if onlineReplicaIDs:
      gLogger.info( "StageRequest.__checkIntegrity: %s replicas found Online." % len( onlineReplicaIDs ) )
    if offlineReplicaIDs:
      gLogger.info( "StageRequest.__checkIntegrity: %s replicas found Offline." % len( offlineReplicaIDs ) )
      res = self.stagerClient.updateReplicaStatus( offlineReplicaIDs, 'Offline' )
    return S_OK( {'Online': onlineReplicaIDs, 'Offline': offlineReplicaIDs} )
