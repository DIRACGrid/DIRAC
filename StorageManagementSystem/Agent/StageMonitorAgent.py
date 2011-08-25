# $Header: /tmp/libdirac/tmp.FKduyw2449/dirac/DIRAC3/DIRAC/StorageManagementSystem/Agent/StageMonitor.py,v 1.2 2009/10/30 22:03:03 acsmith Exp $
__RCSID__ = "$Id$"

from DIRAC import gLogger, gConfig, gMonitor, S_OK, S_ERROR, rootPath

from DIRAC.Core.Base.AgentModule                                  import AgentModule
from DIRAC.StorageManagementSystem.Client.StorageManagerClient    import StorageManagerClient
from DIRAC.DataManagementSystem.Client.DataIntegrityClient        import DataIntegrityClient
from DIRAC.DataManagementSystem.Client.ReplicaManager             import ReplicaManager
from DIRAC.StorageManagementSystem.DB.StorageManagementDB       import StorageManagementDB

import time, os, sys, re
from types import *

AGENT_NAME = 'StorageManagement/StageMonitorAgent'

class StageMonitorAgent( AgentModule ):

  def initialize( self ):
    self.replicaManager = ReplicaManager()
    #self.stagerClient = StorageManagerClient()
    self.dataIntegrityClient = DataIntegrityClient()
    self.storageDB = StorageManagementDB()
    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    return S_OK()

  def execute( self ):

    res = self.monitorStageRequests()
    return res

  def monitorStageRequests( self ):
    """ This is the third logical task manages the StageSubmitted->Staged transition of the Replicas
    """
    res = self.__getStageSubmittedReplicas()
    if not res['OK']:
      gLogger.fatal( "StageMonitor.monitorStageRequests: Failed to get replicas from StorageManagementDB.", res['Message'] )
      return res
    if not res['Value']:
      gLogger.info( "StageMonitor.monitorStageRequests: There were no StageSubmitted replicas found" )
      return res
    seReplicas = res['Value']['SEReplicas']
    replicaIDs = res['Value']['ReplicaIDs']
    gLogger.info( "StageMonitor.monitorStageRequests: Obtained %s StageSubmitted replicas for monitoring." % len( replicaIDs ) )
    for storageElement, seReplicaIDs in seReplicas.items():
      self.__monitorStorageElementStageRequests( storageElement, seReplicaIDs, replicaIDs )
    return S_OK()

  def __monitorStorageElementStageRequests( self, storageElement, seReplicaIDs, replicaIDs ):
    terminalReplicaIDs = {}
    oldRequests = []
    stagedReplicas = []
    pfnRepIDs = {}
    pfnReqIDs = {}
    for replicaID in seReplicaIDs:
      pfn = replicaIDs[replicaID]['PFN']
      pfnRepIDs[pfn] = replicaID
      requestID = replicaIDs[replicaID].get( 'RequestID', None )
      if requestID:
        pfnReqIDs[pfn] = replicaIDs[replicaID]['RequestID']

    gLogger.info( "StageMonitor.__monitorStorageElementStageRequests: Monitoring %s stage requests for %s." % ( len( pfnRepIDs ), storageElement ) )
    res = self.replicaManager.getStorageFileMetadata( pfnReqIDs.keys(), storageElement )
    if not res['OK']:
      gLogger.error( "StageMonitor.__monitorStorageElementStageRequests: Completely failed to monitor stage requests for replicas.", res['Message'] )
      return
    prestageStatus = res['Value']
    for pfn, reason in prestageStatus['Failed'].items():
      if re.search( 'File does not exist', reason ):
        gLogger.error( "StageMonitor.__monitorStorageElementStageRequests: PFN did not exist in the StorageElement", pfn )
        terminalReplicaIDs[pfnRepIDs[pfn]] = 'PFN did not exist in the StorageElement'
    for pfn, staged in prestageStatus['Successful'].items():
      if staged and 'Cached' in staged and staged['Cached']:
        stagedReplicas.append( pfnRepIDs[pfn] )
      if staged and 'Cached' in staged and not staged['Cached']:
        oldRequests.append( pfnRepIDs[pfn] ); #only ReplicaIDs
    #here take the old requests which are still not Cached should be removed, new ones submitted

    # Update the states of the replicas in the database
    if terminalReplicaIDs:
      gLogger.info( "StageMonitor.__monitorStorageElementStageRequests: %s replicas are terminally failed." % len( terminalReplicaIDs ) )
      res = self.storageDB.updateReplicaFailure( terminalReplicaIDs )
      if not res['OK']:
        gLogger.error( "StageMonitor.__monitorStorageElementStageRequests: Failed to update replica failures.", res['Message'] )
    if stagedReplicas:
      gLogger.info( "StageMonitor.__monitorStorageElementStageRequests: %s staged replicas to be updated." % len( stagedReplicas ) )
      res = self.storageDB.setStageComplete( stagedReplicas )
      if not res['OK']:
        gLogger.error( "StageMonitor.__monitorStorageElementStageRequests: Failed to updated staged replicas.", res['Message'] )
      res = self.storageDB.updateReplicaStatus( stagedReplicas, 'Staged' )
      if not res['OK']:
        gLogger.error( "StageRequest.__monitorStorageElementStageRequests: Failed to insert replica status.", res['Message'] )
    if oldRequests:
      gLogger.info( "StageMonitor.__monitorStorageElementStageRequests: %s old requests will be retried." % len( oldRequests ) )
      res = self.__wakeupOldRequests( oldRequests )
      if not res['OK']:
        gLogger.error( "StageMonitor.__monitorStorageElementStageRequests: Failed to wakeup old requests.", res['Message'] )
    return

  def __getStageSubmittedReplicas( self ):
    """ This obtains the StageSubmitted replicas from the Replicas table and the RequestID from the StageRequests table """
    res = self.storageDB.getCacheReplicas( {'Status':'StageSubmitted'} )
    if not res['OK']:
      gLogger.error( "StageRequest.__getStageSubmittedReplicas: Failed to get replicas with StageSubmitted status.", res['Message'] )
      return res
    if not res['Value']:
      gLogger.debug( "StageRequest.__getStageSubmittedReplicas: No StageSubmitted replicas found to process." )
      return S_OK()
    else:
      gLogger.debug( "StageRequest.__getStageSubmittedReplicas: Obtained %s StageSubmitted replicas(s) to process." % len( res['Value'] ) )

    seReplicas = {}
    replicaIDs = res['Value']
    for replicaID, info in replicaIDs.items():
      storageElement = info['SE']
      if not seReplicas.has_key( storageElement ):
        seReplicas[storageElement] = []
      seReplicas[storageElement].append( replicaID )

    # RequestID was missing from replicaIDs dictionary BUGGY?
    res = self.storageDB.getStageRequests( {'ReplicaID':replicaIDs.keys()} )
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR( 'Could not obtain request IDs for replicas %s from StageRequests table' % ( replicaIDs.keys() ) )

    for replicaID, info in res['Value'].items():
      reqID = info['RequestID']
      replicaIDs[replicaID]['RequestID'] = reqID

    return S_OK( {'SEReplicas':seReplicas, 'ReplicaIDs':replicaIDs} )

  def __reportProblematicFiles( self, lfns, reason ):
    return S_OK()
    res = self.dataIntegrityClient.setFileProblematic( lfns, reason, self.name )
    if not res['OK']:
      gLogger.error( "RequestPreparation.__reportProblematicFiles: Failed to report missing files.", res['Message'] )
      return res
    if res['Value']['Successful']:
      gLogger.info( "RequestPreparation.__reportProblematicFiles: Successfully reported %s missing files." % len( res['Value']['Successful'] ) )
    if res['Value']['Failed']:
      gLogger.info( "RequestPreparation.__reportProblematicFiles: Failed to report %s problematic files." % len( res['Value']['Failed'] ) )
    return res

  def __wakeupOldRequests( self, oldRequests ):
    res = self.storageDB.wakeupOldRequests( oldRequests )
    if not res['OK']:
      gLogger.error( "StageMonitor.__wakeupOldRequests: Failed to resubmit old requests.", res['Message'] )
      return res
    return S_OK()


