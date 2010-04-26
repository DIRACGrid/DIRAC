# $HeadURL: /tmp/libdirac/tmp.FKduyw2449/dirac/DIRAC3/DIRAC/StorageManagementSystem/Agent/StageRequest.py,v 1.2 2009/10/30 22:03:03 acsmith Exp $
__RCSID__ = "$Id: StageRequest.py,v 1.2 2009/10/30 22:03:03 acsmith Exp $"

from DIRAC import gLogger, gConfig, gMonitor, S_OK, S_ERROR, rootPath

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import sortList

from DIRAC.DataManagementSystem.Client.DataIntegrityClient import DataIntegrityClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

import time,os,sys,re
from types import *

AGENT_NAME = 'StorageManagement/StageRequestAgent'

class StageRequestAgent(AgentModule):

  def initialize(self):
    self.replicaManager = ReplicaManager()
    self.stagerClient = RPCClient('StorageManagement/StorageManagerHandler')
    self.dataIntegrityClient = DataIntegrityClient()
    
    proxyLocation = self.am_getOption('ProxyLocation', '' )
    if not proxyLocation:
      proxyLocation = False
    self.am_setModuleParam('shifterProxy','DataManager')
    self.am_setModuleParam('shifterProxyLocation',proxyLocation)
    return S_OK()

  def execute(self):

    # Get the current submitted stage space and the amount of pinned space for each storage element
    res = self.stagerClient.getSubmittedStagePins()
    if not res['OK']:
      gLogger.fatal("StageRequest.submitStageRequests: Failed to obtain submitted requests from StagerDB.",res['Message'])
      return res
    self.storageElementUsage = res['Value']
    if self.storageElementUsage:
      gLogger.info("StageRequest.execute: Active stage/pin requests found at the following sites:")
      for storageElement in sortList(self.storageElementUsage.keys()):
        seDict = self.storageElementUsage[storageElement]
        gLogger.info("StageRequest.execute: %s: %s replicas with a size of %.2f TB." % (storageElement.ljust(15), str(seDict['Replicas']).rjust(6), seDict['TotalSize']/(1000*1000*1000*1000.0)))
    res = self.submitStageRequests()
    return res

  def submitStageRequests(self):
    """ This manages the Waiting->StageSubmitted transition of the Replicas
    """
    res = self.__getWaitingReplicas()
    if not res['OK']:
      gLogger.fatal("StageRequest.submitStageRequests: Failed to get replicas from StagerDB.",res['Message'])
      return res
    if not res['Value']:
      gLogger.info("StageRequest.submitStageRequests: There were no Waiting replicas found")
      return res
    seReplicas = res['Value']['SEReplicas']
    allReplicaInfo = res['Value']['ReplicaIDs']
    gLogger.info("StageRequest.submitStageRequests: Obtained %s replicas Waiting for staging." % len(allReplicaInfo))
    for storageElement,seReplicaIDs in seReplicas.items():
      self.__issuePrestageRequests(storageElement,seReplicaIDs,allReplicaInfo)
    return S_OK()

  def __issuePrestageRequests(self,storageElement,seReplicaIDs,allReplicaInfo):
    # First select which files can be eligible for prestaging based on the available space
    usedSpace = 0
    if self.storageElementUsage.has_key(storageElement):
      usedSpace = self.storageElementUsage[storageElement]['TotalSize']
    totalSpace = gConfig.getValue("/Resources/StorageElements/%s/CacheSize" % storageElement,0)
    if not totalSpace:
      gLogger.info("StageRequest__issuePrestageRequests: No space restriction at %s" % (storageElement))
      selectedReplicaIDs = seReplicaIDs
    elif (totalSpace > usedSpace):
      gLogger.info("StageRequest__issuePrestageRequests: %.2f GB available at %s" % ((totalSpace-usedSpace)/(1000*1000*1000.0),storageElement))
      selectedReplicaIDs = []
      for replicaID in seReplicaIDs:
        if totalSpace > usedSpace:
          usedSpace += allReplicaInfo[replicaID]['Size']
          selectedReplicaIDs.append(replicaID)
    else:
      gLogger.info("StageRequest__issuePrestageRequests: %.2f GB used at %s (limit %2.f GB)" % ((usedSpace)/(1000*1000*1000.0),storageElement,totalSpace/(1000*1000*1000.0)))
      return
    gLogger.info("StageRequest__issuePrestageRequests: Selected %s files eligible for staging at %s." % (len(selectedReplicaIDs),storageElement))

    # Now check that the integrity of the eligible files
    pfnRepIDs = {}
    for replicaID in selectedReplicaIDs:
      pfn = allReplicaInfo[replicaID]['PFN']
      pfnRepIDs[pfn] = replicaID
    res = self.__checkIntegrity(storageElement,pfnRepIDs,allReplicaInfo)
    if not res['OK']:
      return res
    pfnRepIDs = res['Value']

    # Now issue the prestage requests for the remaining replicas
    stageRequestMetadata = {}
    if pfnRepIDs:
      gLogger.info("StageRequest.__issuePrestageRequests: Submitting %s stage requests for %s." % (len(pfnRepIDs),storageElement))
      res = self.replicaManager.prestageStorageFile(pfnRepIDs.keys(),storageElement)
      if not res['OK']:
        gLogger.error("StageRequest.__issuePrestageRequests: Completely failed to sumbmit stage requests for replicas.",res['Message'])
      else:
        for pfn,requestID in res['Value']['Successful'].items():
          if not stageRequestMetadata.has_key(requestID):
            stageRequestMetadata[requestID] = []
          stageRequestMetadata[requestID].append(pfnRepIDs[pfn])
    if stageRequestMetadata:
      gLogger.info("StageRequest.__issuePrestageRequests: %s stage request metadata to be updated." % len(stageRequestMetadata))
      res = self.stagerClient.insertStageRequest(stageRequestMetadata)
      if not res['OK']:
        gLogger.error("StageRequest.__issuePrestageRequests: Failed to insert stage request metadata.", res['Message'])
    return

  def __getWaitingReplicas(self):
    """ This obtains the Waiting replicas from the Replicas table and for each LFN the requested storage element """
    # First obtain the Waiting replicas from the Replicas table
    res = self.stagerClient.getWaitingReplicas()
    if not res['OK']:
      gLogger.error("StageRequest.__getWaitingReplicas: Failed to get replicas with Waiting status.", res['Message'])
      return res
    if not res['Value']:
      gLogger.debug("StageRequest.__getWaitingReplicas: No Waiting replicas found to process.")
      return S_OK()
    else:
     gLogger.debug("StageRequest.__getWaitingReplicas: Obtained %s Waiting replicas(s) to process." % len(res['Value']))
    seReplicas = {}
    replicaIDs = {}
    for replicaID,info in res['Value'].items():
      lfn,storageElement,size,pfn = info
      replicaIDs[replicaID] = {'LFN':lfn,'PFN':pfn,'Size':size,'StorageElement':storageElement}
      if not seReplicas.has_key(storageElement):
        seReplicas[storageElement] = []
      seReplicas[storageElement].append(replicaID)
    return S_OK({'SEReplicas':seReplicas,'ReplicaIDs':replicaIDs})

  def __checkIntegrity(self,storageElement,pfnRepIDs,replicaIDs):
    # Check the integrity of the files to ensure they are available
    terminalReplicaIDs = {}
    gLogger.info("StageRequest.__checkIntegrity: Checking the integrity of %s replicas at %s." % (len(pfnRepIDs),storageElement))
    res = self.replicaManager.getStorageFileMetadata(pfnRepIDs.keys(),storageElement)
    if not res['OK']:
      gLogger.error("StageRequest.__checkIntegrity: Completely failed to obtain metadata for replicas.",res['Message'])
      return res
    for pfn,metadata in res['Value']['Successful'].items():
      if metadata['Cached']:
        gLogger.info("StageRequest.__checkIntegrity: Cache hit for file.")
      if metadata['Size'] != replicaIDs[pfnRepIDs[pfn]]['Size']:
        gLogger.error("StageRequest.__checkIntegrity: PFN StorageElement size does not match FileCatalog",pfn)
        terminalReplicaIDs[pfnRepIDs[pfn]] = 'PFN StorageElement size does not match FileCatalog'
        pfnRepIDs.pop(pfn)
      elif metadata['Lost']:
        gLogger.error("StageRequest.__checkIntegrity: PFN has been Lost by the StorageElement",pfn)
        terminalReplicaIDs[pfnRepIDs[pfn]] = 'PFN has been Lost by the StorageElement'
        pfnRepIDs.pop(pfn)
      elif metadata['Unavailable']:
        gLogger.error("StageRequest.__checkIntegrity: PFN is declared Unavailable by the StorageElement",pfn)
        terminalReplicaIDs[pfnRepIDs[pfn]] = 'PFN is declared Unavailable by the StorageElement'
        pfnRepIDs.pop(pfn)
    for pfn,reason in res['Value']['Failed'].items():
      pfnRepIDs.pop(pfn)
      if re.search('File does not exist',reason):
        gLogger.error("StageRequest.__checkIntegrity: PFN does not exist in the StorageElement",pfn)
        terminalReplicaIDs[pfnRepIDs[pfn]] = 'PFN does not exist in the StorageElement'
    # Update the states of the replicas in the database #TODO Sent status to integrity DB
    if terminalReplicaIDs:
      gLogger.info("StageRequest.__checkIntegrity: %s replicas are terminally failed." % len(terminalReplicaIDs))
      res = self.stagerClient.updateReplicaFailure(terminalReplicaIDs)
      if not res['OK']:
        gLogger.error("StageRequest.__checkIntegrity: Failed to update replica failures.", res['Message'])
    return S_OK(pfnRepIDs)

  def __reportProblematicFiles(self,lfns,reason):
    return S_OK()
    res = self.dataIntegrityClient.setFileProblematic(lfns,reason,self.name)
    if not res['OK']:
      gLogger.error("RequestPreparation.__reportProblematicFiles: Failed to report missing files.",res['Message'])
      return res
    if res['Value']['Successful']:
      gLogger.info("RequestPreparation.__reportProblematicFiles: Successfully reported %s missing files." % len(res['Value']['Successful']))
    if res['Value']['Failed']:
      gLogger.info("RequestPreparation.__reportProblematicFiles: Failed to report %s problematic files." % len(res['Value']['Failed']))
    return res
