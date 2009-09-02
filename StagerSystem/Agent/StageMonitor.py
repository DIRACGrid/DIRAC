# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/Agent/StageMonitor.py,v 1.4 2009/09/02 12:48:22 acsmith Exp $
__RCSID__ = "$Id: StageMonitor.py,v 1.4 2009/09/02 12:48:22 acsmith Exp $"

from DIRAC import gLogger, gConfig, gMonitor, S_OK, S_ERROR, rootPath

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv

from DIRAC.DataManagementSystem.Client.DataIntegrityClient import DataIntegrityClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

import time,os,sys,re
from types import *

AGENT_NAME = 'Stager/StageMonitor'

class StageMonitor(AgentModule):

  def initialize(self):
    self.replicaManager = ReplicaManager()
    self.stagerClient = RPCClient('dips://volhcb08.cern.ch:9149/Stager/Stager')
    self.dataIntegrityClient = DataIntegrityClient()
    return S_OK()

  def execute(self):
    res = setupShifterProxyInEnv('DataManager','%s/runit/%s/proxy' % (rootPath,AGENT_NAME))
    if not res['OK']:
      gLogger.fatal("StageMonitor.execute: Failed to setup data manager proxy.", res['Message'])
      return res
    res = self.monitorStageRequests()
    return res

  def monitorStageRequests(self): 
    """ This is the third logical task manages the StageSubmitted->Staged transition of the Replicas
    """
    res = self.__getStageSubmittedReplicas()
    if not res['OK']:
      gLogger.fatal("StageMonitor.monitorStageRequests: Failed to get replicas from StagerDB.",res['Message'])
      return res
    if not res['Value']:
      gLogger.info("StageMonitor.monitorStageRequests: There were no StageSubmitted replicas found")
      return res
    seReplicas = res['Value']['SEReplicas']
    replicaIDs = res['Value']['ReplicaIDs']
    gLogger.info("StageMonitor.monitorStageRequests: Obtained %s StageSubmitted replicas for monitoring." % len(replicaIDs))
    for storageElement,seReplicaIDs in seReplicas.items():
      self.__monitorStorageElementStageRequests(storageElement,seReplicaIDs,replicaIDs)
    return S_OK()

  def __monitorStorageElementStageRequests(self,storageElement,seReplicaIDs,replicaIDs):
    terminalReplicaIDs = {}
    stagedReplicas = []
    pfnRepIDs = {}
    pfnReqIDs = {}
    for replicaID in seReplicaIDs:
      pfn = replicaIDs[replicaID]['PFN']
      pfnRepIDs[pfn] = replicaID
      pfnReqIDs[pfn] = replicaIDs[replicaID]['RequestID']
    gLogger.info("StageMonitor.__monitorStorageElementStageRequests: Monitoring %s stage requests for %s." % (len(pfnRepIDs),storageElement))
    res = self.replicaManager.getPrestageStorageFileStatus(pfnReqIDs,storageElement)
    if not res['OK']:
      gLogger.error("StageMonitor.__monitorStorageElementStageRequests: Completely failed to monitor stage requests for replicas.",res['Message'])
      return
    for pfn,reason in res['Value']['Failed'].items():
      if re.search('File does not exist',reason):
        gLogger.error("StageMonitor.__monitorStorageElementStageRequests: PFN did not exist in the StorageElement",pfn)
        terminalReplicaIDs[pfnRepIDs[pfn]] = 'PFN did not exist in the StorageElement'
    for pfn,staged in res['Value']['Successful'].items():
      if staged: stagedReplicas.append(pfnRepIDs[pfn])
    # Update the states of the replicas in the database
    if terminalReplicaIDs:
      gLogger.info("StageMonitor.__monitorStorageElementStageRequests: %s replicas are terminally failed." % len(terminalReplicaIDs))
      res = self.stagerClient.updateReplicaFailure(terminalReplicaIDs)
      if not res['OK']:
        gLogger.error("StageMonitor.__monitorStorageElementStageRequests: Failed to update replica failures.", res['Message'])
    if stagedReplicas:
      gLogger.info("StageMonitor.__monitorStorageElementStageRequests: %s staged replicas to be updated." % len(stagedReplicas))
      res = self.stagerClient.setStageComplete(stagedReplicas)
      if not res['OK']:
        gLogger.error("StageMonitor.__monitorStorageElementStageRequests: Failed to updated staged replicas.", res['Message'])
    return 

  def __getStageSubmittedReplicas(self):
    """ This obtains the StageSubmitted replicas from the Replicas table and the RequestID from the StageRequests table """
    res = self.stagerClient.getStageSubmittedReplicas()
    if not res['OK']:
      gLogger.error("StageRequest.__getStageSubmittedReplicas: Failed to get replicas with StageSubmitted status.", res['Message'])
      return res
    if not res['Value']:
      gLogger.debug("StageRequest.__getStageSubmittedReplicas: No StageSubmitted replicas found to process.")
      return S_OK()
    else:
     gLogger.debug("StageRequest.__getStageSubmittedReplicas: Obtained %s StageSubmitted replicas(s) to process." % len(res['Value']))
    seReplicas = {}
    replicaIDs = res['Value']
    for replicaID,info in replicaIDs.items():
      storageElement = info['StorageElement']
      if not seReplicas.has_key(storageElement):
        seReplicas[storageElement] = []
      seReplicas[storageElement].append(replicaID)
    return S_OK({'SEReplicas':seReplicas,'ReplicaIDs':replicaIDs})

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
