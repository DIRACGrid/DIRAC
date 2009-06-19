# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/StagerSystem/Agent/StageRequest.py,v 1.2 2009/06/19 20:04:53 acsmith Exp $
__RCSID__ = "$Id: StageRequest.py,v 1.2 2009/06/19 20:04:53 acsmith Exp $"

from DIRAC import gLogger, gConfig, gMonitor, S_OK, S_ERROR, rootPath

from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv
from DIRAC.Core.Utilities.ThreadPool import ThreadPool,ThreadedJob

from DIRAC.DataManagementSystem.Client.DataIntegrityClient import DataIntegrityClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

import time,os,sys,re
from types import *

AGENT_NAME = 'Stager/StageRequest'

class StageRequest(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.replicaManager = ReplicaManager()
    self.stagerClient = RPCClient('dips://volhcb08.cern.ch:9149/Stager/Stager')
    self.dataIntegrityClient = DataIntegrityClient()
    return S_OK()

  def execute(self):
    res = setupShifterProxyInEnv('DataManager','%s/%s' % (rootPath,time.time()))
    if not res['OK']:
      gLogger.fatal("StageRequest.execute: Failed to setup data manager proxy.", res['Message'])
      return res
    res = self.submitStageRequests()
    return res

  def submitStageRequests(self): 
    """ This is the second logical task to be executed and manages the Waiting->StageSubmitted transition of the Replicas
    """
    res = self.__getWaitingReplicas()
    if not res['OK']:
      gLogger.fatal("StageRequest.submitStageRequests: Failed to get replicas from StagerDB.",res['Message'])
      return res
    if not res['Value']:
      gLogger.info("StageRequest.submitStageRequests: There were no Waiting replicas found")
      return res
    seReplicas = res['Value']['SEReplicas']
    replicaIDs = res['Value']['ReplicaIDs']
    gLogger.info("StageRequest.submitStageRequests: Obtained %s Waiting replicas for preparation." % len(replicaIDs))

    # Get the current submitted stage space and the amount of pinned space for each storage element
    res = self.stagerClient.getSubmittedStagePins()
    if not res['OK']:
      gLogger.fatal("StageRequest.submitStageRequests: Failed to obtain submitted requests from StagerDB.",res['Message'])
      return res

    for storageElement,seReplicaIDs in seReplicas.items():
      self.__issuePrestageRequests(storageElement,seReplicaIDs,replicaIDs)
    return S_OK()

  def __issuePrestageRequests(self,storageElement,seReplicaIDs,replicaIDs):
    terminalReplicaIDs = {}
    stageRequestMetadata = {}
    pfnRepIDs = {}
    for replicaID in seReplicaIDs:
      pfn = replicaIDs[replicaID]['PFN']
      pfnRepIDs[pfn] = replicaID
    # Check the integrity of the files to ensure they are available
    gLogger.info("StageRequest.submitStageRequests: Checking the integrity of %s replicas at %s." % (len(pfnRepIDs),storageElement))
    res = self.replicaManager.getPhysicalFileMetadata(pfnRepIDs.keys(),storageElement)
    if not res['OK']:
      gLogger.error("StageRequest.submitStageRequests: Completely failed to obtain metadat for replicas.",res['Message'])
    for pfn,metadata in res['Value']['Successful'].items():
      if metadata['Cached']:
        gLogger.info("StageRequest.submitStageRequests: Cache hit for file.")
      if metadata['Size'] != replicaIDs[pfnRepIDs[pfn]]['Size']:
        gLogger.error("StageRequest.submitStageRequests: PFN StorageElement size does not match FileCatalog",pfn)
        terminalReplicaIDs[pfnRepIDs[pfn]] = 'PFN StorageElement size does not match FileCatalog'
        pfnRepIDs.pop(pfn) 
      elif metadata['Lost']:
        gLogger.error("StageRequest.submitStageRequests: PFN has been Lost by the StorageElement",pfn)
        terminalReplicaIDs[pfnRepIDs[pfn]] = 'PFN has been Lost by the StorageElement'
        pfnRepIDs.pop(pfn)
      elif metadata['Unavailable']:
        gLogger.error("StageRequest.submitStageRequests: PFN is declared Unavailable by the StorageElement",pfn)
        terminalReplicaIDs[pfnRepIDs[pfn]] = 'PFN is declared Unavailable by the StorageElement'
        pfnRepIDs.pop(pfn)
    for pfn,reason in res['Value']['Failed'].items():
      if re.search('File does not exist',reason):
        gLogger.error("StageRequest.submitStageRequests: PFN does not exist in the StorageElement",pfn)
        terminalReplicaIDs[pfnRepIDs[pfn]] = 'PFN does not exist in the StorageElement'
        pfnRepIDs.pop(pfn) 
    # Now issue the prestage requests for the remaining replicas
    if pfnRepIDs:
      gLogger.info("StageRequest.submitStageRequests: Submitting %s stage requests for %s." % (len(pfnRepIDs),storageElement))
      res = self.replicaManager.prestagePhysicalFile(pfnRepIDs.keys(),storageElement)
      if not res['OK']:
        gLogger.error("StageRequest.submitStageRequests: Completely failed to sumbmit stage requests for replicas.",res['Message'])
      for pfn,reason in res['Value']['Failed'].items():
        if re.search('File does not exist',reason):
          terminalReplicaIDs[pfnRepIDs[pfn]] = 'PFN did not exist in the StorageElement'
      requestIDs = {}
      for pfn,requestID in res['Value']['Successful'].items():
        if not stageRequestMetadata.has_key(requestID):
          stageRequestMetadata[requestID] = []
        stageRequestMetadata[requestID].append(pfnRepIDs[pfn])
    # Update the states of the replicas in the database
    if terminalReplicaIDs:
      gLogger.info("StageRequest.submitStageRequests: %s replicas are terminally failed." % len(terminalReplicaIDs))
      res = self.stagerClient.updateReplicaFailure(terminalReplicaIDs)
      if not res['OK']:
        gLogger.error("StageRequest.submitStageRequest: Failed to update replica failures.", res['Message'])
    if stageRequestMetadata:
      gLogger.info("StageRequest.submitStageRequest: %s stage request metadata to be updated." % len(stageRequestMetadata))
      res = self.stagerClient.insertStageRequest(stageRequestMetadata)
      if not res['OK']:
        gLogger.error("StageRequest.submitStageRequest: Failed to insert stage request metadata.", res['Message'])

  def __getWaitingReplicas(self):
    """ This obtains the Waiting replicas from the Replicas table and for each LFN the requested storage element """
    # First obtain the Waiting replicas from the Replicas table
    res = self.stagerClient.getReplicasWithStatus('Waiting')
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
