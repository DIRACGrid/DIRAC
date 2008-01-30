"""  RAWIntegrityAgent determines whether RAW files in Castor were migrated correctly
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.RequestManagementSystem.Client.Request import RequestClient
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.DB.RAWIntegrityDB import RAWIntegrityDB
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.GridCredentials import setupProxy,restoreProxy,setDIRACGroup, getProxyTimeLeft


import time
from types import *

AGENT_NAME = 'DataManagement/RAWIntegrityAgent'

class RAWIntegrityAgent(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    # need to get the online requestDB URL.'http://lbora01.cern.ch:9135'
    self.RequestDBClient = RequestClient()
    self.ReplicaManager = ReplicaManager()
    self.RAWIntegrityDB = RAWIntegrityDB()
    self.wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    self.proxyDN = gConfig.getValue(self.section+'/ProxyDN')
    self.proxyGroup = gConfig.getValue(self.section+'/ProxyGroup')
    self.proxyLength = gConfig.getValue(self.section+'/DefaultProxyLength',12)
    self.proxyLocation = gConfig.getValue(self.section+'/ProxyLocation')
    return result

  def execute(self):

    ############################################################
    #
    # Get a valid proxy for the current activity
    #
    self.log.info("RAWIntegrityAgent.execute: Determining the length of the %s proxy." %self.proxyDN)
    obtainProxy = False
    if not os.path.exists(self.proxyLocation):
      obtainProxy = True
    else:
      currentProxy = open(self.proxyLocation,'r')
      oldProxyStr = currentProxy.read()
      res = getProxyTimeLeft(oldProxyStr)
      if not res["OK"]:
        gLogger.error("RAWIntegrityAgent.execute: Could not determine the time left for proxy.", res['Message'])
        return S_OK()
      proxyValidity = int(res['Value'])
      self.log.info("RAWIntegrityAgent.execute: %s proxy found to be valid for %s seconds."% (self.proxyDN,proxyValidity))
      if proxyValidity <= 60:
        obtainProxy = True

    if obtainProxy:
      self.log.info("RAWIntegrityAgent.execute: Attempting to renew %s proxy." %self.proxyDN)
      res = self.wmsAdmin.getProxy(self.proxyDN,self.proxyGroup,self.proxyLength)
      if not res['OK']:
        gLogger.error("RAWIntegrityAgent.execute: Could not retrieve proxy from WMS Administrator", res['Message'])
        return S_OK()
      proxyStr = res['Value']
      if not os.path.exists(os.path.dirname(self.proxyLocation)):
        os.makedirs(os.path.dirname(self.proxyLocation))
      res = setupProxy(proxyStr,self.proxyLocation)
      if not res['OK']:
        gLogger.error("RAWIntegrityAgent.execute: Could not create environment for proxy.", res['Message'])
        return S_OK()
      setDIRACGroup(self.proxyGroup)
      self.log.info("RAWIntegrityAgent.execute: Successfully renewed %s proxy." %self.proxyDN)

    ############################################################
    #
    # Obtain the files which have not yet been migrated
    #
    gLogger.info("RAWIntegrityAgent.execute: Obtaining un-migrated files.")
    res = self.RAWIntegrityDB.getActiveFiles()
    if not res['OK']:
      errStr = "RAWIntegrityAgent.execute: Failed to obtain un-migrated files."
      gLogger.error(errStr,res['Message'])
      return S_OK()
    activeFiles = res['Value']
    gLogger.info("RAWIntegrityAgent.execute: Obtained %s un-migrated files." % len(activeFiles.keys()))
    if not len(activeFiles.keys()) > 0:
      return S_OK()

    ############################################################
    #
    # Obtain the physical file metadata for the files awating migration
    #
    gLogger.info("RAWIntegrityAgent.execute: Obtaining physical file metadata.")
    sePfns = {}
    pfnDict = {}
    for lfn,metadataDict in activeFiles.items():
      pfn = metadataDict['PFN']
      pfnDict[pfn] = lfn
      se = metadataDict['SE']
      if not sePfns.has_key(se):
        sePfns[se] = []
      sePfns[se].append(pfn)
    pfnMetadata = {'Successful':{},'Failed':{}}
    for se,pfnList in sePfns.items():
      res = self.ReplicaManager.getPhysicalFileMetadata(pfnList,se)
      if not res['OK']:
        errStr = "RAWIntegrityAgent.execute: Failed to obtain physical file metadata."
        gLogger.error(errStr,res['Message'])
        for pfn in pfnList:
          pfnMetadata['Failed'][pfn] = errStr
      else:
        pfnMetadata['Successful'].update(res['Value']['Successful'])
        pfnMetadata['Failed'].update(res['Value']['Failed'])
    if len(pfnMetadata['Failed']) > 0:
      gLogger.info("RAWIntegrityAgent.execute: Failed to obtain physical file metadata for %s files." % len(pfnMetadata['Failed']))
    gLogger.info("RAWIntegrityAgent.execute: Obtained physical file metadata for %s files." % len(pfnMetadata['Successful']))
    if not len(pfnMetadata['Successful']) > 0:
      return S_OK()

    ############################################################
    #
    # Determine the files that have been newly migrated and their success
    #
    filesToRemove = []
    filesToTransfer = []
    for pfn,pfnMetadataDict in pfnMetadata.items():
      if pfnMetadataDict['Migrated']:
        lfn = pfnDict[pfn]
        gLogger.info("RAWIntegrityAgent.execute: %s is newly migrated." % lfn)
        if pfnMetadataDict['Checksum'] == activeFile[lfn]['Checksum']:
          gLogger.info("RAWIntegrityAgent.execute: %s migrated checksum match." % lfn)
          filesToRemove.append(lfn)
        else:
          gLogger.error("RAWIntegrityAgent.execute: Migrated checksum mis-match.",lfn)
          filesToTransfer.append(lfn)
    gLogger.info("RAWIntegrityAgent.execute: %s files newly migrated." % len(filesToRemove)+len(filesToTransfer))
    gLogger.info("RAWIntegrityAgent.execute: Found %s checksum matches." % len(filesToRemove))
    gLogger.info("RAWIntegrityAgent.execute: Found %s checksum mis-matches." % len(filesToTransfer))


    if len(filesToRemove) > 0:
      ############################################################
      #
      # Register the correctly migrated files to the file catalogue
      #
      gLogger.info("RAWIntegrityAgent.execute: Registering correctly migrated files to the File Catalog.")
      for lfn in filesToRemove:
        pfn = activeFiles[lfn]['PFN']
        size = activeFiles[lfn]['Size']
        se = activeFiles[lfn]['SE']
        guid = activeFiles[lfn]['GUID']
        fileTuple = (lfn,pfn,size,se,guid)
        res = self.ReplicaManager.registerFile(fileTuple)
        if not res['OK']:
          gLogger.error("RAWIntegrityAgent.execute: Completely failed to register successfully migrated file.", res['Message'])
        elif not res['Value']['Successful'].has_key(lfn):
          gLogger.error("RAWIntegrityAgent.execute: Failed to register lfn in the File Catalog.", res['Value']['Failed'][lfn])
        else:
          gLogger.info("RAWIntegrityAgent.execute: Successfully registered %s in the File Catalog.", lfn)
          ############################################################
          #
          # Create a removal request and set it to the gateway request DB
          #
          gLogger.info("RAWIntegrityAgent.execute: Creating removal request for correctly migrated files.")
          oRequest = DataManagementRequest()
          subRequestIndex = oRequest.initiateSubRequest('removal')['Value']
          attributeDict = {'Operation':'physicalRemoval','TargetSE':'OnlineRunDB'}
          oRequest.setSubRequestAttributes(subRequestIndex,'removal',attributeDict)
          filesDict = [{'LFN':lfn,'PFN':pfn}]
          oRequest.setSubRequestFiles(subRequestIndex,'removal',filesDict)
          fileName = os.path.basename(lfn)
          requestName = 'remove_%s.xml' % fileName
          requestString = oRequest.toXML()['Value']
          gLogger.info("RAWIntegrityAgent.execute: Attempting to put %s to gateway requestDB." %  requestName)
          res = self.RequestDBClient.setRequest(requestName,requestString)
          if not res['OK']:
            gLogger.error("RAWIntegrityAgent.execute: Failed to set removal request to gateway requestDB.", res['Message'])
          else:
            gLogger.info("RAWIntegrityAgent.execute: Successfully put %s to gateway requestDB." %  requestName)
            ############################################################
            #
            # Remove the file from the list of files awaiting migration in database
            #
            gLogger.info("RAWIntegrityAgent.execute: Updating status of %s in raw integrity database." %  lfn)
            res = self.RAWIntegrityDB.setFileStatus(lfn,'Done')
            if not res['OK']:
              gLogger.error("RAWIntegrityAgent.execute: Failed to update status in raw integrity database.", res['Message'])
            else:
              gLogger.info("RAWIntegrityAgent.execute: Successfully updated status in raw integrity database.")


    if len(filesToTransfer) > 0:
      ############################################################
      #
      # Remove the incorrectly migrated files from the storage element (will be over written anyways)
      #
      gLogger.info("RAWIntegrityAgent.execute: Removing incorrectly migrated files from Storage Element.")
      for lfn in filesToRemove:
        pfn = activeFiles[lfn]['PFN']
        size = activeFiles[lfn]['Size']
        se = activeFiles[lfn]['SE']
        guid = activeFiles[lfn]['GUID']
        res = self.ReplicaManager.removePhysicalFile(se,pfn)
        if not res['OK']:
          gLogger.error("RAWIntegrityAgent.execute: Completely failed to remove pfn from the storage element.", res['Message'])
        elif not res['Value']['Successful'].has_key(pfn):
          gLogger.error("RAWIntegrityAgent.execute: Failed to remove pfn from the storage element.", res['Value']['Failed'][pfn])
        else:
          gLogger.info("RAWIntegrityAgent.execute: Successfully removed pfn from the storage element.")
          ############################################################
          #
          # Create a transfer request for the files incorrectly migrated
          #
          gLogger.info("RAWIntegrityAgent.execute: Creating (re)transfer request for incorrectly migrated files.")
          oRequest = DataManagementRequest()
          subRequestIndex = oRequest.initiateSubRequest('transfer')['Value']
          attributeDict = {'Operation':'putAndRegister','TargetSE':se}
          oRequest.setSubRequestAttributes(subRequestIndex,'transfer',attributeDict)
          fileName = os.path.basename(lfn)
          filesDict = [{'LFN':lfn,'PFN':fileName,'GUID':guid}]
          oRequest.setSubRequestFiles(subRequestIndex,'transfer',filesDict)
          requestName = 'retransfer_%s.xml' % fileName
          requestString = oRequest.toXML()['Value']
          gLogger.info("RAWIntegrityAgent.execute: Attempting to put %s to gateway requestDB." %  requestName)
          res = self.RequestDBClient.setRequest(requestName,requestString)
          if not res['OK']:
            gLogger.error("RAWIntegrityAgent.execute: Failed to set removal request to gateway requestDB.", res['Message'])
          else:
            gLogger.info("RAWIntegrityAgent.execute: Successfully put %s to gateway requestDB." %  requestName)
            ############################################################
            #
            # Remove the file from the list of files awaiting migration in database
            #
            gLogger.info("RAWIntegrityAgent.execute: Updating status of %s in raw integrity database." %  lfn)
            res = self.RAWIntegrityDB.setFileStatus(lfn,'Failed')
            if not res['OK']:
              gLogger.error("RAWIntegrityAgent.execute: Failed to update status in raw integrity database.", res['Message'])
            else:
              gLogger.info("RAWIntegrityAgent.execute: Successfully updated status in raw integrity database.")

    return S_OK()



  #############################################################################
  def __setupProxy(self,job,ownerDN,jobGroup,proxyDir):
    """Retrieves user proxy with correct role for job and sets up environment to
       run job locally.
    """

    self.log.verbose(setupResult)
    return setupResult


