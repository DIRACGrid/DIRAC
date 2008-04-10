"""  SEvsLFCAgent takes data integrity checks from the RequestDB and verifies the integrity of the supplied directory.
"""
from DIRAC  import gLogger, gConfig, gMonitor, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.GridCredentials import setupProxy,restoreProxy,setDIRACGroup, getProxyTimeLeft
from DIRAC.RequestManagementSystem.Client.Request import RequestClient
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Agent.NamespaceBrowser import NamespaceBrowser
from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog

import time,os
from types import *

AGENT_NAME = 'DataManagement/SEvsLFCAgent'

class SEvsLFCAgent(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.RequestDBClient = RequestClient()
    self.ReplicaManager = ReplicaManager()
    self.lfc = FileCatalog(['LcgFileCatalogCombined'])

    self.IntegrityDB = RPCClient('DataManagement/DataIntegrity')
    self.useProxies = gConfig.getValue(self.section+'/UseProxies','True')
    if self.useProxies == 'True':
      self.wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
      self.proxyDN = gConfig.getValue(self.section+'/ProxyDN','')
      self.proxyGroup = gConfig.getValue(self.section+'/ProxyGroup','')
      self.proxyLength = gConfig.getValue(self.section+'/DefaultProxyLength',12)
      self.proxyLocation = gConfig.getValue(self.section+'/ProxyLocation','')
      if os.path.exists(self.proxyLocation):
        os.remove(self.proxyLocation)
    return result

  def execute(self):

    if self.useProxies == 'True':
      ############################################################
      #
      # Get a valid proxy for the current activity
      #
      self.log.info("SEvsLFCAgent.execute: Determining the length of the %s proxy." %self.proxyDN)
      obtainProxy = False
      if not os.path.exists(self.proxyLocation):
        self.log.info("SEvsLFCAgent: No proxy found.")
        obtainProxy = True
      else:
        currentProxy = open(self.proxyLocation,'r')
        oldProxyStr = currentProxy.read()
        res = getProxyTimeLeft(oldProxyStr)
        if not res["OK"]:
          gLogger.error("SEvsLFCAgent: Could not determine the time left for proxy.", res['Message'])
          return S_OK()
        proxyValidity = int(res['Value'])
        gLogger.debug("SEvsLFCAgent: Current proxy found to be valid for %s seconds." % proxyValidity)
        self.log.info("SEvsLFCAgent: %s proxy found to be valid for %s seconds."% (self.proxyDN,proxyValidity))
        if proxyValidity <= 60:
          obtainProxy = True

      if obtainProxy:
        self.log.info("SEvsLFCAgent: Attempting to renew %s proxy." %self.proxyDN)
        res = self.wmsAdmin.getProxy(self.proxyDN,self.proxyGroup,self.proxyLength)
        if not res['OK']:
          gLogger.error("SEvsLFCAgent: Could not retrieve proxy from WMS Administrator", res['Message'])
          return S_OK()
        proxyStr = res['Value']
        if not os.path.exists(os.path.dirname(self.proxyLocation)):
          os.makedirs(os.path.dirname(self.proxyLocation))
        res = setupProxy(proxyStr,self.proxyLocation)
        if not res['OK']:
          gLogger.error("SEvsLFCAgent: Could not create environment for proxy.", res['Message'])
          return S_OK()
        setDIRACGroup(self.proxyGroup)
        self.log.info("SEvsLFCAgent: Successfully renewed %s proxy." %self.proxyDN)

    res = self.RequestDBClient.getRequest('integrity')
    if not res['OK']:
      gLogger.info("SEvsLFCAgent.execute: Failed to get request from database.")
      return S_OK()
    elif not res['Value']:
      gLogger.info("SEvsLFCAgent.execute: No requests to be executed found.")
      return S_OK()
    requestString = res['Value']['requestString']
    requestName = res['Value']['requestName']
    sourceServer= res['Value']['Server']
    gLogger.info("SEvsLFCAgent.execute: Obtained request %s" % requestName)
    oRequest = DataManagementRequest(request=requestString)

    ################################################
    # Find the number of sub-requests from the request
    res = oRequest.getNumSubRequests('integrity')
    if not res['OK']:
      errStr = "SEvsLFCAgent.execute: Failed to obtain number of integrity subrequests."
      gLogger.error(errStr,res['Message'])
      return S_OK()
    gLogger.info("SEvsLFCAgent.execute: Found %s sub requests." % res['Value'])

    ################################################
    # For all the sub-requests in the request
    for ind in range(res['Value']):
      gLogger.info("SEvsLFCAgent.execute: Processing sub-request %s." % ind)
      subRequestAttributes = oRequest.getSubRequestAttributes(ind,'integrity')['Value']
      if subRequestAttributes['Status'] == 'Waiting':
        subRequestFiles = oRequest.getSubRequestFiles(ind,'integrity')['Value']
        operation = subRequestAttributes['Operation']

        ################################################
        #  If the sub-request is a lfcvsse operation
        if operation == 'SEvsLFC':
          gLogger.info("SEvsLFCAgent.execute: Attempting to execute %s sub-request." % operation)
          storageElementName = subRequestAttributes['StorageElement']
          for subRequestFile in subRequestFiles:
            if subRequestFile['Status'] == 'Waiting':
              lfn = subRequestFile['LFN']

              storageElement = StorageElement(storageElementName)
              if not storageElement.isValid()['Value']:
                errStr = "SEvsLFCAgent.execute: Failed to instantiate destination StorageElement."
                gLogger.error(errStr,storageElement)
              else:
                res = storageElement.getPfnForLfn(lfn)
                if not res['OK']:
                  gLogger.info('shit bugger do something.')
                else:
                  oNamespaceBrowser = NameSpaceBrowser(res['Value'])
                  # Loop over all the directories and sub-directories
                  while (oNamespaceBrowser.isActive()):
                    currentDir = oNamespaceBrowser.getActiveDir()

                    res = storageElement.listDirectory(currentDir)
                    if not res['Value']['Successful'].has_key(currentDir):
                      subDirs = [currentDir]
                    else:
                      subDirs = res['Value']['Successful'][currentDir]['SubDirs']
                      files = res['Value']['Successful'][currentDir]['Files']
                      selectedLfns = []
                      lfnPfnDict = {}
                      pfnSize = {}
                      for pfn,pfnDict in files.items():
                        res = storageElement.getPfnPath(pfn)
                        if not res['OK']:
                          gLogger.error("SEvsLFCAgent.execute: Failed to get determine LFN from pfn.", "%s %s" % (pfn,res['Message']))
                          fileMetadata = {'Prognosis':'NonConventionPfn','LFN':'','PFN':pfn,'StorageElement':storageElementName,'Size':pfnDict['Size']}
                          res = self.IntegrityDB.insertProblematic(AGENT_NAME,fileMetadata)
                          if res['OK']:
                            gLogger.info("SEvsLFCAgent.execute: Successfully added to IntegrityDB.")
                            gLogger.error("Change the status in the LFC,ProcDB....")
                          else:
                            gLogger.error("Shit, fuck, bugger. Add the failover.")
                        else:
                          lfn = res['Value']
                          selectedLfns.append(lfn)
                          lfnPfnDict[lfn] = pfn
                          pfnSize[pfn] = pfnDict['Size']

                        res = self.lfc.getFileMetadata(selectedLfns)
                        if not res['OK']:
                          subDirs = [currentDir]
                        else:
                          for lfn in res['Value']['Failed'].keys():
                            gLogger.error("SEvsLFCAgent.execute: Failed to get metadata.","%s %s" % (lfn,res['Value']['Failed'][lfn]))
                            pfn = lfnPfnDict[lfn]
                            fileMetadata = {'Prognosis':'SEPfnNoLfn','LFN':lfn,'PFN':pfn,'StorageElement':storageElementName,'Size':pfnSize[pfn]}
                            res = self.IntegrityDB.insertProblematic(AGENT_NAME,fileMetadata)
                            if res['OK']:
                              gLogger.info("SEvsLFCAgent.execute: Successfully added to IntegrityDB.")
                              gLogger.error("Change the status in the LFC,ProcDB....")
                            else:
                              gLogger.error("Shit, fuck, bugger. Add the failover.")

                          for lfn,lfnDict in res['Value']['Successful'].items():
                            pfn = lfnPfnDict[lfn]
                            storageSize = pfnSize[pfn]
                            catalogSize = lfnDict['Size']
                            if int(catalogSize) == int(storageSize):
                              gLogger.info("SEvsLFCAgent.execute: Catalog and storage sizes match.","%s %s" % (pfn,storageElementName))
                              gLogger.info("Change the status in the LFC")
                            elif int(storageSize) == 0:
                              gLogger.error("SEvsLFCAgent.execute: Physical file size is 0.", "%s %s" % (pfn,storageElementName))
                              fileMetadata = {'Prognosis':'ZeroSizePfn','LFN':lfn,'PFN':pfn,'StorageElement':storageElementName}
                              res = self.IntegrityDB.insertProblematic(AGENT_NAME,fileMetadata)
                              if res['OK']:
                                gLogger.info("SEvsLFCAgent.execute: Successfully added to IntegrityDB.")
                                gLogger.error("Change the status in the LFC,ProcDB....")
                              else:
                                gLogger.error("Shit, fuck, bugger. Add the failover.")
                            else:
                              gLogger.error("SEvsLFCAgent.execute: Catalog and storage size mis-match.","%s %s" % (pfn,storageElementName))
                              fileMetadata = {'Prognosis':'PfnSizeMismatch','LFN':lfn,'PFN':pfn,'StorageElement':storageElementName}
                              res = self.IntegrityDB.insertProblematic(AGENT_NAME,fileMetadata)
                              if res['OK']:
                                gLogger.info("SEvsLFCAgent.execute: Successfully added to IntegrityDB.")
                                gLogger.error("Change the status in the LFC,ProcDB....")
                              else:
                                gLogger.error("Shit, fuck, bugger. Add the failover.")

                          res = self.lfc.getReplicas(lfns)
                          if not res['OK']:
                            subDirs = [currentDir]
                          else:
                            for lfn in res['Value']['Failed'].keys():
                              gLogger.error("SEvsLFCAgent.execute: Failed to get replica information.","%s %s" % (lfn,res['Value']['Failed'][lfn]))
                              pfn = lfnPfnDict[lfn]
                              fileMetadata = {'Prognosis':'PfnNoReplica','LFN':lfn,'PFN':pfn,'StorageElement':storageElementName,'Size':pfnSize[pfn]}
                              res = self.IntegrityDB.insertProblematic(AGENT_NAME,fileMetadata)
                              if res['OK']:
                                gLogger.info("SEvsLFCAgent.execute: Successfully added to IntegrityDB.")
                                gLogger.error("Change the status in the LFC,ProcDB....")
                              else:
                                gLogger.error("Shit, fuck, bugger. Add the failover.")

                            for lfn,repDict in res['Value']['Successful'].items():
                              pfn = lfnPfnDict[lfn]
                              registeredPfns = repDict.values()
                              if not pfn in registeredPfns:
                                gLogger.error("SEvsLFCAgent.execute: SE PFN not registered.","%s %s" % (lfn,pfn))
                                fileMetadata = {'Prognosis':'PfnNoReplica','LFN':lfn,'PFN':pfn,'StorageElement':storageElementName}
                                res = self.IntegrityDB.insertProblematic(AGENT_NAME,fileMetadata)
                                if res['OK']:
                                  gLogger.info("SEvsLFCAgent.execute: Successfully added to IntegrityDB.")
                                  gLogger.error("Change the status in the LFC,ProcDB....")
                                else:
                                  gLogger.error("Shit, fuck, bugger. Add the failover.")
                              else:
                                gLogger.info("SEvsLFCAgent.execute: SE Pfn verified.", pfn)

                    oNamespaceBrowser.updateDirs(subDirs)
                  oRequest.setSubRequestFileAttributeValue(ind,'integrity',lfn,'Status','Done')

        ################################################
        #  If the sub-request is none of the above types
        else:
          gLogger.info("SEvsLFCAgent.execute: Operation not supported.", operation)

        ################################################
        #  Determine whether there are any active files
        if oRequest.isSubRequestEmpty(ind,'integrity')['Value']:
          oRequest.setSubRequestStatus(ind,'integrity','Done')

      ################################################
      #  If the sub-request is already in terminal state
      else:
        gLogger.info("SEvsLFCAgent.execute: Sub-request %s is status '%s' and  not to be executed." % (ind,subRequestAttributes['Status']))

    ################################################
    #  Generate the new request string after operation
    requestString = oRequest.toXML()['Value']
    res = self.RequestDBClient.updateRequest(requestName,requestString,sourceServer)

    return S_OK()
