"""  LFCvsSEAgent takes data integrity checks from the RequestDB and verifies the integrity of the supplied directory.
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

AGENT_NAME = 'DataManagement/LFCvsSEAgent'

class LFCvsSEAgent(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.RequestDBClient = RequestClient()
    self.ReplicaManager = ReplicaManager()
    self.lfc = FileCatalog(['LFC'])

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
      self.log.info("LFCvsSEAgent.execute: Determining the length of the %s proxy." %self.proxyDN)
      obtainProxy = False
      if not os.path.exists(self.proxyLocation):
        self.log.info("LFCvsSEAgent: No proxy found.")
        obtainProxy = True
      else:
        currentProxy = open(self.proxyLocation,'r')
        oldProxyStr = currentProxy.read()
        res = getProxyTimeLeft(oldProxyStr)
        if not res["OK"]:
          gLogger.error("LFCvsSEAgent: Could not determine the time left for proxy.", res['Message'])
          return S_OK()
        proxyValidity = int(res['Value'])
        gLogger.debug("LFCvsSEAgent: Current proxy found to be valid for %s seconds." % proxyValidity)
        self.log.info("LFCvsSEAgent: %s proxy found to be valid for %s seconds."% (self.proxyDN,proxyValidity))
        if proxyValidity <= 60:
          obtainProxy = True

      if obtainProxy:
        self.log.info("LFCvsSEAgent: Attempting to renew %s proxy." %self.proxyDN)
        res = self.wmsAdmin.getProxy(self.proxyDN,self.proxyGroup,self.proxyLength)
        if not res['OK']:
          gLogger.error("LFCvsSEAgent: Could not retrieve proxy from WMS Administrator", res['Message'])
          return S_OK()
        proxyStr = res['Value']
        if not os.path.exists(os.path.dirname(self.proxyLocation)):
          os.makedirs(os.path.dirname(self.proxyLocation))
        res = setupProxy(proxyStr,self.proxyLocation)
        if not res['OK']:
          gLogger.error("LFCvsSEAgent: Could not create environment for proxy.", res['Message'])
          return S_OK()
        setDIRACGroup(self.proxyGroup)
        self.log.info("LFCvsSEAgent: Successfully renewed %s proxy." %self.proxyDN)

    res = self.RequestDBClient.getRequest('integrity')
    if not res['OK']:
      gLogger.info("LFCvsSEAgent.execute: Failed to get request from database.")
      return S_OK()
    elif not res['Value']:
      gLogger.info("LFCvsSEAgent.execute: No requests to be executed found.")
      return S_OK()
    requestString = res['Value']['requestString']
    requestName = res['Value']['requestName']
    sourceServer= res['Value']['Server']
    gLogger.info("LFCvsSEAgent.execute: Obtained request %s" % requestName)
    oRequest = DataManagementRequest(request=requestString)

    ################################################
    # Find the number of sub-requests from the request
    res = oRequest.getNumSubRequests('integrity')
    if not res['OK']:
      errStr = "LFCvsSEAgent.execute: Failed to obtain number of integrity subrequests."
      gLogger.error(errStr,res['Message'])
      return S_OK()
    gLogger.info("LFCvsSEAgent.execute: Found %s sub requests." % res['Value'])

    ################################################
    # For all the sub-requests in the request
    for ind in range(res['Value']):
      gLogger.info("LFCvsSEAgent.execute: Processing sub-request %s." % ind)
      subRequestAttributes = oRequest.getSubRequestAttributes(ind,'integrity')['Value']
      if subRequestAttributes['Status'] == 'Waiting':
        subRequestFiles = oRequest.getSubRequestFiles(ind,'integrity')['Value']
        operation = subRequestAttributes['Operation']

        ################################################
        #  If the sub-request is a lfcvsse operation
        if operation == 'LFCvsSE':
          gLogger.info("LFCvsSEAgent.execute: Attempting to execute %s sub-request." % operation)
          for subRequestFile in subRequestFiles:
            if subRequestFile['Status'] == 'Waiting':
              lfn = subRequestFile['LFN']
              oNamespaceBrowser = NameSpaceBrowser(lfn)

              # Loop over all the directories and sub-directories
              while (oNamespaceBrowser.isActive()):
                currentDir = oNamespaceBrowser.getActiveDir()
                res = self.lfc.getDirectoryContents(currentDir)
                if not res['OK']:
                  subDirs = [currentDir]
                else:
                  subDirs = res['Value']['SubDirs']
                  files = res['Value']['Files']

                  lfnSizeDict = {}
                  pfnLfnDict = {}
                  pfnStatusDict = {}
                  sePfnDict = {}
                  for lfn, lfnDict in files.items():
                    lfnSizeDict[lfn] = lfnDict['MetaData']['Size']
                    for se in lfnDict['Replicas'].keys():
                      pfn = lfnDict['Replicas'][se]['PFN']
                      status = lfnDict['Replicas'][se]['Status']
                      pfnStatusDict[pfn] = status
                      pfnLfnDict[pfn] = lfn
                      if not sePfnDict.has_key(se):
                        sePfnDict[se] = []
                      sePfnDict[se].append(pfn)

                  for storageElementName,physicalFiles in sePfnDict.items():
                    res = self.ReplicaManager.getPhysicalFileMetadata(physicalFiles, storageElementName)
                    if not res['OK']:
                      gLogger.error("LFCvsSEAgent.execute: Completely failed to get physical file metadata.",res['Message'])
                    else:
                      for pfn in res['Value']['Failed'].keys():
                        gLogger.error("LFCvsSEAgent.execute: Failed to get metadata.","%s %s" % (pfn,res['Value']['Failed'][pfn]))
                        lfn = pfnLfnDict[pfn]
                        fileMetadata = {'Prognosis':'MissingSEPfn','LFN':lfn,'PFN':pfn,'StorageElement':storageElementName,'Size':lfnSizeDict[lfn]}
                        res = self.IntegrityDB.insertProblematic(AGENT_NAME,fileMetadata)
                        if res['OK']:
                          gLogger.info("LFCvsSEAgent.execute: Successfully added to IntegrityDB.")
                          gLogger.error("Change the status in the LFC,ProcDB....")
                        else:
                          gLogger.error("Shit, fuck, bugger. Add the failover.")
                      for pfn,pfnDict in res['Value']['Successful'].items():
                        lfn = pfnLfnDict[pfn]
                        catalogSize = lfnSizeDict[lfn]
                        storageSize = pfnDict['Size']
                        if int(catalogSize) == int(storageSize):
                          gLogger.info("LFCvsSEAgent.execute: Catalog and storage sizes match.","%s %s" % (pfn,storageElementName))
                          gLogger.info("Change the status in the LFC")
                        else:
                          gLogger.error("LFCvsSEAgent.execute: Catalog and storage size mis-match.","%s %s" % (pfn,storageElementName))
                          fileMetadata = {'Prognosis':'PfnSizeMismatch','LFN':lfn,'PFN':pfn,'StorageElement':storageElementName}
                          res = self.IntegrityDB.insertProblematic(AGENT_NAME,fileMetadata)
                          if res['OK']:
                            gLogger.info("LFCvsSEAgent.execute: Successfully added to IntegrityDB.")
                            gLogger.error("Change the status in the LFC,ProcDB....")
                          else:
                            gLogger.error("Shit, fuck, bugger. Add the failover.")
                oNamespaceBrowser.updateDirs(subDirs)
              oRequest.setSubRequestFileAttributeValue(ind,'integrity',lfn,'Status','Done')

        ################################################
        #  If the sub-request is none of the above types
        else:
          gLogger.info("LFCvsSEAgent.execute: Operation not supported.", operation)

        ################################################
        #  Determine whether there are any active files
        if oRequest.isSubRequestEmpty(ind,'integrity')['Value']:
          oRequest.setSubRequestStatus(ind,'integrity','Done')
          gMonitor.addMark( "Done", 1 )

      ################################################
      #  If the sub-request is already in terminal state
      else:
        gLogger.info("LFCvsSEAgent.execute: Sub-request %s is status '%s' and  not to be executed." % (ind,subRequestAttributes['Status']))

    ################################################
    #  Generate the new request string after operation
    requestString = oRequest.toXML()['Value']
    res = self.RequestDBClient.updateRequest(requestName,requestString,sourceServer)

    return S_OK()


