"""  StorageUsageAgent takes the LFC as the primary source of information to determine storage usage.
"""
from DIRAC  import gLogger, gConfig, gMonitor, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.GridCredentials import setupProxy,restoreProxy,setDIRACGroup, getProxyTimeLeft
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Agent.NamespaceBrowser import NamespaceBrowser
from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog
from DIRAC.Core.Utilities.List import sortList

import time,os
from types import *


class StorageUsageAgent(Agent):

  def __init__(self):
    """ Standard constructor
    """
    AGENT_NAME = gConfig.getValue('/AgentName','DataManagement/StorageUsageAgent')
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.lfc = FileCatalog(['LcgFileCatalogCombined'])

    self.StorageUsageDB = RPCClient('DataManagement/StorageUsage')
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
      self.log.info("StorageUsageAgent.execute: Determining the length of the %s proxy." %self.proxyDN)
      obtainProxy = False
      if not os.path.exists(self.proxyLocation):
        self.log.info("StorageUsageAgent: No proxy found.")
        obtainProxy = True
      else:
        currentProxy = open(self.proxyLocation,'r')
        oldProxyStr = currentProxy.read()
        res = getProxyTimeLeft(oldProxyStr)
        if not res["OK"]:
          gLogger.error("StorageUsageAgent: Could not determine the time left for proxy.", res['Message'])
          return S_OK()
        proxyValidity = int(res['Value'])
        gLogger.debug("StorageUsageAgent: Current proxy found to be valid for %s seconds." % proxyValidity)
        self.log.info("StorageUsageAgent: %s proxy found to be valid for %s seconds."% (self.proxyDN,proxyValidity))
        if proxyValidity <= 6000:
          obtainProxy = True

      if obtainProxy:
        self.log.info("StorageUsageAgent: Attempting to renew %s proxy." %self.proxyDN)
        res = self.wmsAdmin.getProxy(self.proxyDN,self.proxyGroup,self.proxyLength)
        if not res['OK']:
          gLogger.error("StorageUsageAgent: Could not retrieve proxy from WMS Administrator", res['Message'])
          return S_OK()
        proxyStr = res['Value']
        if not os.path.exists(os.path.dirname(self.proxyLocation)):
          os.makedirs(os.path.dirname(self.proxyLocation))
        res = setupProxy(proxyStr,self.proxyLocation)
        if not res['OK']:
          gLogger.error("StorageUsageAgent: Could not create environment for proxy.", res['Message'])
          return S_OK()
        setDIRACGroup(self.proxyGroup)
        self.log.info("StorageUsageAgent: Successfully renewed %s proxy." %self.proxyDN)

    baseDir = gConfig.getValue(self.section+'/BaseDirectory','/lhcb')
    ignoreDirectories = gConfig.getValue(self.section+'/Ignore',[])

    oNamespaceBrowser = NamespaceBrowser(baseDir)
    gLogger.info("StorageUsageAgent: Initiating with %s as base directory." % baseDir)

    # Loop over all the directories and sub-directories
    while (oNamespaceBrowser.isActive()):
      currentDir = oNamespaceBrowser.getActiveDir()
      gLogger.info("StorageUsageAgent: Getting usage for %s." % currentDir)
      res = self.lfc.getDirectorySize(currentDir)
      if not res['OK']:
        gLogger.error("StorageUsageAgent: Completely failed to get usage.", "%s %s" % (currentDir,res['Message']))
        subDirs = [currentDir]
      elif res['Value']['Failed'].has_key(currentDir):
        gLogger.error("StorageUsageAgent: Failed to get usage.", "%s %s" % (currentDir,res['Value']['Failed'][currentDir]))
        subDirs = [currentDir]
      else:
        subDirs = res['Value']['Successful'][currentDir]['SubDirs']
        gLogger.info("StorageUsageAgent: Found %s sub-directories." % len(subDirs))
        numberOfFiles = int(res['Value']['Successful'][currentDir]['Files'])
        gLogger.info("StorageUsageAgent: Found %s files in the directory." % numberOfFiles)
        totalSize = long(res['Value']['Successful'][currentDir]['TotalSize'])

        siteUsage = res['Value']['Successful'][currentDir]['SiteUsage']

        if numberOfFiles > 0:
          res = self.StorageUsageDB.insertDirectory(currentDir,numberOfFiles,totalSize)
	  if not res['OK']:
            gLogger.error("StorageUsageAgent: Failed to insert the directory.", "%s %s" % (currentDir,res['Message']))
            subDirs = [currentDir] 
          else:
            gLogger.info("StorageUsageAgent: Successfully inserted directory.\n")
            gLogger.info("StorageUsageAgent: %s %s %s" % ('Storage Element'.ljust(40),'Number of files'.rjust(20),'Total size'.rjust(20)))
            for storageElement in sortList(siteUsage.keys()):
              usageDict = siteUsage[storageElement]
              res = self.StorageUsageDB.publishDirectoryUsage(currentDir,storageElement,long(usageDict['Size']),usageDict['Files'])
              if not res['OK']:
                gLogger.error("StorageUsageAgent: Failed to update the Storage Usage database.", "%s %s" % (storageElement,res['Message']))
                subDirs = [currentDir]
              else:
                gLogger.info("StorageUsageAgent: %s %s %s" % (storageElement.ljust(40),str(usageDict['Files']).rjust(20),str(usageDict['Size']).rjust(20)))

      
      chosenDirs = []
      for subDir in subDirs:
        if subDir not in ignoreDirectories:
          chosenDirs.append(subDir)
      oNamespaceBrowser.updateDirs(chosenDirs)
      gLogger.info("StorageUsageAgent: There are %s active directories to be searched." % oNamespaceBrowser.getNumberActiveDirs())

    gLogger.info("StorageUsageAgent: Finished recursive directory search.") 
    return S_OK()


