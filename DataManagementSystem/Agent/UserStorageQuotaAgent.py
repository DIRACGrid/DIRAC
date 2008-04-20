"""  UserStorageQuotaAgent determines the current storage usage for each user.\
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
from DIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB

import time,os
from types import *

AGENT_NAME = 'DataManagement/UserStorageQuota'

class UserStorageQuotaAgent(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)

    self.StorageUsageDB = StorageUsageDB()
    return result

  def execute(self):

    res = self.StorageUsageDB.getUserStorageUsage()
    usageDict = res['Value']
    usageToUserDict = {}

    byteToGB = 1000*1000*1000

    gLogger.info("%s %s" % ('User'.ljust(30),'Total usage'.ljust(30)))
    for userName in sortList(usageDict.keys()):
      gLogger.info("%s %s" % (userName.ljust(30),str(usageDict[userName]/byteToGB).ljust(30)))
      usageToUserDict[usageDict[userName]] = userName

    
    gLogger.info("Top ten users.")
    gLogger.info("%s %s" % ('User'.ljust(30),'Total usage'.ljust(30)))
    for usage in sortList(usageToUserDict.keys(),True):
      gLogger.info("%s %s" % (usageToUserDict[usage].ljust(30),str(usage/byteToGB).ljust(30)))
    return S_OK()


