"""  UserStorageUsageAgent simply inherits the StorageUsage agent and loops over the /lhcb/user directory
"""
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/Agent/UserStorageUsageAgent.py,v 1.1 2009/08/11 15:00:44 acsmith Exp $
__RCSID__ = "$Id: UserStorageUsageAgent.py,v 1.1 2009/08/11 15:00:44 acsmith Exp $"

from DIRAC  import gLogger, gMonitor, S_OK, S_ERROR, rootPath
from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv
from DIRAC.Core.Utilities.List import sortList

from DIRAC.DataManagementSystem.Agent.NamespaceBrowser import NamespaceBrowser
from DIRAC.DataManagementSystem.Client.ReplicaManager import CatalogDirectory
from DIRAC.DataManagementSystem.Agent.StorageUsageAgent import StorageUsageAgent

import time,os
from types import *

AGENT_NAME = 'DataManagement/UserStorageUsageAgent'

class UserStorageUsageAgent(StorageUsageAgent):

  def initialize(self):
    self.catalog = CatalogDirectory()
    self.StorageUsageDB = RPCClient('DataManagement/StorageUsage')
    self.am_setModuleParam("shifterProxy", "DataManager")
    self.am_setModuleParam("shifterProxyLocation","%s/runit/%s/proxy" % (rootPath,AGENT_NAME))
    return S_OK()

  def removeEmptyDir(self,directory):
    gLogger.info("removeEmptyDir: Not removing user owned empty directory.")
    return S_OK()
