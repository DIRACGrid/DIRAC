########################################################################
# $HeadURL:  $
########################################################################
""" This agents feeds the ClientsCache table.
"""

import copy

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Utilities.CS import *

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import *
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Command.CommandCaller import CommandCaller
from DIRAC.ResourceStatusSystem.Command.ClientsInvoker import ClientsInvoker

__RCSID__ = "$Id:  $"

AGENT_NAME = 'ResourceStatus/ClientsCacheFeeder'

class ClientsCacheFeeder(AgentModule):

#############################################################################

  def initialize(self):
    """ ClientsCacheFeeder initialization
    """
    
    try:

      self.rsDB = ResourceStatusDB()
      
      self.clientsInvoker = ClientsInvoker()

      VOExtension = getExt()
      
      configModule = __import__(VOExtension+"DIRAC.ResourceStatusSystem.Policy.Configurations", 
                                globals(), locals(), ['*'])
      commandsList_ClientsCache = copy.deepcopy(configModule.Commands_ClientsCache)

      commandsList_AccountingCache = copy.deepcopy(configModule.Commands_AccountingCache)

      self.commandObjectsList_ClientsCache = []

      self.commandObjectsList_AccountingCache = []

      cc = CommandCaller()

      RPCWMSAdmin = RPCClient("WorkloadManagement/WMSAdministrator")
      RPCAccounting = RPCClient("Accounting/ReportGenerator")

      for command in commandsList_ClientsCache:
        cObj = cc.setCommandObject(command)
        cc.setCommandClient(command, cObj, RPCWMSAdmin = RPCWMSAdmin, 
                            RPCAccounting = RPCAccounting)
        self.commandObjectsList_ClientsCache.append((command, cObj))
        
      for command in commandsList_AccountingCache:
        cObj = cc.setCommandObject(command)
        cc.setCommandClient(command, cObj, RPCAccounting = RPCAccounting)
        self.commandObjectsList_AccountingCache.append((command, cObj))
        
      return S_OK()

    except Exception:
      errorStr = "ClientsCacheFeeder initialization"
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  def execute(self):
    """ The main ClientsCacheFeeder execution method
    """
    
    try:
      
      for co in self.commandObjectsList_ClientsCache:
        self.clientsInvoker.setCommand(co[1])
        res = self.clientsInvoker.doCommand()
        for key in res.keys():
          if 'ID' in res[key].keys():
            for value in res[key].keys():
              if value != 'ID':
                self.rsDB.addOrModifyClientsCacheRes(key, co[0][1].split('_')[0], 
                                                     value, res[key][value], res[key]['ID'])
          else:
            for value in res[key].keys():
              self.rsDB.addOrModifyClientsCacheRes(key, co[0][1].split('_')[0], 
                                                   value, res[key][value])
      
      for co in self.commandObjectsList_AccountingCache:
        self.clientsInvoker.setCommand(co[1])
        res = self.clientsInvoker.doCommand()
        plotType = res.keys()[0]
        for name in res[plotType].keys():
          self.rsDB.addOrModifyAccountingCacheRes(name, plotType, co[0][1].split('_')[0],
                                                  res[plotType][name])
      
      return S_OK()
    
    except Exception:
      errorStr = "ClientsCacheFeeder execution"
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################
