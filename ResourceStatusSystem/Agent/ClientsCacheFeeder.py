########################################################################
# $HeadURL:  $
########################################################################
""" This agents feeds the ClientsCache table.
"""

import copy

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger, gConfig
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

      VOExtension = gConfig.getValue("DIRAC/Extensions")

      if 'LHCb' in VOExtension:
        VOExtension = 'LHCb'
      
      configModule = __import__(VOExtension+"DIRAC.ResourceStatusSystem.Policy.Configurations", 
                                globals(), locals(), ['*'])
      commandsList = copy.deepcopy(configModule.Commands_to_use)

      self.commandObjectsList = []

      cc = CommandCaller()

      RPCWMSAdmin = RPCClient("WorkloadManagement/WMSAdministrator")
      RPCAccounting = RPCClient("Accounting/ReportGenerator")

      for command in commandsList:
        cObj = cc.setCommandObject(command)
        cc.setCommandClient(command, cObj, RPCWMSAdmin = RPCWMSAdmin, 
                            RPCAccounting = RPCAccounting)
        self.commandObjectsList.append((command, cObj))
        
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
      
      for co in self.commandObjectsList:
        self.clientsInvoker.setCommand(co[1])
        res = self.clientsInvoker.doCommand()
        for key in res.keys():
          for value in res[key].keys():
            self.rsDB.addOrModifyClientsCacheRes(key, co[0][1].split('_')[0], 
                                                 value, res[key][value])
      
      return S_OK()
    
    except Exception:
      errorStr = "ClientsCacheFeeder execution"
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################
