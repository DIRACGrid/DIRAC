"""
    Module used for calling policies. Its class is used for invoking
    real policies, based on the policy name
"""

from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker

class CommandCaller:

#############################################################################

  def commandInvocation(self, granularity = None, name = None, command = None,  
                        args = None, comm = None, extraArgs = None):
    
    c = command
    a = args
    
    if c is None: 
      c = self.setCommandObject(comm)

    a = (granularity, name)
#    if a is None:
#      a = self.setCommandArgs(comm)
#      if a is None:
#        a = (granularity, name)

    if extraArgs is not None:
      a = a + extraArgs

    res = self._innerCall(c, a)

    return res

    
#############################################################################

  def setCommandObject(self, comm):

    if comm == 'DT_Link':
      from DIRAC.ResourceStatusSystem.Client.Command.GOCDBStatus_Command import GOCDBInfo_Command 
      c = GOCDBInfo_Command()

    if comm == 'DT_Status':
      from DIRAC.ResourceStatusSystem.Client.Command.GOCDBStatus_Command import GOCDBStatus_Command 
      c = GOCDBStatus_Command()

    elif comm == 'GGUS_Link':
      from DIRAC.ResourceStatusSystem.Client.Command.GGUSTickets_Command import GGUSTickets_Link 
      c = GGUSTickets_Link()

    elif comm == 'GGUS_Info':
      from DIRAC.ResourceStatusSystem.Client.Command.GGUSTickets_Command import GGUSTickets_Info 
      c = GGUSTickets_Info()

    elif comm == 'GGUS_Open':
      from DIRAC.ResourceStatusSystem.Client.Command.GGUSTickets_Command import GGUSTickets_Open 
      c = GGUSTickets_Open()

    elif comm == 'JE_S':
      from DIRAC.ResourceStatusSystem.Client.Command.Jobs_Command import JobsEffSimple_Command
      c = JobsEffSimple_Command()

    elif comm == 'JE_S_Cached':
      from DIRAC.ResourceStatusSystem.Client.Command.Jobs_Command import JobsEffSimpleCached_Command
      c = JobsEffSimpleCached_Command()

    elif comm == 'PE_S':
      from DIRAC.ResourceStatusSystem.Client.Command.Pilots_Command import PilotsEffSimple_Command
      c = PilotsEffSimple_Command()

    elif comm == 'ServiceStats':
      from DIRAC.ResourceStatusSystem.Client.Command.RS_Command import ServiceStats_Command 
      c = ServiceStats_Command()

    elif comm == 'ResourceStats':
      from DIRAC.ResourceStatusSystem.Client.Command.RS_Command import ResourceStats_Command 
      c = ResourceStats_Command()

    elif comm == 'StorageElementsStats':
      from DIRAC.ResourceStatusSystem.Client.Command.RS_Command import StorageElementsStats_Command 
      c = StorageElementsStats_Command()

    elif comm == 'MonitoredStatus':
      from DIRAC.ResourceStatusSystem.Client.Command.RS_Command import MonitoredStatus_Command
      c = MonitoredStatus_Command()

    elif comm == 'SETransfer':
      from DIRAC.ResourceStatusSystem.Client.Command.DIRACAccounting_Command import TransferQuality_Command
      c = TransferQuality_Command()

    elif comm == 'DiracAccountingGraph':
      from DIRAC.ResourceStatusSystem.Client.Command.DIRACAccounting_Command import DIRACAccounting_Command 
      c = DIRACAccounting_Command()

    elif comm == 'SAM_Tests':
      from DIRAC.ResourceStatusSystem.Client.Command.SAMResults_Command import SAMResults_Command 
      c = SAMResults_Command()

    elif comm == 'SLS_Link':
      from DIRAC.ResourceStatusSystem.Client.Command.SLS_Command import SLSLink_Command 
      c = SLSLink_Command()

    elif comm == 'SLS_Status':
      from DIRAC.ResourceStatusSystem.Client.Command.SLS_Command import SLSStatus_Command 
      c = SLSStatus_Command()

    elif comm == 'SLS_ServiceInfo':
      from DIRAC.ResourceStatusSystem.Client.Command.SLS_Command import SLSServiceInfo_Command 
      c = SLSServiceInfo_Command()

    elif comm == 'JobsEffSimpleEveryOne':
      from DIRAC.ResourceStatusSystem.Client.Command.Collective_Command import JobsEffSimpleEveryOne_Command
      c = JobsEffSimpleEveryOne_Command()

    elif comm == 'PilotsEffSimpleEverySites':
      from DIRAC.ResourceStatusSystem.Client.Command.Collective_Command import PilotsEffSimpleEverySites_Command
      c = PilotsEffSimpleEverySites_Command()

    else:
      from DIRAC.ResourceStatusSystem.Client.Command.Command import Command 
      c = Command()

    return c
  
#############################################################################
  
  def setCommandArgs(self, comm):

    if comm == 'GGUS_Link' or comm == 'GGUS_Info':
      return (name, )

    else:
      return None
  
#############################################################################

  def setCommandClient(self, comm, cObj, RPCWMSAdmin = None):
    
    if comm == 'JobsEffSimpleEveryOne':
      from DIRAC.ResourceStatusSystem.Client.JobsClient import JobsClient
      client = JobsClient()
      cObj.setRPC(RPCWMSAdmin)

    elif comm == 'PilotsEffSimpleEverySites':
      from DIRAC.ResourceStatusSystem.Client.PilotsClient import PilotsClient
      client = PilotsClient()
      cObj.setRPC(RPCWMSAdmin)
      
    cObj.setClient(client)

#############################################################################
  
  def _innerCall(self, c, a, clientIn = None):
    """ command call
    """
    clientsInvoker = ClientsInvoker()
  
    clientsInvoker.setCommand(c)
    clientsInvoker.setArgs(a)
    clientsInvoker.setClient(clientIn)
    
    res = clientsInvoker.doCommand()
    return res 
      
#############################################################################