"""
    Module used for calling policies. Its class is used for invoking
    real policies, based on the policy name
"""

from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
#from DIRAC.ResourceStatusSystem.Policy import Configurations


class CommandCaller:

#############################################################################

  def commandInvocation(self, granularity = None, name = None, command = None,  
                        args = None, comm = None, extraArgs = None):
    
    c = command
    a = args
    
    if comm == 'DT_Link':
      if c is None:
        from DIRAC.ResourceStatusSystem.Client.Command.GOCDBStatus_Command import GOCDBInfo_Command 
        c = GOCDBInfo_Command()

    elif comm == 'GGUS_Link':
      if c is None:
        from DIRAC.ResourceStatusSystem.Client.Command.GGUSTickets_Command import GGUSTickets_Link 
        c = GGUSTickets_Link()
      if a is None:
        a = (name, )

    elif comm == 'GGUS_Info':
      if c is None:
        from DIRAC.ResourceStatusSystem.Client.Command.GGUSTickets_Command import GGUSTickets_Info 
        c = GGUSTickets_Info()
      if a is None:
        a = (name, )

    elif comm == 'DiracAccountingGraph':
      if c is None:
        from DIRAC.ResourceStatusSystem.Client.Command.DIRACAccounting_Command import DIRACAccounting_Command 
        c = DIRACAccounting_Command()

    elif comm == 'SAM_Tests':
      if c is None:
        from DIRAC.ResourceStatusSystem.Client.Command.SAMResults_Command import SAMResults_Command 
        c = SAMResults_Command()

    elif comm == 'SLS_Link':
      if c is None:
        from DIRAC.ResourceStatusSystem.Client.Command.SLS_Command import SLSLink_Command 
        c = SLSLink_Command()

    else:
      if c is None:
        from DIRAC.ResourceStatusSystem.Client.Command.Command import Command 
        c = Command()

    if a is None:
      a = (granularity, name)

    if extraArgs is not None:
      a = a + extraArgs

    res = self._innerCall(c, a)

    return res

    
#############################################################################

  def _innerCall(self, c, a, clientIn = None):
    """ command call
    """
    clientsInvoker = ClientsInvoker()
  
    clientsInvoker.setCommand(c)
    res = clientsInvoker.doCommand(a, clientIn = clientIn)
    return res 
      
#############################################################################