"""
    Module used for calling policies. Its class is used for invoking
    real policies, based on the policy name
"""

from DIRAC.ResourceStatusSystem.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Utilities.CS import getExt

class CommandCaller:

#############################################################################

  def commandInvocation(self, granularity = None, name = None, command = None,  
                        args = None, comm = None, extraArgs = None):
    
    c = command
#    a = args
    
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
    """ 
    Returns a command object, given comm
    
    :params:
      `comm`: a tuple, where comm[0] is a module name and comm[1] is a class name (inside the module)
    """ 

    moduleBase = "DIRAC.ResourceStatusSystem.Command."
    
    ext = getExt()
    
    # TRY FIRST TO IMPORT FROM DIRAC. AS IT IS NOW, THERE ARE MUCH MORE COMMANDS IN
    # DIRAC THAN IN THE EXTENSION. IT MIGHT CHANGE.
    
    try:
      cModule = comm[0]
      cClass = comm[1]
      module = moduleBase + cModule
      commandModule = __import__(module, globals(), locals(), ['*'])
    except ImportError:  
      try:
        cModule = comm[0]
        cClass = comm[1]
        module = ext + moduleBase + cModule
        commandModule = __import__(module, globals(), locals(), ['*'])
      except ImportError:
        cModule = "Command"
        cClass = "Command"
        module = moduleBase + cModule
        commandModule = __import__(module, globals(), locals(), ['*'])
      
    c = getattr(commandModule, cClass)()

    return c
  
#############################################################################
  
#  def setCommandArgs(self, comm):
#
#    if comm == 'GGUS_Link' or comm == 'GGUS_Info':
#      return (name, )
#
#    else:
#      return None
  
#############################################################################

  def setCommandClient(self, comm, cObj, RPCWMSAdmin = None, RPCAccounting = None):
    
    client = None
    
    if comm == 'JobsEffSimpleEveryOne_Command':
      from DIRAC.ResourceStatusSystem.Client.JobsClient import JobsClient
      client = JobsClient()
      cObj.setRPC(RPCWMSAdmin)

    elif comm == 'PilotsEffSimpleEverySites_Command':
      from DIRAC.ResourceStatusSystem.Client.PilotsClient import PilotsClient
      client = PilotsClient()
      cObj.setRPC(RPCWMSAdmin)
      
    elif comm in ('TransferQualityEverySEs_Command', 'TransferQualityEverySEsSplitted_Command'):
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      client = ReportsClient(rpcClient = RPCAccounting)
      cObj.setRPC(RPCAccounting)
      
    cObj.setClient(client)

#############################################################################
  
  def _innerCall(self, c, a):#, clientIn = None):
    """ command call
    """
    clientsInvoker = ClientsInvoker()
  
    c.setArgs(a)
#    c.setClient(clientIn)
    clientsInvoker.setCommand(c)
    
    res = clientsInvoker.doCommand()

    return res 
      
#############################################################################