"""
    Module used for calling policies. Its class is used for invoking
    real policies, based on the policy name
"""

from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
#from DIRAC.ResourceStatusSystem.Policy import Configurations


class CommandCaller:

#############################################################################

  def commandInvocation(self, granularity = None, name = None, extra = None, 
                        command = None, args = None, comm = None):
    
    c = command
    a = args
    
    if comm == 'DT_link':
      if c is None:
        from DIRAC.ResourceStatusSystem.Client.Command.GOCDBStatus_Command import GOCDBInfo_Command 
        c = GOCDBInfo_Command()
      if a is None:
        a = (granularity, name)

    else:
      if c is None:
        from DIRAC.ResourceStatusSystem.Client.Command.Command import Command 
        c = Command()
      if a is None:
        a = ()


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