""" The ServiceStatus_Policy class is a policy class used to update the status of
    the service, based on statistics of nodes for that service
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class ServiceStatus_Policy(PolicyBase):
  
  def evaluate(self, args, commandIn=None, knownInfo=None):
    """ evaluate policy on Service Status, using args (tuple). 
        
        input:
          - args[0] should be the name of the service
          - args[1] should be the present status
        
        output:
            { 
              'SAT':True|False, 
              'Status':Active|Probing|Banned, 
              'Reason':'x out of y nodes in xxxx status',
            }
    """ 

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluate)
    
    if args[1] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    if knownInfo is not None:
      if '' in knownInfo.keys():
        status = knownInfo
    else:
      if commandIn is not None:
        command = commandIn
      else:
        # use standard Command
        from DIRAC.ResourceStatusSystem.Client.Command.Service_Command import ServiceStats_Command
        command = ServiceStats_Command()
        
      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand(command)
      status = clientsInvoker.doCommand((args[0], args[1]))
    

    result = {}
    
#    if args[2] == 'Active':
#      if status == None:
