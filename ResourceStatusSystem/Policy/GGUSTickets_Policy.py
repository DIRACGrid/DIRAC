""" The SAMResults_Policy class is a policy class that checks 
    the SAM job results
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Policy import Configurations

class GGUSTickets_Policy(PolicyBase):
  
  def evaluate(self, args, commandIn=None, knownInfo=None):
    """ evaluate policy on opened tickets, using args (tuple). 
        
        :params:
          :attr:`args`: a tuple 
            - args[0]: string - should be a ValidRes
        
            - args[1]: string - should be the name
        
            - args[2]: string - should be the status
        
        returns:
            { 
              'SAT':True|False, 
              'Status':Active|Probing|Banned, 
              'Reason':'GGUSTickets: n unsolved',
            }
    """ 
   
    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluate)
    
    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.evaluate)

    if args[2] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    if knownInfo is not None:
      if 'OpenT' in knownInfo.keys():
        GGUS_N = knownInfo['OpenT']
    else:
      if commandIn is not None:
        command = commandIn
      else:
        # use standard Command
        from DIRAC.ResourceStatusSystem.Client.Command.GGUSTickets_Command import GGUSTickets_Open
        command = GGUSTickets_Open()
        
      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand(command)
      GGUS_N = clientsInvoker.doCommand((args[1], ))['OpenT']
    
    result = {}
    
    if args[2] == 'Active':
      if GGUS_N >= 1:
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] = 'GGUSTickets unsolved: %d' %(GGUS_N)
      else:
        result['SAT'] = False
        result['Status'] = 'Active'
        result['Reason'] = 'NO GGUSTickets unsolved'
    elif args[2] == 'Probing':
      if GGUS_N >= 1:
        result['SAT'] = False
        result['Status'] = 'Probing'
        result['Reason'] = 'GGUSTickets unsolved: %d' %(GGUS_N)
      else:
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] = 'NO GGUSTickets unsolved'
    elif args[2] == 'Banned':
      if GGUS_N >= 1:
        result['SAT'] = False
        result['Status'] = 'Banned'
        result['Reason'] = 'GGUSTickets unsolved: %d' %(GGUS_N)
      else:
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] = 'NO GGUSTickets unsolved'

    return result