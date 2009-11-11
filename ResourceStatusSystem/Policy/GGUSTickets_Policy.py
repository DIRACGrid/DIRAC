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
        - args[0] should be the name of the Site
        - args[1] should be the present status
        
        returns:
            { 
              'SAT':True|False, 
              'Status':Active|Probing|Banned, 
              'Reason':'GGUSTickets: unsolved </> x',
            }
    """ 
   
    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluate)
    
    if args[1] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    if knownInfo is not None:
      if 'GGUSTickets' in knownInfo.keys():
        GGUSTickets = knownInfo['GGUSTickets']
    else:
      if commandIn is not None:
        command = commandIn
      else:
        # use standard Command
        from DIRAC.ResourceStatusSystem.Client.Command.GGUSTickets_Command import GGUSTickets_Command
        command = GGUSTickets_Command()
        
      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand(command)
      GGUSTickets = clientsInvoker.doCommand((args[0], args[1]))['GGUSTickets']
    
    result = {}
    
    if args[1] == 'Active':
      if GGUSTickets > Configurations.HIGH_TICKTES_NUMBER:
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] = 'GGUSTickets:unsolved > %d' %(Configurations.HIGH_TICKTES_NUMBER)
      else:
        result['SAT'] = False
        result['Status'] = 'Active'
        result['Reason'] = 'GGUSTickets:unsolved < %d' %(Configurations.HIGH_TICKTES_NUMBER)
    elif args[1] == 'Probing':
      if GGUSTickets > Configurations.HIGH_TICKTES_NUMBER:
        result['SAT'] = False
        result['Status'] = 'Probing'
        result['Reason'] = 'GGUSTickets:unsolved > %d' %(Configurations.HIGH_TICKTES_NUMBER)
      else:
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] = 'GGUSTickets:unsolved < %d' %(Configurations.HIGH_TICKTES_NUMBER)
    elif args[1] == 'Banned':
      if GGUSTickets > Configurations.HIGH_TICKTES_NUMBER:
        result['SAT'] = None
      else:
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] = 'GGUSTickets:unsolved < %d' %(Configurations.HIGH_TICKTES_NUMBER)

    return result