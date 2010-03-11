""" The SEOccupancy_Policy class is a policy class satisfied when a site is in downtime, 
    or when a downtime is revoked
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class SEOccupancy_Policy(PolicyBase):
  
  def evaluate(self, args, commandIn=None, knownInfo=None):
    """ evaluate policy on SE occupancy, using args (tuple). 
        
        :params:
          :attr:`args`: a tuple 
            - `args[0]` should be a ValidRes

            - `args[1]` should be the name of the ValidRes

            - `args[2]` should be the present status
          
          :attr:`commandIn`: optional command object
          
          :attr:`knownInfo`: optional information dictionary
        
        
        :returns:
            { 
              'SAT':True|False, 
              'Status':Active|Probing|Banned, 
              'Reason':'SE_Occupancy:High'|'SE_Occupancy:Mid-High'|'SE_Occupancy:Low',
            }
    """ 

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluate)
    
    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.evaluate)
    
    if args[2] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    if knownInfo is not None:
      if 'SLS' in knownInfo.keys():
        status = knownInfo
    else:
      if commandIn is not None:
        command = commandIn
      else:
        # use standard Command
        from DIRAC.ResourceStatusSystem.Client.Command.SLS_Command import SLSStatus_Command
        command = SLSStatus_Command()
        
      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand(command)
      status = clientsInvoker.doCommand((args[0], args[1]))
    
    result = {}
    
    if args[2] == 'Active':
      if status['SLS'] is None:
        result['SAT'] = None
      else:
        if status['SLS'] > 15:
          result['SAT'] = False
          result['Status'] = 'Active'
          result['Reason'] = 'SE_Occupancy:Low'
        elif status['SLS'] <= 2:
          result['SAT'] = True
          result['Status'] = 'Banned'
          result['Reason'] = 'SE_Occupancy:High'
        else:
          result['SAT'] = True
          result['Status'] = 'Probing'
          result['Reason'] = 'SE_Occupancy:Mid-High'
          
    elif args[2] == 'Probing':
      if status['SLS'] is None:
        result['SAT'] = None
      else:
        if status['SLS'] > 15:
          result['SAT'] = True
          result['Status'] = 'Active'
          result['Reason'] = 'SE_Occupancy:Low'
        elif status['SLS'] <= 2:
          result['SAT'] = True
          result['Status'] = 'Banned'
          result['Reason'] = 'SE_Occupancy:High'
        else:
          result['SAT'] = False
          result['Status'] = 'Probing'
          result['Reason'] = 'SE_Occupancy:Mid-High'
      
    elif args[2] == 'Banned':
      if status['SLS'] is None:
        result['SAT'] = None
      else:
        if status['SLS'] > 15:
          result['SAT'] = True
          result['Status'] = 'Active'
          result['Reason'] = 'SE_Occupancy:Low'
        elif status['SLS'] <= 2:
          result['SAT'] = False
          result['Status'] = 'Banned'
          result['Reason'] = 'SE_Occupancy:High'
        else:
          result['SAT'] = True
          result['Status'] = 'Probing'
          result['Reason'] = 'SE_Occupancy:Mid-High'
    
    return result
