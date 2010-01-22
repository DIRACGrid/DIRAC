""" The DT_Policy class is a policy class satisfied when a site is in downtime, 
    or when a downtime is revoked
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class DT_Policy(PolicyBase):
  
  def evaluate(self, args, commandIn=None, knownInfo=None):
    """ evaluate policy on DT, using args (tuple). 
        
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
              'Reason':'DT:None'|'DT:OUTAGE|'DT:AT_RISK',
              'Enddate':datetime (if needed)
            }
    """ 

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluate)
    
    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.evaluate)
    
    if args[2] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    if knownInfo is not None:
      if 'DT' in knownInfo.keys():
        status = knownInfo
    else:
      if commandIn is not None:
        command = commandIn
      else:
        # use standard Command
        from DIRAC.ResourceStatusSystem.Client.Command.GOCDBStatus_Command import GOCDBStatus_Command
        command = GOCDBStatus_Command()
        
      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand(command)
      status = clientsInvoker.doCommand((args[0], args[1]))
    

    result = {}
    
    if args[2] == 'Active':
      if status['DT'] == 'None':
        result['SAT'] = False
        result['Status'] = 'Active'
        result['Reason'] = 'DT:None'
      else:
        if status['DT'] == 'OUTAGE':
          result['SAT'] = True
          result['Status'] = 'Banned'
          result['Reason'] = 'DT:OUTAGE'
          result['Enddate'] = status['Enddate']
        elif status['DT'] == 'AT_RISK':
          result['SAT'] = True
          result['Status'] = 'Probing'
          result['Reason'] = 'DT:AT_RISK'
          result['Enddate'] = status['Enddate']
#        elif status['DT'] == 'No Info':
#          result['SAT'] = None
    
    elif args[2] == 'Probing':
      if status['DT'] == 'None':
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] = 'DT:None'
      else:
        if status['DT'] == 'OUTAGE':
          result['SAT'] = True
          result['Status'] = 'Banned'
          result['Reason'] = 'DT:OUTAGE'
          result['Enddate'] = status['Enddate']
        elif status['DT'] == 'AT_RISK':
          result['SAT'] = False
          result['Status'] = 'Probing'
          result['Reason'] = 'DT:AT_RISK'
#        elif status['DT'] == 'No Info':
#          result['SAT'] = None
      
    elif args[2] == 'Banned':
      if status['DT'] == 'None':
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] = 'DT:None'
      else:
        if status['DT'] == 'OUTAGE':
          result['SAT'] = False
          result['Status'] = 'Banned'
          result['Reason'] = 'DT:OUTAGE'
        elif status['DT'] == 'AT_RISK':
          result['SAT'] = True
          result['Status'] = 'Probing'
          result['Reason'] = 'DT:AT_RISK'
          result['Enddate'] = status['Enddate']
#        elif status['DT'] == 'No Info':
#          result['SAT'] = None
   
    
    return result
