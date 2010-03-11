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
              'EndDate':datetime (if needed)
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
      if len(args) == 3:
        status = clientsInvoker.doCommand((args[0], args[1]))
      elif len(args) == 4:
        status = clientsInvoker.doCommand((args[0], args[1], args[3]))
    
#    if len(args) == 3:
#      when = 'OnGoing'
#    elif len(args) == 4:
#      when = 'Scheduled'

    result = {}
    
    if args[2] == 'Active':
      if status['DT'] == 'None':
        result['SAT'] = False
        result['Status'] = 'Active'
      else:
        if 'OUTAGE' in status['DT']:
          result['SAT'] = True
          result['Status'] = 'Banned'
          result['EndDate'] = status['EndDate']
        elif 'AT_RISK' in status['DT']:
          result['SAT'] = True
          result['Status'] = 'Probing'
          result['EndDate'] = status['EndDate']
    
    elif args[2] == 'Probing':
      if status['DT'] == 'None':
        result['SAT'] = True
        result['Status'] = 'Active'
      else:
        if 'OUTAGE' in status['DT']:
          result['SAT'] = True
          result['Status'] = 'Banned'
          result['EndDate'] = status['EndDate']
        elif 'AT_RISK' in status['DT']:
          result['SAT'] = False
          result['Status'] = 'Probing'
          result['EndDate'] = status['EndDate']
      
    elif args[2] == 'Banned':
      if status['DT'] == 'None':
        result['SAT'] = True
        result['Status'] = 'Active'
      else:
        if 'OUTAGE' in status['DT']:
          result['SAT'] = False
          result['Status'] = 'Banned'
          result['EndDate'] = status['EndDate']
        elif 'AT_RISK' in status['DT']:
          result['SAT'] = True
          result['Status'] = 'Probing'
          result['EndDate'] = status['EndDate']
   
    if status['DT'] == 'None':
      result['Reason'] = 'No DownTime announced'
    else:
      result['Reason'] = 'DownTime found: %s' %status['DT']
    
    return result
