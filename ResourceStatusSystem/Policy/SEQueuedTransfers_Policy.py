""" The SEQueuedTransfers_Policy class is a policy class satisfied when a SE has a high number of
    queued transfers.
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class SEQueuedTransfers_Policy(PolicyBase):
  
  def evaluate(self, args, commandIn=None, knownInfo=None):
    """ evaluate policy on SE Queued Transfers, using args (tuple). 
        
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
              'Status':Active|Probing|Bad, 
              'Reason':'QueuedTransfers:High'|'QueuedTransfers:Mid-High'|'QueuedTransfers:Low',
            }
    """ 

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluate)
    
    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.evaluate)
    
    if args[2] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    if knownInfo is not None:
      if 'SLSInfo' in knownInfo.keys():
        status = knownInfo['SLSInfo']
    else:
      if commandIn is not None:
        command = commandIn
      else:
        # use standard Command
        from DIRAC.ResourceStatusSystem.Client.Command.SLS_Command import SLSServiceInfo_Command
        command = SLSServiceInfo_Command()
        
      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand(command)
      status = clientsInvoker.doCommand((args[0], args[1], args[3]))['SLSInfo']
      
    if status == 'Unknown':
      return {'SAT':'Unknown'}
    
    result = {}
    
    if status is None or status == -1:
      result['SAT'] = None

    status = status['Queued transfers']
    
    if args[2] == 'Active':
      if status > 100:
        result['SAT'] = True
        result['Status'] = 'Bad'
      elif status < 70:
        result['SAT'] = False
        result['Status'] = 'Active'
      else:
        result['SAT'] = True
        result['Status'] = 'Probing'
        
    elif args[2] == 'Probing':
      if status > 100:
        result['SAT'] = True
        result['Status'] = 'Bad'
      elif status < 70:
        result['SAT'] = True
        result['Status'] = 'Active'
      else:
        result['SAT'] = False
        result['Status'] = 'Probing'
    
    elif args[2] == 'Bad':
      if status > 100:
        result['SAT'] = False
        result['Status'] = 'Bad'
      elif status < 70:
        result['SAT'] = True
        result['Status'] = 'Active'
      else:
        result['SAT'] = True
        result['Status'] = 'Probing'
  
    elif args[2] == 'Banned':
      if status > 100:
        result['SAT'] = True
        result['Status'] = 'Bad'
      elif status < 70:
        result['SAT'] = True
        result['Status'] = 'Active'
      else:
        result['SAT'] = True
        result['Status'] = 'Probing'
  
    
    if status is not None and status != -1:
    
      result['Reason'] = "Queued transfers on the SE: %f -> " %status
    
      if status > 100:
        str = 'HIGH'
      elif status < 70:
        str = 'Low'
      else:
        str = 'Mid-High'
      
      result['Reason'] = result['Reason'] + str
      
    return result
