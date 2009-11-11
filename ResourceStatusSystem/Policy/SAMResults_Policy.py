""" The SAMResults_Policy class is a policy class that checks 
    the SAM job results
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class SAMResults_Policy(PolicyBase):
  
  def evaluate(self, args, commandIn=None, knownInfo=None):
    """ evaluate policy on SAM jobs results, using args (tuple). 
        - args[0] should be the name of the Site
        - args[1] should be the name of the Resource
        - args[2] should be the present status
        
        returns:
            { 
              'SAT':True|False, 
              'Status':Active|Probing|Banned, 
              'Reason':'SAMRes:ok|down|na|degraded|partial|maint',
            }
    """ 
   
    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluate)
    
    if args[2] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    if knownInfo is not None:
      if 'Status' in knownInfo.keys():
        status = knownInfo['Status']
    else:
      if commandIn is not None:
        command = commandIn
      else:
        # use standard Command
        from DIRAC.ResourceStatusSystem.Client.Command.SAMResults_Command import SAMResults_Command
        command = SAMResults_Command()
        
      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand(command)
      status = clientsInvoker.doCommand((args[0], args[1]))['Status']
    
    
    result = {}
    
    if args[2] == 'Active':
      if status == 'ok':
        result['SAT'] = False
        result['Status'] = 'Active'
        result['Reason'] = 'SAM:ok'
      elif status == 'down':
        result['SAT'] = True
        result['Status'] = 'Banned'
        result['Reason'] = 'SAM:down'
      elif status == 'na':
        result['SAT'] = None
      elif status == 'degraded':
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] = 'SAM:degraded'
      elif status == 'partial':
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] = 'SAM:partial'
      elif status == 'maint':
        result['SAT'] = True
        result['Status'] = 'Banned'
        result['Reason'] = 'SAM:maint'
      
    elif args[2] == 'Probing':
      if status == 'ok':
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] = 'SAM:ok'
      elif status == 'down':
        result['SAT'] = True
        result['Status'] = 'Banned'
        result['Reason'] = 'SAM:down'
      elif status == 'na':
        result['SAT'] = None
      elif status == 'degraded':
        result['SAT'] = False
        result['Status'] = 'Probing'
        result['Reason'] = 'SAM:degraded'
      elif status == 'partial':
        result['SAT'] = False
        result['Status'] = 'Probing'
        result['Reason'] = 'SAM:partial'
      elif status == 'maint':
        result['SAT'] = True
        result['Status'] = 'Banned'
        result['Reason'] = 'SAM:maint'
      
    elif args[2] == 'Banned':
      if status == 'ok':
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] = 'SAM:ok'
      elif status == 'down':
        result['SAT'] = False
        result['Status'] = 'Banned'
        result['Reason'] = 'SAM:down'
      elif status == 'na':
        result['SAT'] = None
      elif status == 'degraded':
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] = 'SAM:degraded'
      elif status == 'partial':
        result['SAT'] = False
        result['Status'] = 'Banned'
        result['Reason'] = 'SAM:partial'
      elif status == 'maint':
        result['SAT'] = False
        result['Status'] = 'Banned'
        result['Reason'] = 'SAM:maint'
      
    return result
