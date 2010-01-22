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
        
        :params:
          :attr:`args`:

          - args[0]: string - should be the name granularity ('Site' or 'Resource')

          - args[1]: string - should be the name of the entity in check

          - args[2]: string - should be the present status: a ValidStatus
        
          - args[3]: string - Only for resource, optional name of the site (if known)
        
          - args[4]: list - Only for resource, optional list of tests
        
          :attr:`commandIn`: an optional custom command object
          
          :attr:`knownInfo`: an optional dictionary with known infos 
      
        :return:
            { 
              'SAT':True|False|None, 
              'Status':Active|Probing|Banned, 
              'Reason':'SAMRes:ok|down|na|degraded|partial|maint',
            }
    """ 
   
    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluate)
    
    if args[2] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    if knownInfo is not None:
      if 'SAM-Status' in knownInfo.keys():
        status = knownInfo['SAM-Status']
    else:
      if commandIn is not None:
        command = commandIn
      else:
        # use standard Command
        from DIRAC.ResourceStatusSystem.Client.Command.SAMResults_Command import SAMResults_Command
        command = SAMResults_Command()
        
      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand(command)
      if len(args) == 3:
        status = clientsInvoker.doCommand((args[0], args[1]))
      if len(args) == 4:
        status = clientsInvoker.doCommand((args[0], args[1], args[3]))
      if len(args) == 5:
        status = clientsInvoker.doCommand((args[0], args[1], args[3], args[4]))
      
      status = status['SAM-Status']
    
    if status is None:
      return {'SAT':None}
    
    values = []
    for s in status.values():
      if s == 'ok':
        values.append(100)
      elif s == 'down':
        values.append(0)
      elif s == 'na':
        continue
      elif s == 'degraded':
        values.append(70)
      elif s == 'partial':
        values.append(30)
      elif s == 'maint':
        values.append(0)
    
    if len(values) == 0:
      status = 'na'
    else:
      mean = sum(values)/len(values)
      if mean >= 80:
        status = 'ok'
      elif mean >= 70:
        status = 'degraded'
      elif mean >= 30:
        status = 'partial'
      elif mean >= 10:
        status = 'maint'
      else:
        status = 'down'
        
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
