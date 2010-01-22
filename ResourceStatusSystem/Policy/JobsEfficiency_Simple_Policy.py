""" The JobsEfficiency_Simple_Policy class is a policy class 
    that checks the efficiency of the pilots
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Policy import Configurations

class JobsEfficiency_Simple_Policy(PolicyBase):
  
  def evaluate(self, args, knownInfo=None, commandIn=None):
    """ evaluate policy on jobs stats, using args (tuple). 
        
        :params:
          :attr:`args`:
            - args[0]: string - should be a ValidRes

            - args[1]: string - should be the name of the ValidRes
    
            - args[2]: string - should be the present status (ValidStatus)
        
        returns:
            { 
              'SAT':True|False, 
              'Status':Active|Probing|Banned, 
              'Reason':'JobsEff:Good|JobsEff:Fair|JobsEff:Poor|JobsEff:Bad|JobsEff:Idle',
            }
    """ 

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluate)
    
    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.evaluate)
    
    if args[2] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    if knownInfo is not None:
      if 'JobsEff' in knownInfo.keys():
        status = knownInfo['JobsEff']
    else:
      if commandIn is not None:
        command = commandIn
      else:
        # use standard command
        from DIRAC.ResourceStatusSystem.Client.Command.Jobs_Command import JobsEffSimple_Command
        command = JobsEffSimple_Command()
        
      clientsInvoker = ClientsInvoker()
      clientsInvoker.setCommand(command)
      status = clientsInvoker.doCommand((args[0], args[1]))
      
      status = status['JobsEff']
    
    result = {}

    if args[2] == 'Active':
      if status == 'Good':
        result['SAT'] = False
        result['Status'] = 'Active'
        result['Reason'] = 'JobsEff:Good'
      elif status == 'Fair':
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] = 'JobsEff:Fair'
      elif status == 'Poor':
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] = 'JobsEff:Poor'
      elif status == 'Idle':
        result['SAT'] = None
      elif status == 'Bad':
        result['SAT'] = True
        result['Status'] = 'Banned'
        result['Reason'] = 'JobsEff:Bad'
    elif args[2] == 'Probing':
      if status == 'Good':
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] = 'JobsEff:Good'
      elif status == 'Fair':
        result['SAT'] = False
        result['Status'] = 'Probing'
        result['Reason'] = 'JobsEff:Fair'
      elif status == 'Poor':
        result['SAT'] = False
        result['Status'] = 'Probing'
        result['Reason'] = 'JobsEff:Poor'
      elif status == 'Idle':
        result['SAT'] = None
      elif status == 'Bad':
        result['SAT'] = True
        result['Status'] = 'Banned'
        result['Reason'] = 'JobsEff:Bad'
    elif args[2] == 'Banned':
      if status == 'Good':
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] = 'JobsEff:Good'
      elif status == 'Fair':
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] = 'JobsEff:Fair'
      elif status == 'Poor':
        result['SAT'] = True
        result['Status'] = 'Probing'
        result['Reason'] = 'JobsEff:Poor'
      elif status == 'Idle':
        result['SAT'] = None
      elif status == 'Bad':
        result['SAT'] = False
        result['Status'] = 'Banned'
        result['Reason'] = 'JobsEff:Bad'

    return result