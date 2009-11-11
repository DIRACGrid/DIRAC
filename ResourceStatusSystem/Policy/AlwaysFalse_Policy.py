""" The AlwaysFalse_Policy class is a policy class that... checks nothing!
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker

class AlwaysFalse_Policy(PolicyBase):
  
  def evaluate(self, args, commandIn=None, knownInfo=None):
    """ does nothing.
        - args[0] should be a ValidRes
        - args[1] should be the name of the ValidRes
        - args[2] should be the present status
        
        Always returns:
            { 
              'SAT':False,
              'Status':args[2]
              'Reason':None 
            }
    """ 

    result = {'SAT':False, 'Status':args[2], 'Reason':'None'}
    return result
