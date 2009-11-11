""" The DoNothing_Policy class is a policy class that... checks nothing!
"""

from DIRAC.ResourceStatusSystem.PolicySystem.Policy.Policy import Policy
from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem import *

class DoNothing_Policy(Policy):
  
  def evaluate(self, args=None, commandIn=None, knownInfo=None):
    """ does nothing.
    
        Always returns:
            { 
              'SAT':False, 
            }
    """ 
    result = {'SAT':False, 'Reason':'None'}
    return result
