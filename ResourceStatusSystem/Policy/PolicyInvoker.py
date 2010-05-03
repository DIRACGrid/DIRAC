""" PolicyInvoker is the invoker for policies to be evaluated
"""

from DIRAC.ResourceStatusSystem.Utilities.Utils import *

#############################################################################

class PolicyInvoker:
  
  def setPolicy(self, p):
    """ set policy to p
    """
    self.policy = p
    
  def evaluatePolicy(self, args, commandIn = None, knownInfo=None):
    """ call policy.evaluate()
    """
    
    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluatePolicy)
    
    return self.policy.evaluate(args, commandIn = commandIn, knownInfo = knownInfo)
  
#############################################################################
