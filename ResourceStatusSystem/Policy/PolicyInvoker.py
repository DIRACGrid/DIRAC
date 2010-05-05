""" PolicyInvoker is the invoker for policies to be evaluated
"""

from DIRAC.ResourceStatusSystem.Utilities.Utils import *

#############################################################################

class PolicyInvoker:
  
  def setPolicy(self, p):
    """ set policy to p
    """
    self.policy = p
    
  def evaluatePolicy(self):
    """ call policy.evaluate()
    """
    
    return self.policy.evaluate()
  
#############################################################################
