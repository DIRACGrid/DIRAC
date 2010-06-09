"""
    Module used for calling policies. Its class is used for invoking
    real policies, based on the policy name
"""

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyInvoker import PolicyInvoker

class PolicyCaller:

#############################################################################

  def __init__(self, commandCallerIn = None):
    
    if commandCallerIn is not None:
      self.cc = commandCallerIn
    else:
      from DIRAC.ResourceStatusSystem.Client.Command.CommandCaller import CommandCaller
      self.cc = CommandCaller()
      
    self.policyInvoker = PolicyInvoker() 

#############################################################################

  def policyInvocation(self, VOExtension, granularity = None, name = None, status = None, policy = None,  
                       args = None, pName = None, pModule = None, extraArgs = None, commandIn = None):
    
    p = policy
    a = args
    
    moduleBase = VOExtension + "DIRAC.ResourceStatusSystem.Policy."
    
    if p is None:
      try:
        module = moduleBase + pModule
        policyModule = __import__(module, globals(), locals(), ['*'])
      except ImportError:
        pModule = "AlwaysFalse_Policy"
        module = moduleBase + pModule
        policyModule = __import__(module, globals(), locals(), ['*'])
      p = getattr(policyModule, pModule)()
  
    if a is None:
      a = (granularity, name, status)

    if extraArgs is not None:
      if isinstance(extraArgs, tuple):
        a = a + extraArgs
      elif isinstance(extraArgs, list):
        argsList = []
        for argsTuple in extraArgs:
          argsList.append(a + argsTuple)
        a = argsList 
    
    if commandIn is not None:
      commandIn = self.cc.setCommandObject(commandIn)

    res = self._innerEval(p, a, commandIn = commandIn)

    res['PolicyName'] = pName
  
    return res

        
#############################################################################

  def _innerEval(self, p, a, commandIn = None, knownInfo = None):
    """ policy evaluation
    """

    self.policyInvoker.setPolicy(p)
    
    p.setArgs(a)
    p.setCommand(commandIn)
#    p.setInfoName('Result')
    
    res = self.policyInvoker.evaluatePolicy()
    return res 
      
#############################################################################