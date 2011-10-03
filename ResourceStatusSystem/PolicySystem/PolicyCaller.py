"""
    Module used for calling policies. Its class is used for invoking
    real policies, based on the policy name
"""

from DIRAC.ResourceStatusSystem.PolicySystem.PolicyInvoker import PolicyInvoker

class PolicyCaller:

#############################################################################

  def __init__(self, commandCallerIn = None, **clients ):

    if commandCallerIn is not None:
      self.cc = commandCallerIn
    else:
      from DIRAC.ResourceStatusSystem.Command.CommandCaller import CommandCaller
      self.cc = CommandCaller()

    self.clients       = clients
    self.policyInvoker = PolicyInvoker()

#############################################################################

  def policyInvocation(self, VOExtension, granularity = None, name = None, status = None, policy = None,
                       args = None, pName = None, pModule = None, extraArgs = None, commandIn = None):
    """
    Invokes a policy:

    1. If :attr:`policy` is not None, import the right policy module,
    specified with :attr:`VOExtension` (e.g.: 'LHCb') and
    :attr:`pModule` (e.g. 'DT_Policy').

      1.1. Create a policy object.

    2. Set the policy arguments (usually :attr:`granularity`,
    :attr:`name`) + :attr:`extraArgs`.

    3. If commandIn is specified (normally it is), use
    :meth:`DIRAC.ResourceStatusSystem.Command.CommandCaller.CommandCaller.setCommandObject`
    to get a command object
    """

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
      a = (granularity, name)

    if extraArgs is not None:
      if isinstance(extraArgs, tuple):
        a = a + extraArgs
      elif isinstance(extraArgs, list):
        argsList = []
        for argsTuple in extraArgs:
          argsList.append(a + argsTuple)
        a = argsList

    if commandIn is not None:
      commandIn = self.cc.setCommandObject( commandIn )

      for clientName, clientInstance in self.clients.items():
        self.cc.setClient( commandIn, clientName, clientInstance )

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
