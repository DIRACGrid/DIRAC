# $HeadURL $
''' PolicyCaller

  Module used for calling policies. Its class is used for invoking
  real policies, based on the policy name.
  
'''

from DIRAC                                            import gLogger
from DIRAC.ResourceStatusSystem.Utilities             import Utils
from DIRAC.ResourceStatusSystem.Command.CommandCaller import CommandCaller

__RCSID__  = '$Id: $'

class PolicyCaller:
  def __init__( self, commandCallerIn = None, **clients ):
    self.cc      = commandCallerIn if commandCallerIn != None else CommandCaller()
    self.clients = clients

################################################################################

  def policyInvocation( self, granularity = None, name = None,
                        status = None, policy = None, args = None, pName = None,
                        pModule = None, extraArgs = None, commandIn = None ):
    """
    Invokes a policy:

    1. If :attr:`policy` is None, import the policy module specified
    with :attr:`pModule` (e.g. 'DT_Policy').

      1.1. Create a policy object.

    2. Set the policy arguments (usually :attr:`granularity`,
    :attr:`name`) + :attr:`extraArgs`.

    3. If commandIn is specified (normally it is), use
    :meth:`DIRAC.ResourceStatusSystem.Command.CommandCaller.CommandCaller.setCommandObject`
    to get a command object
    """

    p = policy
    a = args

    if not p:
      try:
        policyModule = Utils.voimport("DIRAC.ResourceStatusSystem.Policy." + pModule)
      except ImportError:
        gLogger.warn("Unable to import a policy module named %s, falling back on AlwaysFalse_Policy." % pModule)
        policyModule = __import__("DIRAC.ResourceStatusSystem.Policy.AlwaysFalse_Policy",
                                  globals(), locals(), ['*'])
        pModule = "AlwaysFalse_Policy"
      try:
        p = getattr(policyModule, pModule)()
      except AttributeError as exc:
        print policyModule, pModule
        raise exc

    if not a:
      a = (granularity, name)

    if extraArgs:
      a = a + tuple(extraArgs)

    if commandIn:
      commandIn = self.cc.setCommandObject( commandIn )
      for clientName, clientInstance in self.clients.items():
        self.cc.setAPI( commandIn, clientName, clientInstance )

    res = self._innerEval(p, a, commandIn = commandIn)
    # Just adding the PolicyName to the result of the evaluation of the policy
    res['PolicyName'] = pName
    return res

  def _innerEval(self, policy, arguments, commandIn = None):
    """Policy evaluation"""
    policy.setArgs(arguments)
    policy.setCommand(commandIn)
    return policy.evaluate()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
