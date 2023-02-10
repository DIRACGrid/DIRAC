""" PolicyCaller

  Module used for calling policies. Its class is used for invoking
  real policies, based on the policy name.

"""
from DIRAC import S_ERROR
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ResourceStatusSystem.Command import CommandCaller


class PolicyCaller:
    """
    PolicyCaller loads policies, sets commands and runs them.
    """

    def __init__(self, clients=None):
        """
        Constructor
        """

        self.cCaller = CommandCaller

        self.clients = {}
        if clients is not None:
            self.clients = clients

    def policyInvocation(self, decisionParams, policyDict):
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

        if "module" not in policyDict:
            return S_ERROR(f"Malformed policyDict {policyDict}")
        pModuleName = policyDict["module"]

        if "command" not in policyDict:
            return S_ERROR(f"Malformed policyDict {policyDict}")
        pCommand = policyDict["command"]

        if "args" not in policyDict:
            return S_ERROR(f"Malformed policyDict {policyDict}")
        pArgs = policyDict["args"]

        result = ObjectLoader().loadObject(f"DIRAC.ResourceStatusSystem.Policy.{pModuleName}")
        if not result["OK"]:
            return result
        policyClass = result["Value"]
        policy = policyClass()

        command = self.cCaller.commandInvocation(pCommand, pArgs, decisionParams, self.clients)
        if not command["OK"]:
            return command
        command = command["Value"]

        evaluationResult = self.policyEvaluation(policy, command)

        if evaluationResult["OK"]:
            evaluationResult["Value"]["Policy"] = policyDict

        return evaluationResult

    @staticmethod
    def policyEvaluation(policy, command):
        """
        Method that given a policy and a command objects, assigns the second one as
        a member of the first and evaluates the policy.
        """

        policy.setCommand(command)
        evaluationResult = policy.evaluate()

        return evaluationResult
