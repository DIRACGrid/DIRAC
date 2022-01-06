""" AlwaysBannedPolicy module
"""
from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase


class AlwaysBannedPolicy(PolicyBase):
    """
    The AlwaysBannedPolicy is a dummy module that can be used as example, it
    always returns Banned status.
    """

    @staticmethod
    def _evaluate(commandResult):
        """
        It returns Banned status, evaluates the default command, but its output
        is completely ignored.
        """

        policyResult = {"Status": "Banned", "Reason": "AlwaysBanned"}

        return S_OK(policyResult)
