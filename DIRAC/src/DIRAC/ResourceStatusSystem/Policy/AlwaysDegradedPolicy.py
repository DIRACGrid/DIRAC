""" AlwaysDegradedPolicy module
"""
from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase


class AlwaysDegradedPolicy(PolicyBase):
    """
    The AlwaysDegradedPolicy is a dummy module that can be used as example, it
    always returns Degraded status.
    """

    @staticmethod
    def _evaluate(commandResult):
        """
        It returns Degraded status, evaluates the default command, but its output
        is completely ignored.
        """

        policyResult = {"Status": "Degraded", "Reason": "AlwaysDegraded"}

        return S_OK(policyResult)
