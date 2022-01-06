""" DIRAC.ResourceStatusSystem.Policy.PropagationPolicy

    The following lines are needed in the CS::

      PropagationPolicy
      {
        matchParams
        {
          element = Site
        }
        policyType = PropagationPolicy
      }

"""
from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase


class PropagationPolicy(PolicyBase):
    """
    PropagationPolicy module doc
    """

    @staticmethod
    def _evaluate(commandResult):
        """
        commandResult is the result of 'PropagationCommand' which
        indicates if a site should be 'Active' or 'Banned'

        :returns:
           {
           `Status`:Error|Unknown|Active|Banned,
           `Reason`:'A:X/P:Y/B:Z'
           }
        """

        result = {"Status": None, "Reason": None}

        if not commandResult["OK"]:

            result["Status"] = "Error"
            result["Reason"] = commandResult["Message"]
            return S_OK(result)

        else:

            commandResult = commandResult["Value"]

            result["Status"] = commandResult["Status"]
            result["Reason"] = commandResult["Reason"]
            return S_OK(result)
