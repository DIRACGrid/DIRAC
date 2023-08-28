""" FreeDiskSpacePolicy

   FreeDiskSpacePolicy.__bases__:
     DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase.PolicyBase

"""
from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase


class FreeDiskSpacePolicy(PolicyBase):
    """
    The FreeDiskSpacePolicy class is a policy class satisfied when a SE has a
    low occupancy.

    FreeDiskSpacePolicy, given the space left at the element, proposes a new status.
    """

    @staticmethod
    def _evaluate(commandResult):
        """
        Evaluate policy on SE occupancy: Use FreeDiskSpaceCommand

        :Parameters:
          **commandResult** - S_OK / S_ERROR
            result of the command. It is expected ( iff S_OK ) a dictionary like
            { 'Total' : .., 'Free' : ..}

        :return:
          {
            'Status':Error|Active|Bad|Banned,
            'Reason': Some lame statements that have to be updated
          }
        """

        result = {}

        if not commandResult["OK"]:
            result["Status"] = "Error"
            result["Reason"] = commandResult["Message"]
            return S_OK(result)

        commandResult = commandResult["Value"]

        if not commandResult:
            result["Status"] = "Unknown"
            result["Reason"] = "No values to take a decision"
            return S_OK(result)

        for key in ["Total", "Free"]:
            if key not in commandResult:
                result["Status"] = "Error"
                result["Reason"] = f"Key {key} missing"
                return S_OK(result)

        free = float(commandResult["Free"])

        # Units (TB, GB, MB) may change,
        # depending on the configuration of the command in Configurations.py
        if free < 0.1:
            result["Status"] = "Banned"
            result["Reason"] = "Too little free space"
        elif free < 5:
            result["Status"] = "Degraded"
            result["Reason"] = "Little free space"
        else:
            result["Status"] = "Active"
            result["Reason"] = "Enough free space"

        return S_OK(result)
