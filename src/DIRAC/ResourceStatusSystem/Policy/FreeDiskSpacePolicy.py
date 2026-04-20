"""FreeDiskSpacePolicy

Policy to evaluate the free disk space of a Storage Element.
The unit and thresholds (Banned_threshold, Degraded_threshold) are fully
configurable via the Operations CS under
``/Operations/Defaults/ResourceStatus/Policies/FreeDiskSpace``.

"""

from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase


class FreeDiskSpacePolicy(PolicyBase):
    """
    Policy that proposes a new status for a Storage Element based on its free disk space.

    The free space value and the thresholds (Banned_threshold, Degraded_threshold) are
    expressed in the same unit (TB, GB or MB) as configured for the FreeDiskSpace policy
    in the Operations CS. Default unit is TB; default thresholds are 0.1 (Banned) and 5
    (Degraded).
    """

    @staticmethod
    def _evaluate(commandResult):
        """
        Evaluate the free disk space policy.

        :param dict commandResult: S_OK / S_ERROR result from FreeDiskSpaceCommand.
            On success the value is expected to be a dict with keys:
            ``Free``, ``Total``, ``Banned_threshold``, ``Degraded_threshold``.

        :returns: S_OK wrapping a dict ``{'Status': str, 'Reason': str}`` where Status is one of
            ``Error``, ``Unknown``, ``Banned``, ``Degraded``, ``Active``.
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
        if free < commandResult["Banned_threshold"]:  # default: 0.1
            result["Status"] = "Banned"
            result["Reason"] = "Too little free space"
        elif free < commandResult["Degraded_threshold"]:  # default: 5
            result["Status"] = "Degraded"
            result["Reason"] = "Little free space"
        else:
            result["Status"] = "Active"
            result["Reason"] = "Enough free space"

        return S_OK(result)
