"""FreeDiskSpacePolicy

Policy to evaluate the free disk space of a Storage Element.
The unit, thresholds (Banned_threshold, Degraded_threshold), and fractions
(Banned_fraction, Degraded_fraction) are fully configurable via the Operations CS under
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

    The fractions (Banned_fraction, Degraded_fraction) are expressed as a fraction of the
    total space. Default fractions are 0.01 (1% for Banned) and 0.05 (5% for Degraded).
    The SE is set to Banned or Degraded if EITHER the absolute threshold OR the fraction
    threshold is exceeded.
    """

    @staticmethod
    def _evaluate(commandResult):
        """
        Evaluate the free disk space policy.

        :param dict commandResult: S_OK / S_ERROR result from FreeDiskSpaceCommand.
            On success the value is expected to be a dict with keys:
            ``Free``, ``Total``, ``Banned_threshold``, ``Degraded_threshold``,
            ``Banned_fraction``, ``Degraded_fraction``.

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
        total = float(commandResult["Total"])

        # Calculate the fraction of free space
        freeFraction = free / total if total > 0 else 0.0

        # Get thresholds (with safe defaults for backward compatibility)
        banned_threshold = commandResult.get("Banned_threshold", 0.1)
        degraded_threshold = commandResult.get("Degraded_threshold", 5)
        banned_fraction = commandResult.get("Banned_fraction", 0.01)
        degraded_fraction = commandResult.get("Degraded_fraction", 0.05)

        # Check Banned conditions: absolute threshold OR fraction threshold
        if free < banned_threshold or freeFraction < banned_fraction:
            result["Status"] = "Banned"
            result["Reason"] = "Too little free space"
        # Check Degraded conditions: absolute threshold OR fraction threshold
        elif free < degraded_threshold or freeFraction < degraded_fraction:
            result["Status"] = "Degraded"
            result["Reason"] = "Little free space"
        else:
            result["Status"] = "Active"
            result["Reason"] = "Enough free space"

        return S_OK(result)
