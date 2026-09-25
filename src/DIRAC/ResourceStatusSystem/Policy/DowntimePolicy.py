"""DowntimePolicy

Policy to evaluate the downtime status of a Site or Resource as reported by GOCDB.
The look-ahead window (``hours``) is fully configurable via the Operations CS under
``/Operations/Defaults/ResourceStatus/Policies/Downtime``.

"""

from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase


class DowntimePolicy(PolicyBase):
    """
    Policy that proposes a new status for a Site or Resource based on GOCDB downtime data.

    Whether the policy considers only ongoing downtimes or also scheduled ones within
    a future window is controlled by the ``hours`` command argument (default: 0 = ongoing only).
    """

    @staticmethod
    def _evaluate(commandResult):
        """
        Evaluate the downtime policy against the result of DowntimeCommand.

        Severity mapping:

        * No downtime (``None``) → **Active**
        * ``OUTAGE`` → **Banned**
        * ``WARNING`` → **Degraded**
        * any other severity → **Error**

        :param dict commandResult: S_OK / S_ERROR result from DowntimeCommand.
            On success the value is either ``None`` (no downtime) or a dict with at least
            ``Severity``, ``DowntimeID``, and ``Description`` keys.

        :returns: S_OK wrapping a dict ``{'Status': str, 'Reason': str}`` where Status is one of
            ``Error``, ``Active``, ``Banned``, ``Degraded``.
        """

        result = {"Status": None, "Reason": None}

        if not commandResult["OK"]:
            result["Status"] = "Error"
            result["Reason"] = commandResult["Message"]
            return S_OK(result)

        status = commandResult["Value"]

        if status is None:
            result["Status"] = "Active"
            result["Reason"] = "No DownTime announced"
            return S_OK(result)

        elif status["Severity"] == "OUTAGE":
            result["Status"] = "Banned"

        elif status["Severity"] == "WARNING":
            result["Status"] = "Degraded"

        else:
            _reason = f'DT_Policy: GOCDB returned an unknown value for DT: "{status["DowntimeID"]}"'
            result["Status"] = "Error"
            result["Reason"] = _reason
            return S_OK(result)

        # result[ 'EndDate' ] = status[ 'EndDate' ]
        result["Reason"] = f"{status['DowntimeID']} {status['Description']}"
        return S_OK(result)
