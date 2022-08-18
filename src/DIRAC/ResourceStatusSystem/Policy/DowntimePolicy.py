""" DowntimePolicy module
"""
from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase


class DowntimePolicy(PolicyBase):
    """The DowntimePolicy checks for downtimes, scheduled or ongoing, depending on the command parameters."""

    @staticmethod
    def _evaluate(commandResult):
        """It returns Active status if there is no downtime announced.
        Banned if the element is in OUTAGE.
        Degraded if it is on WARNING status.
        Otherwise, it returns error.
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
            _reason = 'DT_Policy: GOCDB returned an unknown value for DT: "%s"' % status["DowntimeID"]
            result["Status"] = "Error"
            result["Reason"] = _reason
            return S_OK(result)

        # result[ 'EndDate' ] = status[ 'EndDate' ]
        result["Reason"] = "{} {}".format(status["DowntimeID"], status["Description"])
        return S_OK(result)
