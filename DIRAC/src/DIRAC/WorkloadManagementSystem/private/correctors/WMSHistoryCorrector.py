""" WMSHistoryCorrector is the implementation class of the BaseHistoryCorrector
    which gets resources consumption history data from the WMSHistory Accounting
    database
"""
import datetime

from DIRAC import gLogger, S_OK

from DIRAC.WorkloadManagementSystem.private.correctors.BaseHistoryCorrector import BaseHistoryCorrector
from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient


class WMSHistoryCorrector(BaseHistoryCorrector):
    def initialize(self):
        super().initialize()
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        return S_OK()

    def _getHistoryData(self, timeSpan, groupToUse):
        """Get history data from Accounting WMSHistory database

        :param int timeSpan: time span
        :param str groupToUse: requested user group
        :return: dictionary with history data
        """
        reportsClient = ReportsClient()

        reportCondition = {"Status": ["Running"]}
        if not groupToUse:
            reportGrouping = "UserGroup"
        else:
            reportGrouping = "User"
            reportCondition = {"UserGroup": groupToUse}
        now = datetime.datetime.utcnow()
        result = reportsClient.getReport(
            "WMSHistory",
            "AverageNumberOfJobs",
            now - datetime.timedelta(seconds=timeSpan),
            now,
            reportCondition,
            reportGrouping,
            {"lastSeconds": timeSpan},
        )
        return result
