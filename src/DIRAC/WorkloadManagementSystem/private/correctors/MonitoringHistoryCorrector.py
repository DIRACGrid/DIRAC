""" MonitoringHistoryCorrector is the implementation class of the BaseHistoryCorrector
    which gets resources consumption history data from the OpenSearch Monitoring
    database
"""
import datetime

from DIRAC import gLogger, S_OK
from DIRAC.WorkloadManagementSystem.private.correctors.BaseHistoryCorrector import BaseHistoryCorrector
from DIRAC.MonitoringSystem.Client.MonitoringClient import MonitoringClient


class MonitoringHistoryCorrector(BaseHistoryCorrector):
    def initialize(self):
        super().initialize()
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        return S_OK()

    def _getHistoryData(self, timeSpan, groupToUse):
        """Get history data from OpenSearch Monitoring database

        :param int timeSpan: time span
        :param str groupToUse: requested user group
        :return: dictionary with history data
        """

        monitoringClient = MonitoringClient()

        reportCondition = {"Status": ["Running"]}
        if not groupToUse:
            reportGrouping = "UserGroup"
            reportCondition["grouping"] = ["UserGroup"]
        else:
            reportGrouping = "User"
            reportCondition["UserGroup"] = groupToUse
            reportCondition["grouping"] = ["User"]

        now = datetime.datetime.utcnow()
        result = monitoringClient.getReport(
            "WMSHistory",
            "AverageNumberOfJobs",
            now - datetime.timedelta(seconds=timeSpan),
            now,
            reportCondition,
            reportGrouping,
            {"lastSeconds": timeSpan},
        )
        return result
