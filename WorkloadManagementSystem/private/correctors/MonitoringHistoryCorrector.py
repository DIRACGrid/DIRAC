""" MonitoringHistoryCorrector is the implementation class of the BaseHistoryCorrector
    which gets resources consumption history data from the ElasticSearch Monitoring
    database
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import datetime

from DIRAC import gLogger, S_OK
from DIRAC.WorkloadManagementSystem.private.correctors.BaseHistoryCorrector import BaseHistoryCorrector
from DIRAC.Core.Utilities import Time
from DIRAC.MonitoringSystem.Client.MonitoringClient import MonitoringClient


class MonitoringHistoryCorrector(BaseHistoryCorrector):

  def initialize(self):
    super(MonitoringHistoryCorrector, self).initialize()
    self.log = gLogger.getSubLogger("MonitoringHistoryCorrector")
    return S_OK()

  def _getHistoryData(self, timeSpan, groupToUse):
    """ Get history data from ElasticSearch Monitoring database

        :param int timeSpan: time span
        :param str groupToUse: requested user group
        :return: dictionary with history data
    """

    monitoringClient = MonitoringClient()

    reportCondition = {'Status': ['Running']}
    if not groupToUse:
      reportGrouping = 'UserGroup'
      reportCondition["grouping"] = ["UserGroup"]
    else:
      reportGrouping = 'User'
      reportCondition["UserGroup"] = groupToUse
      reportCondition["grouping"] = ["User"]

    now = Time.dateTime()
    result = monitoringClient.getReport('WMSHistory', 'AverageNumberOfJobs',
                                        now - datetime.timedelta(seconds=timeSpan), now,
                                        reportCondition, reportGrouping,
                                        {'lastSeconds': timeSpan})
    return result
