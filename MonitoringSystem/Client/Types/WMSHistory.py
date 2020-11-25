""" Definition for WMSHistory Monitoring type.
    Drop-in replacement for the Accounting/WMSHistory accounting type.

    Filled by the agent "WorkloadManagement/StatesMonitoringAgent"
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType

__RCSID__ = "$Id$"


class WMSHistory(BaseType):

  """
  .. class:: WMSMonitorType
  """

  def __init__(self):
    """ c'tor

    :param self: self reference
    """

    super(WMSHistory, self).__init__()

    self.keyFields = ['Status', 'Site', 'User', 'UserGroup',
                      'JobGroup', 'MinorStatus', 'ApplicationStatus',
                      'JobSplitType']

    self.monitoringFields = ['Jobs', 'Reschedules']

    self.index = 'wmshistory_index'

    self.addMapping({'Status': {'type': 'keyword'},
                     'Site': {'type': 'keyword'},
                     'JobSplitType': {'type': 'keyword'},
                     'ApplicationStatus': {'type': 'keyword'},
                     'MinorStatus': {'type': 'keyword'},
                     'User': {'type': 'keyword'},
                     'JobGroup': {'type': 'keyword'},
                     'UserGroup': {'type': 'keyword'}})
    # {'timestamp': {'type': 'date'}} will be added for all monitoring types

    self.dataToKeep = 86400 * 30

    self.checkType()
