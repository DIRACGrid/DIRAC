""" Definition for WMSHistory Monitoring type.
    Drop-in replacement for the Accounting/WMSHistory accounting type.

    Filled by the agent "WorkloadManagement/StatesMonitoringAgent"
"""

from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType

__RCSID__ = "$Id$"

########################################################################


class WMSHistory(BaseType):

  """
  .. class:: WMSMonitorType
  """
  ########################################################################

  def __init__(self):
    super(WMSHistory, self).__init__()

    """ c'tor

    :param self: self reference
    """

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

    self.dataToKeep = 86400 * 30

    self.checkType()
