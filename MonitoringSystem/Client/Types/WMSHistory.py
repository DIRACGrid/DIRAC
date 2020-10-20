"""
This class is a helper to create the proper index and insert the proper values....
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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
