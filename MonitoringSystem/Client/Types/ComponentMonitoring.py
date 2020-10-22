"""
ComponentMonitoring type used to monitor DIRAC components.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType

__RCSID__ = "$Id$"


class ComponentMonitoring(BaseType):
  """
  .. class:: ComponentMonitoring
  """

  def __init__(self):
    """ c'tor

    :param self: self reference
    """

    super(ComponentMonitoring, self).__init__()

    self.keyFields = ['host', 'component', 'pid', 'status',
                      'componentType', 'componentLocation']

    self.monitoringFields = ['runningTime', 'memoryUsage', 'threads', 'cpuPercentage',
                             'Connections', 'PendingQueries', 'ActiveQueries',
                             'RunningThreads', 'MaxFD', 'ServiceResponseTime',
                             'cycleDuration', 'cycles']

    self.addMapping({"host": {"type": "keyword"},
                     "component": {"type": "keyword"},
                     "status": {"type": "keyword"},
                     "componentType": {"type": "keyword"},
                     "componentLocation": {"type": "keyword"}})

    self.dataToKeep = 86400 * 30  # we need to define...

    self.period = "month"
    self.checkType()
