"""
ComponentMonitoring type used to monitor DIRAC components.
"""

from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType

__RCSID__ = "$Id $"

########################################################################


class ComponentMonitoring(BaseType):

  """
  .. class:: ComponentMonitoring
  """
  ########################################################################

  def __init__(self):
    super(ComponentMonitoring, self).__init__()

    """ c'tor

    :param self: self reference
    """

    self.keyFields = ['host', 'component', 'pid', 'status',
                      'site', 'componentType', 'componentName', 'componentLocation']

    self.monitoringFields = ['runningTime', 'memoryUsage', 'threads', 'cpuUsage',
                             'Connections', 'Queries', 'PendingQueries',
                             'ActiveQueries', 'RunningThreads', 'MaxFD']

    self.doc_type = "ComponentMonitoring"

    self.addMapping({"host": {"type": "keyword"}, "component": {"type": "keyword"}, "status": {"type": "keyword"},
                     "site": {"type": "keyword"}, "componentType": {"type": "keyword"},
                     "componentName": {"type": "keyword"}, "componentLocation": {"type": "keyword"}})

    self.dataToKeep = 86400 * 30  # we need to define...

    self.period = "month"
    self.checkType()
