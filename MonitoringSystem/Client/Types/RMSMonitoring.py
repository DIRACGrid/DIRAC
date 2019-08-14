"""
RMSMonitoring type used to monitor behaviour pattern of requests executed by RequestManagementSystem.
"""

from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType

__RCSID__ = "$Id $"

########################################################################


class RMSMonitoring(BaseType):

  """
  .. class:: RMSMonitoring
  """
  ########################################################################

  def __init__(self):
    super(RMSMonitoring, self).__init__()

    """ c'tor

    :param self: self reference
    """

    self.keyFields = ["host", "objectType", "operationType", "status", "objectID", "parentID"]

    self.monitoringFields = ["nbObject"]

    self.doc_type = "RMSMonitoring"

    self.addMapping({"host": {"type": "keyword"}, "objecType": {"type": "keyword"},
                     "operationType": {"type": "keyword"}, "status": {"type": "keyword"},
                     "objectID": {"type": "long"}, "parentID": {"type": "long"}})

    self.dataToKeep = 86400 * 30  # we need to define...

    self.period = "month"
    self.checkType()
