"""
RMSMonitoring type is used to monitor behaviour pattern of requests executed by RequestManagementSystem
inside DataManagementSystem/Agent/RequestOperations.

Understanding the key fields:
'objectType': This field describes the type of object which can be Request, Operation, or File.
'operationType': This field contains the type of operation being performed for eg. MoveReplica, PutAndRegister, etc.
'status': This field contains the status of the operation performed i.e. Attempted, Failed, or Successful.
'objectID': This field will be RequestID if Request object / OperationID if Operation object.
'parentID': This field will be RequestID if Operation object / OperationID if File object.

Understanding the monitoring fields:
'nbObject': This field is used to describe the number of objects in question during the operation.

In order to enable 'RMSMonitoring' we need to set value of 'EnableRMSMonitoring' flag to 'yes/true' inside
'/Systems/RequestManagement/<Instance>/Agents/RequestExecutingAgent' of the 'cfg' file.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType


class RMSMonitoring(BaseType):
  """
  .. class:: RMSMonitoring
  """

  def __init__(self):
    """ c'tor

    :param self: self reference
    """
    super(RMSMonitoring, self).__init__()

    self.keyFields = ["host", "objectType", "operationType", "status", "objectID", "parentID"]

    self.monitoringFields = ["nbObject"]

    self.addMapping({"host": {"type": "keyword"},
                     "objectType": {"type": "keyword"},
                     "operationType": {"type": "keyword"},
                     "status": {"type": "keyword"},
                     "objectID": {"type": "long"},
                     "parentID": {"type": "long"}})

    self.dataToKeep = 86400 * 30  # we need to define...

    self.period = "month"
    self.checkType()
