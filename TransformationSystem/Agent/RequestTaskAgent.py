"""The Request Task Agent takes request tasks created in the
TransformationDB and submits to the request management system.

+----------------------+---------------------------------------+-------------------------------------------------------+
| **Name**             | **Description**                       | **Example**                                           |
+----------------------+---------------------------------------+-------------------------------------------------------+
| *shifterProxy*       | Use a dedicated proxy to submit jobs  | DataManager                                           |
|                      | to the WMS                            |                                                       |
+----------------------+---------------------------------------+-------------------------------------------------------+
| *ShifterCredentials* | Use delegated credentials, same values|                                                       |
|                      |as for                                 |                                                       |
|                      | shifterProxy, but there will not be   |                                                       |
|                      |any actual                             |                                                       |
|                      | proxy used. (New in v6r21)            |                                                       |
|                      |                                       |                                                       |
+----------------------+---------------------------------------+-------------------------------------------------------+
| *TransType*          |                                       |                                                       |
+----------------------+---------------------------------------+-------------------------------------------------------+
| *PluginLocation*     |                                       |  DIRAC.TransformationSystem.Client.TaskManagerPlugin  |
+----------------------+---------------------------------------+-------------------------------------------------------+
| *maxNumberOfThreads* |                                       | 15                                                    |
+----------------------+---------------------------------------+-------------------------------------------------------+
| *TasksPerLoop*       |                                       |                                                       |
+----------------------+---------------------------------------+-------------------------------------------------------+
| *TaskUpdateStatus*   |                                       | Checking, Deleted, Killed, Staging, Stalled, Matched, |
|                      |                                       | Scheduled, Rescheduled, Completed, Submitted,         |
|                      |                                       | Assigned, Received, Waiting, Running                  |
+----------------------+---------------------------------------+-------------------------------------------------------+
| *SubmitTasks*        |                                       |                                                       |
+----------------------+---------------------------------------+-------------------------------------------------------+
| *SubmitStatus*       |                                       | Active, Completing                                    |
+----------------------+---------------------------------------+-------------------------------------------------------+
| *MonitorTasks*       |                                       |                                                       |
+----------------------+---------------------------------------+-------------------------------------------------------+
| *MonitorFiles*       |                                       |                                                       |
+----------------------+---------------------------------------+-------------------------------------------------------+
| *CheckReserved*      |                                       |                                                       |
+----------------------+---------------------------------------+-------------------------------------------------------+
| *CheckReservedStatus*|                                       | Active, Completing, Stopped                           |
+----------------------+---------------------------------------+-------------------------------------------------------+
| *UpdateTaskStatus*   |                                       | Active, Completing, Stopped                           |
+----------------------+---------------------------------------+-------------------------------------------------------+
| *UpdateFileStatus*   |                                       | Active, Completing, Stopped                           |
+----------------------+---------------------------------------+-------------------------------------------------------+

.. versionadded:: v6r21

 It is possible to run the RequestTaskAgent without a *shifterProxy* or
 *ShifterCredentials*, in this case the credentials of the authors of the
 transformations are used to submit the jobs to the RMS. This enables the use of
 a single RequestTaskAgent for multiple VOs. See also the section about the
 :ref:`trans-multi-vo`.

"""

from DIRAC import S_OK

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase import TaskManagerAgentBase
from DIRAC.TransformationSystem.Client.TaskManager import RequestTasks

__RCSID__ = "$Id$"

AGENT_NAME = 'Transformation/RequestTaskAgent'


class RequestTaskAgent(TaskManagerAgentBase):
  """ An AgentModule to submit requests tasks
  """

  def __init__(self, *args, **kwargs):
    """ c'tor
    """
    TaskManagerAgentBase.__init__(self, *args, **kwargs)

    self.transType = []
    self.taskManager = None

  def initialize(self):
    """ Standard initialize method
    """
    res = TaskManagerAgentBase.initialize(self)
    if not res['OK']:
      return res

    # clients
    self.taskManager = RequestTasks(transClient=self.transClient)

    agentTSTypes = self.am_getOption('TransType', [])
    if agentTSTypes:
      self.transType = agentTSTypes
    else:
      self.transType = Operations().getValue('Transformations/DataManipulation', ['Replication', 'Removal'])

    return S_OK()

  def _getClients(self):
    """ Here the taskManager becomes a RequestTasks object
    """
    res = TaskManagerAgentBase._getClients(self)
    threadTaskManager = RequestTasks()
    res.update({'TaskManager': threadTaskManager})
    return res

  def _getDelegatedClients(self, ownerDN=None, ownerGroup=None):
    """Set the clients for per transformation credentials."""
    res = super(RequestTaskAgent, self)._getDelegatedClients(ownerDN=ownerDN, ownerGroup=ownerGroup)
    threadTaskManager = RequestTasks(ownerDN=ownerDN, ownerGroup=ownerGroup)
    res.update({'TaskManager': threadTaskManager})
    return res
