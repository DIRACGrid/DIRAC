"""The Request Task Agent takes request tasks created in the
TransformationDB and submits to the request management system.

The following options can be set for the RequestTaskAgent.

::

  RequestTaskAgent
  {
    # Use a dedicated proxy to submit requests to the RMS
    shifterProxy=DataManager
    # Use delegated credentials. Use this instead of the shifterProxy option (New in v6r20p5)
    ShifterCredentials=
    # Transformation types to be taken into account by the agent
    TransType=Replication,Removal
    # Location of the transformation plugins
    PluginLocation=DIRAC.TransformationSystem.Client.TaskManagerPlugin
    # maximum number of threads to use in this agent
    maxNumberOfThreads=15

    # Give this option a value if the agent should submit Requests
    SubmitTasks=
    # Status of transformations for which to submit Requests
    SubmitStatus=Active,Completing
    # Number of tasks to submit in one execution cycle per transformation
    TasksPerLoop=50

    # Give this option a value if the agent should monitor tasks
    MonitorTasks=
    # Status of transformations for which to monitor tasks
    UpdateTasksStatus  = Active,Completing,Stopped
    # Task statuses considered transient that should be monitored for updates
    TaskUpdateStatus=Checking,Deleted,Killed,Staging,Stalled,Matched
    TaskUpdateStatus+=Scheduled,Rescheduled,Completed,Submitted
    TaskUpdateStatus+=Assigned,Received,Waiting,Running
    # Number of tasks to be updated in one call
    TaskUpdateChunkSize=0

    # Give this option a value if the agent should monitor files
    MonitorFiles=
    # Status of transformations for which to monitor Files
    UpdateFilesStatus=Active,Completing,Stopped

    # Give this option a value if the agent should check Reserved tasks
    CheckReserved=
    # Status of transformations for which to check reserved tasks
    CheckReservedStatus= Active,Completing,Stopped

  }

* The options *SubmitTasks*, *MonitorTasks*, *MonitorFiles*, and *CheckReserved*
  need to be assigned any non-empty value to be activated

* .. versionadded:: v6r20p5

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

  def _getClients(self, ownerDN=None, ownerGroup=None):
    """Set the clients for task submission.

    Here the taskManager becomes a RequestTasks object.

    See :func:`DIRAC.TransformationSystem.TaskManagerAgentBase._getClients`.
    """
    res = super(RequestTaskAgent, self)._getClients(ownerDN=ownerDN, ownerGroup=ownerGroup)
    threadTaskManager = RequestTasks(ownerDN=ownerDN, ownerGroup=ownerGroup)
    res.update({'TaskManager': threadTaskManager})
    return res
