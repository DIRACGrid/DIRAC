"""The Workflow Task Agent takes workflow tasks created in the transformation
database and submits to the workload management system.


The WorkflowTaskAgent takes workflow tasks created in the TransformationDB and submits them to the
WMS. Since version v6r13 there are some new capabilities in the form of TaskManager plugins.

The following options can be set for the WorkflowTaskAgent.

::

  WorkflowTaskAgent
  {
    # Use a dedicated proxy to submit jobs to the WMS
    shifterProxy = ProductionManager
    # Use delegated credentials. Use this instead of the shifterProxy option (New in v6r20p5)
    ShifterCredentials =
    # Transformation types to be taken into account by the agent
    TransType = MCSimulation,DataReconstruction,DataStripping,MCStripping,Merge
    # Location of the transformation plugins
    PluginLocation = DIRAC.TransformationSystem.Client.TaskManagerPlugin
    # maximum number of threads to use in this agent
    maxNumberOfThreads = 15

    # Give this option a value if the agent should submit Requests
    SubmitTasks = yes
    # Status of transformations for which to submit Requests
    SubmitStatus = Active,Completing
    # Number of tasks to submit in one execution cycle per transformation
    TasksPerLoop = 50

    # Give this option a value if the agent should monitor tasks
    MonitorTasks = yes
    # Status of transformations for which to monitor tasks
    UpdateTasksStatus = Active,Completing,Stopped
    # Task statuses considered transient that should be monitored for updates
    TaskUpdateStatus = Submitted,Received,Waiting,Running,Matched,Completed,Failed
    # Number of tasks to be updated in one call
    TaskUpdateChunkSize = 0

    # Give this option a value if the agent should monitor files
    MonitorFiles =
    # Status of transformations for which to monitor Files
    UpdateFilesStatus = Active,Completing,Stopped

    # Give this option a value if the agent should check Reserved tasks
    CheckReserved =
    # Status of transformations for which to check reserved tasks
    CheckReservedStatus = Active,Completing,Stopped

    # Fill in this option if you want to activate bulk submission (for speed up)
    BulkSubmission = yes

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

__RCSID__ = "$Id$"

from DIRAC import S_OK

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase import TaskManagerAgentBase

AGENT_NAME = 'Transformation/WorkflowTaskAgent'


class WorkflowTaskAgent(TaskManagerAgentBase):
  """ An AgentModule class to submit workflow tasks
  """

  def __init__(self, *args, **kwargs):
    """ c'tor
    """
    TaskManagerAgentBase.__init__(self, *args, **kwargs)

    self.transType = []

  def initialize(self):
    """ Standard initialize method
    """
    res = TaskManagerAgentBase.initialize(self)
    if not res['OK']:
      return res

    agentTSTypes = self.am_getOption('TransType', [])
    if agentTSTypes:
      self.transType = agentTSTypes
    else:
      self.transType = Operations().getValue('Transformations/DataProcessing', ['MCSimulation', 'Merge'])

    return S_OK()
