"""The Request Task Agent takes request tasks created in the
TransformationDB and submits to the request management system.

The following options can be set for the RequestTaskAgent.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN RequestTaskAgent
  :end-before: ##END
  :dedent: 2
  :caption: RequestTaskAgent options

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
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase import TaskManagerAgentBase

AGENT_NAME = "Transformation/RequestTaskAgent"


class RequestTaskAgent(TaskManagerAgentBase):
    """An AgentModule to submit requests tasks"""

    def __init__(self, *args, **kwargs):
        """c'tor"""
        TaskManagerAgentBase.__init__(self, *args, **kwargs)

        self.transType = []
        self.taskManager = None

    def initialize(self):
        """Standard initialize method"""
        res = TaskManagerAgentBase.initialize(self)
        if not res["OK"]:
            return res

        objLoader = ObjectLoader()
        _class = objLoader.loadObject("TransformationSystem.Client.RequestTasks", "RequestTasks")

        if not _class["OK"]:
            raise Exception(_class["Message"])

        self.requestTasksCls = _class["Value"]

        # clients
        self.taskManager = self.requestTasksCls(transClient=self.transClient)

        agentTSTypes = self.am_getOption("TransType", [])
        if agentTSTypes:
            self.transType = agentTSTypes
        else:
            self.transType = Operations().getValue("Transformations/DataManipulation", ["Replication", "Removal"])

        return S_OK()

    def _getClients(self, ownerDN=None, ownerGroup=None):
        """Set the clients for task submission.

        Here the taskManager becomes a RequestTasks object.

        See :func:`DIRAC.TransformationSystem.TaskManagerAgentBase._getClients`.
        """
        res = super()._getClients(ownerDN=ownerDN, ownerGroup=ownerGroup)
        threadTaskManager = self.requestTasksCls(ownerDN=ownerDN, ownerGroup=ownerGroup)
        res.update({"TaskManager": threadTaskManager})
        return res
