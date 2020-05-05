""" The WorkflowTaskAgent takes workflow tasks created in the TransformationDB and submits them to the WMS.

The following options can be set for the WorkflowTaskAgent.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN WorkflowTaskAgent
  :end-before: ##END
  :dedent: 2
  :caption: WorkflowTaskAgent options

The options *SubmitTasks*, *MonitorTasks*, *MonitorFiles*, and *CheckReserved*
need to be assigned any non-empty value to be activated

* .. versionadded:: v6r20p5

   It is possible to run the RequestTaskAgent without a *shifterProxy* or
   *ShifterCredentials*, in this case the credentials of the authors of the
   transformations are used to submit the jobs to the RMS. This enables the use of
   a single RequestTaskAgent for multiple VOs. See also the section about the
   :ref:`trans-multi-vo`.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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
