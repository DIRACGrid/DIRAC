"""The Workflow Task Agent takes workflow tasks created in the transformation
database and submits to the workload management system.


The WorkflowTaskAgent takes workflow tasks created in the TransformationDB and submits them to the
WMS. Since version v6r13 there are some new capabilities in the form of TaskManager plugins.

+------------------------------+-------------------------------------------+-------------------------------------+
| **Name**                     | **Description**                           | **Example**                         |
+------------------------------+-------------------------------------------+-------------------------------------+
| *TransType*                  |                                           |                                     |
+------------------------------+-------------------------------------------+-------------------------------------+
| *TaskUpdateStatus*           |                                           |                                     |
+------------------------------+-------------------------------------------+-------------------------------------+
| *shifterProxy*               | Use a dedicated proxy to submit jobs to   |                                     |
|                              | the WMS                                   |                                     |
+------------------------------+-------------------------------------------+-------------------------------------+
| *ShifterCredentials*         | Use delegated credentials, same values as |                                     |
|                              | for shifterProxy, but there will not be   |                                     |
|                              | any actual proxy used. (New in v6r21)     |                                     |
+------------------------------+-------------------------------------------+-------------------------------------+
| *CheckReserved*              |                                           |                                     |
+------------------------------+-------------------------------------------+-------------------------------------+
| *MonitorFiles*               |                                           |                                     |
+------------------------------+-------------------------------------------+-------------------------------------+
| *SubmitTasks*                |                                           |                                     |
+------------------------------+-------------------------------------------+-------------------------------------+
| *TasksPerLoop*               |                                           |                                     |
+------------------------------+-------------------------------------------+-------------------------------------+
| *MonitorTasks*               |                                           |                                     |
+------------------------------+-------------------------------------------+-------------------------------------+

.. versionadded:: v6r21

 It is possible to run the WorkflowTaskAgent without a *shifterProxy* or
 *ShifterCredentials*, in this case the credentials of the authors of the
 transformations are used to submit the jobs to the WMS. This enables the use of
 a single WorkflowTaskAgent for multiple VOs. See also the section about the
 :ref:`trans-multi-vo`.

"""

from DIRAC import S_OK

from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase  import TaskManagerAgentBase

__RCSID__ = "$Id$"

AGENT_NAME = 'Transformation/WorkflowTaskAgent'

class WorkflowTaskAgent( TaskManagerAgentBase ):
  """ An AgentModule class to submit workflow tasks
  """
  def __init__( self, *args, **kwargs ):
    """ c'tor
    """
    TaskManagerAgentBase.__init__( self, *args, **kwargs )

    self.transType = []

  def initialize( self ):
    """ Standard initialize method
    """
    res = TaskManagerAgentBase.initialize( self )
    if not res['OK']:
      return res

    agentTSTypes = self.am_getOption( 'TransType', [] )
    if agentTSTypes:
      self.transType = agentTSTypes
    else:
      self.transType = Operations().getValue( 'Transformations/DataProcessing', ['MCSimulation', 'Merge'] )

    return S_OK()
