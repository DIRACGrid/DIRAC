""" The Workflow Task Agent takes workflow tasks created in the
    transformation database and submits to the workload management system.
"""

from DIRAC import S_OK

from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase  import TaskManagerAgentBase
from DIRAC.TransformationSystem.Client.TaskManager          import WorkflowTasks
from DIRAC.WorkloadManagementSystem.Client.WMSClient        import WMSClient
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

__RCSID__ = "$Id$"

AGENT_NAME = 'Transformation/WorkflowTaskAgent'

class WorkflowTaskAgent( TaskManagerAgentBase ):
  """ An AgentModule class to submit workflow tasks
  """
  def __init__( self, *args, **kwargs ):
    """ c'tor
    """
    TaskManagerAgentBase.__init__( self, *args, **kwargs )

    self.taskManager = None
    self.transClient = None
    self.shifterProxy = 'ProductionManager'
    self.transType = []

  def initialize( self ):
    """ Standard initialize method
    """
    res = TaskManagerAgentBase.initialize( self )
    if not res['OK']:
      return res

    # clients
    self.taskManager = WorkflowTasks( transClient = self.transClient, submissionClient = WMSClient() )
    self.transClient = TransformationClient()

    agentTSTypes = self.am_getOption( 'TransType', [] )
    if agentTSTypes:
      self.transType = agentTSTypes
    else:
      self.transType = Operations().getValue( 'Transformations/DataProcessing', ['MCSimulation', 'Merge'] )

    return S_OK()
