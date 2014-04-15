''' The Workflow Task Agent takes workflow tasks created in the
    transformation database and submits to the workload management system.
'''
from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase  import TaskManagerAgentBase
from DIRAC.TransformationSystem.Client.TaskManager          import WorkflowTasks
from DIRAC.WorkloadManagementSystem.Client.WMSClient        import WMSClient

__RCSID__ = "$Id$"

AGENT_NAME = 'Transformation/WorkflowTaskAgent'

class WorkflowTaskAgent( TaskManagerAgentBase ):
  ''' An AgentModule class to submit workflow tasks
  '''
  def __init__( self, *args, **kwargs ):
    ''' c'tor
    '''
    TaskManagerAgentBase.__init__( self, *args, **kwargs )

    self.submissionClient = WMSClient()
    self.taskManager = WorkflowTasks( transClient = self.transClient, submissionClient = self.submissionClient )
    self.shifterProxy = 'ProductionManager'
    agentTSTypes = self.am_getOption( 'TransType', [] )
    if agentTSTypes:
      self.transType = agentTSTypes
    else:
      self.transType = Operations().getValue( 'Transformations/DataProcessing', ['MCSimulation', 'Merge'] )
