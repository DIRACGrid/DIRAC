''' The Request Task Agent takes request tasks created in the transformation database
    and submits to the request management system
'''

from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase          import TaskManagerAgentBase
from DIRAC.TransformationSystem.Client.TaskManager                  import RequestTasks

AGENT_NAME = 'Transformation/RequestTaskAgent'

class RequestTaskAgent( TaskManagerAgentBase ):
  ''' An AgentModule to submit requests tasks
  '''
  def __init__( self, agentName, loadName, baseAgentName, properties ):
    ''' c'tor
    '''
    TaskManagerAgentBase.__init__( self, agentName, loadName, baseAgentName, properties )

    self.taskManager = RequestTasks( transClient = self.transClient )
    self.shifterProxy = 'ProductionManager'
    self.transType = ['Replication', 'Removal']
