''' The Request Task Agent takes request tasks created in the transformation database
    and submits to the request management system
'''

from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase  import TaskManagerAgentBase
from DIRAC.TransformationSystem.Client.TaskManager          import RequestTasks

__RCSID__ = "$Id$"

AGENT_NAME = 'Transformation/RequestTaskAgent'

class RequestTaskAgent( TaskManagerAgentBase ):
  ''' An AgentModule to submit requests tasks
  '''
  def __init__( self, *args, **kwargs ):
    ''' c'tor
    '''
    TaskManagerAgentBase.__init__( self, *args, **kwargs )

    self.taskManager = RequestTasks( transClient = self.transClient )
    self.shifterProxy = 'ProductionManager'
    agentTSTypes = self.am_getOption( 'TransType', [] )
    if agentTSTypes:
      self.transType = agentTSTypes
    else:
      self.transType = Operations().getValue( 'Transformations/DataManipulation', ['Replication', 'Removal'] )
    
