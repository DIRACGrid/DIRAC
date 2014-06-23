""" The Request Task Agent takes request tasks created in the transformation database
    and submits to the request management system
"""

from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase  import TaskManagerAgentBase
from DIRAC.TransformationSystem.Client.TaskManager          import RequestTasks
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

__RCSID__ = "$Id$"

AGENT_NAME = 'Transformation/RequestTaskAgent'

class RequestTaskAgent( TaskManagerAgentBase ):
  """ An AgentModule to submit requests tasks
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
    self.taskManager = RequestTasks( transClient = self.transClient )
    self.transClient = TransformationClient()

    agentTSTypes = self.am_getOption( 'TransType', [] )
    if agentTSTypes:
      self.transType = agentTSTypes
    else:
      self.transType = Operations().getValue( 'Transformations/DataManipulation', ['Replication', 'Removal'] )
    
