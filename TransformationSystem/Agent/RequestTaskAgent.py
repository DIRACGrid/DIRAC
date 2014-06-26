""" The Request Task Agent takes request tasks created in the transformation database
    and submits to the request management system
"""

from DIRAC import S_OK

from DIRAC.ConfigurationSystem.Client.Helpers.Operations    import Operations
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase  import TaskManagerAgentBase
from DIRAC.TransformationSystem.Client.TaskManager          import RequestTasks

__RCSID__ = "$Id$"

AGENT_NAME = 'Transformation/RequestTaskAgent'

class RequestTaskAgent( TaskManagerAgentBase ):
  """ An AgentModule to submit requests tasks
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

    # clients
    self.taskManager = RequestTasks( transClient = self.transClient )

    agentTSTypes = self.am_getOption( 'TransType', [] )
    if agentTSTypes:
      self.transType = agentTSTypes
    else:
      self.transType = Operations().getValue( 'Transformations/DataManipulation', ['Replication', 'Removal'] )

    return S_OK()
    
  def _getClients( self ):
    """ Here the taskManager becomes a RequestTasks object
    """
    res = TaskManagerAgentBase._getClients( self )
    threadTaskManager = RequestTasks()
    res.update( {'TaskManager': threadTaskManager} )
    return res
