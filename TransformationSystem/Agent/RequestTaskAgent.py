########################################################################
# $HeadURL$
########################################################################
"""  The Request Task Agent takes request tasks created in the transformation database and submits to the request management system. """
__RCSID__ = "$Id$"

from DIRAC                                                          import S_OK, S_ERROR, gConfig, gMonitor, gLogger, rootPath
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase          import TaskManagerAgentBase
from DIRAC.TransformationSystem.Client.TaskManager                  import RequestTasks

AGENT_NAME = 'Transformation/RequestTaskAgent'

class RequestTaskAgent( TaskManagerAgentBase ):
  """ An AgentModule to submit requests tasks
  """

  #############################################################################
  def initialize( self ):
    """ Sets defaults """
    
    taskManager = RequestTasks()
    
    TaskManagerAgentBase.initialize( self, taskManager = taskManager )
    self.transType = ['Replication', 'Removal']

    # This sets the Default Proxy to used as that defined under 
    # /Operations/Shifter/ProductionManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'ProductionManager' )

    return S_OK()
