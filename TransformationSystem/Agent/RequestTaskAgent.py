########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/TransformationSystem/Agent/RequestTaskAgent.py $
########################################################################
"""  The Request Task Agent takes request tasks created in the transformation database and submits to the request management system. """
__RCSID__ = "$Id: ReplicationSubmissionAgent.py 20001 2010-01-20 12:47:38Z acsmith $"

from DIRAC                                                          import S_OK, S_ERROR, gConfig, gMonitor, gLogger, rootPath
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase          import TaskManagerAgentBase
from DIRAC.TransformationSystem.Client.TaskManager                  import RequestTasks

AGENT_NAME = 'Transformation/RequestTaskAgent'

class RequestTaskAgent( TaskManagerAgentBase, RequestTasks ):
  """ An AgentModule to submit requests tasks
  """

  #############################################################################
  def initialize( self ):
    """ Sets defaults """
    TaskManagerAgentBase.initialize( self )
    RequestTasks.__init__( self )
    self.transType = ['Replication', 'Removal']

    # This sets the Default Proxy to used as that defined under 
    # /Operations/Shifter/ProductionManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'ProductionManager' )

    return S_OK()
