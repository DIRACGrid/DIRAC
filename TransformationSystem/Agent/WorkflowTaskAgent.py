########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/TransformationSystem/Agent/WorkflowTaskAgent.py $
########################################################################
"""  The Request Task Agent takes workflow tasks created in the transformation database and submits to the workload management system. """
__RCSID__ = "$Id: WorkflowTaskAgent.py 20001 2010-01-20 12:47:38Z acsmith $"

from DIRAC                                                          import S_OK, S_ERROR, gConfig, gMonitor, gLogger, rootPath
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase          import TaskManagerAgentBase
from DIRAC.TransformationSystem.Client.TaskManager                  import WorkflowTasks

AGENT_NAME = 'Transformation/WorkflowTaskAgent'

class WorkflowTaskAgent( TaskManagerAgentBase, WorkflowTasks ):
  """ An AgentModule class to submit workflow tasks
  """

  #############################################################################
  def initialize( self ):
    """ Sets defaults """
    TaskManagerAgentBase.initialize( self )
    WorkflowTasks.__init__( self )
    self.transType = self.am_getOption( "TransType", ['MCSimulation', 'DataReconstruction', 'DataStripping', 'MCStripping', 'Merge'] )

    # This sets the Default Proxy to used as that defined under 
    # /Operations/Shifter/ProductionManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'ProductionManager' )

    return S_OK()
