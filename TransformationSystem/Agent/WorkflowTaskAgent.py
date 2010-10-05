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

  #############################################################################
  def initialize( self ):
    """ Sets defaults """
    TaskManagerAgentBase.initialize( self )
    WorkflowTasks.__init__( self )
    self.transType = self.am_getOption("TransType",['MCSimulation', 'DataReconstruction', 'DataStripping', 'MCStripping', 'Merge'])
    self.am_setModuleParam( 'shifterProxy', 'ProductionManager' )
    self.am_setModuleParam( "shifterProxyLocation", "%s/runit/%s/proxy" % ( gConfig.getValue( '/LocalSite/InstancePath', rootPath ), AGENT_NAME ) )
    return S_OK()
