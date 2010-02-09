########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/TransformationSystem/Agent/ReplicationSubmissionAgent.py $
########################################################################
"""  The Replication Submission Agent takes replication tasks created in the transformation database and submits the replication requests to the transfer management system. """
__RCSID__ = "$Id: ReplicationSubmissionAgent.py 20001 2010-01-20 12:47:38Z acsmith $"

from DIRAC                                                          import S_OK, S_ERROR, gConfig, gMonitor, gLogger, rootPath
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase          import TaskManagerAgentBase
from DIRAC.TransformationSystem.Client.TaskManager                  import RequestTasks

AGENT_NAME = 'TransformationSystem/RequestSubmissionAgent'

class RequestSubmissionAgent(TaskManagerAgentBase,RequestTasks):

  #############################################################################
  def initialize(self):
    """ Sets defaults """
    TaskManagerAgentBase.initialize()
    RequestTasks.__init__(self)
    return S_OK()
