########################################################################
# $HeadURL$
# File : SandboxCleaningAgent.py
# Author : A.T.
########################################################################

"""  The Sandbox Cleaning Agent controls compression and cleaning of the sandbox
     database partitions.
     This is an obsoleted agent that shoudl be removed from the system 
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Agent                         import Agent
from DIRAC.WorkloadManagementSystem.DB.SandboxDB   import SandboxDB
from DIRAC                                         import S_OK, S_ERROR, gConfig, gLogger
import DIRAC.Core.Utilities.Time as Time

AGENT_NAME = 'WorkloadManagement/SandboxCleaningAgent'

class SandboxCleaningAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor for Agent
    """
    Agent.__init__(self,AGENT_NAME)

  #############################################################################
  def initialize(self):
    """Sets defaults
    """
    result = Agent.initialize(self)
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',1000)
    self.sandboxDB = SandboxDB('SandboxDB')

    return result

  #############################################################################
  def execute(self):
    """The agent execution method.
    """

    for sandbox in ['InputSandbox','OutputSandbox']:
      result = self.sandboxDB.cleanSandbox(sandbox)
      
    return result  
