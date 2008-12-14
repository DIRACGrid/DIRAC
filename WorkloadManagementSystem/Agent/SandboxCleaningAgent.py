########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Agent/SandboxCleaningAgent.py,v 1.1 2008/12/14 20:10:52 atsareg Exp $
# File : SandboxCleaningAgent.py
# Author : A.T.
########################################################################

"""  The Sandbox Cleaning Agent controls compression and cleaning of the sandbox
     database partitions 
"""

__RCSID__ = "$Id: SandboxCleaningAgent.py,v 1.1 2008/12/14 20:10:52 atsareg Exp $"

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
