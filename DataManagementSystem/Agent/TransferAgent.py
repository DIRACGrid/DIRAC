"""  TransferAgent takes transfer requests from the RequestDB and replicates them
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.Core.Utilities.Pfn import pfnparse, pfnunparse
from DIRAC.RequestManagementSystem.Client.Request import RequestClient

import time
from types import *

AGENT_NAME = 'DataManagement/TransferAgent'

class TransferAgent(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)
    self.RequestDBClient = RequestClient()
    return result

  def execute(self):
    return S_OK()
