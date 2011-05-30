########################################################################
# $HeadURL$
########################################################################

""" RegistrationAgent takes 'register' requests from the RequestDB and registers them.
"""

__RCSID__ = "$Id$"

## imports
from DIRAC import gLogger, S_OK, S_ERROR, gConfig, gMonitor
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ProcessPool import ProcessPool, ProcessTask
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.DataManagementSystem.private.RequestAgentBase import RequestAgentBase
from DIRAC.DataManagementSystem.private.RequestTask import RequestTask
from DIRAC.DataManagementSystem.Agent.RegistrationTask import RegistrationTask

## agent name
AGENT_NAME = 'DataManagement/RegistrationAgent'

class RegistrationAgent( RequestAgentBase ):
  """ 
  .. class:: RegistrationAgent

  This agent is processing 'register' requests.

  Config Options
  --------------

  * maximal number of requests in one cycle
    RequestsPerCycle = 10
  * minimal number of sub-processes working together 
    MinProcess = 2
  * maximal number of sub-processes working togehter
    MaxProcess = 8
  * results queue size
    ProcessPoolQueueSize = 10
  * request type
    RequestType = register
  * default proxy to use
    shifterProxy = DataManager

  """
  def initialize( self ):
    """ agent initialisation 

    :param self: self reference
    """
    self.setRequestTask( RegistrationTask )
    self.log.info("Will use '%s' for task processing." % RegistrationTask.__class__.__name__ )
    self.__configPath = PathFinder.getAgentSection( AGENT_NAME )    
    self.am_setOption( "shifterProxy", "DataManager" )
    self.log.info("Will use DataManager proxy for processing requests." )
    return S_OK()
  


