########################################################################
# $HeadURL$
########################################################################

""" RegistrationAgent takes 'register' requests from the RequestDB and registers them.

  :deprecated:
"""

__RCSID__ = "$Id$"

## imports
from DIRAC.DataManagementSystem.private.RequestAgentBase import RequestAgentBase
from DIRAC.DataManagementSystem.private.RegistrationTask import RegistrationTask

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
  def __init__( self, *args, **kwargs ):
    """ agen c'tor

    :param self: self reference
    """
    self.setRequestType( "register" )
    self.setRequestTask( RegistrationTask )
    RequestAgentBase.__init__( self, *args, **kwargs )
    agentName = args[0]
    self.log.info("%s has been constructed" % agentName  )
    
  


