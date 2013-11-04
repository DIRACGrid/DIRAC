########################################################################
# $HeadURL$
########################################################################
"""  RemovalAgent takes removal requests from the RequestDB and executes them

    :deprecated:
"""

__RCSID__ = "$Id$"

## from DIRAC
from DIRAC import gMonitor
from DIRAC.DataManagementSystem.private.RequestAgentBase import RequestAgentBase
from DIRAC.DataManagementSystem.private.RemovalTask import RemovalTask

# agent's name
AGENT_NAME = "DataManagement/RemovalAgent"

class RemovalAgent( RequestAgentBase ):
  """
  ..  class:: RemovalAgent

  This agent is processing 'removal' requests read from RequestClient.
  Each request is executed in a separate sub-process using ProcessPool and 
  RemovalTask.

  Config Options
  --------------

  * set the number of requests to be processed in agent's cycle:
    RequestsPerCycle = 10
  * minimal number of sub-processes running together
    MinProcess = 1
  * maximal number of sub-processes running togehter
    MaxProcess = 4
  * results queue size
    ProcessPoolQueueSize = 10
  * request type 
    RequestType = removal
  * default proxy for handling requests 
    shifterProxy = DataManager
  """

  def __init__( self, *args, **kwargs ):
    """ agent initialisation
 
    :param self: self reference
    """
    self.setRequestType( "removal" )
    self.setRequestTask( RemovalTask )
    RequestAgentBase.__init__( self, *args, **kwargs )

    agentName = args[0]

    # gMonitor stuff goes here
    self.monitor.registerActivity( "PhysicalRemovalAtt", "Physical removals attempted",
                                   "RemovalAgent", "Removal/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "PhysicalRemovalDone", "Successful physical removals",
                                   "RemovalAgent", "Removal/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "PhysicalRemovalFail", "Failed physical removals",
                                   "RemovalAgent", "Removal/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "PhysicalRemovalSize", "Physically removed size",
                                   "RemovalAgent", "Bytes", gMonitor.OP_ACUM )
    
    self.monitor.registerActivity( "ReplicaRemovalAtt", "Replica removal attempted",
                                   "RemovalAgent", "Removal/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "ReplicaRemovalDone", "Successful replica removals",
                                   "RemovalAgent", "Removal/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "ReplicaRemovalFail", "Failed replica removals",
                                   "RemovalAgent", "Removal/min", gMonitor.OP_SUM )
    
    self.monitor.registerActivity( "RemoveFileAtt", "File removal attempted",
                                   "RemovalAgent", "Removal/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "RemoveFileDone", "File removal done",
                                   "RemovalAgent", "Removal/min", gMonitor.OP_SUM )
    self.monitor.registerActivity( "RemoveFileFail", "File removal failed",
                                   "RemovalAgent", "Removal/min", gMonitor.OP_SUM )    
    ## ready to go
    self.log.info( "%s agent has been constructed" % agentName )
