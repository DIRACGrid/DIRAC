########################################################################
# $HeadURL$
########################################################################
"""  RemovalAgent takes removal requests from the RequestDB and executes them
"""

__RCSID__ = "$Id$"

## imports
import re, os, time
from types import StringTypes
## from DIRAC
from DIRAC import gLogger, gMonitor, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.DataManagementSystem.private.RequestAgentBase import RequestAgentBase
from DIRAC.DataManagementSystem.private.RequestTask import RequestTask
from DIRAC.Core.Utilities.ProcessPool import ProcessPool, ProcessTask
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.DataManagementSystem.Agent.RemovalTask import RemovalTask
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer

# agent's name
AGENT_NAME = "DataManagement/RemovalAgent"

class RemovalAgent( RequestAgentBase ):
  """
  ..  class:: RemovalAgent

  This agent is preocessing 'removal' requests read from RequestClient.
  Each request is executed in a separate sub-process using ProcessPool and 
  RemovalTask.

  Config Options
  --------------

  * set the number of requests to be processed in agent's cycle:
    RequestsPerCycle = 10
  * minimal number of sub-processes running together
    MinProcess = 2
  * maximal number of sub-processes running togehter
    MaxProcess = 8
  * results queue size
    ProcessPoolQueueSize = 10
  * request type 
    RequestType = removal
  * default proxy for handling requests 
    shifterProxy = DataManager

  """
  def __init__( self, agentName, baseAgentName = False, properties = dict() ):
    """ agent initialisation
 
    :param self: self reference
    """
    RequestAgentBase.__init__( self, agentName, baseAgentName, properties )
    
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

    self.log.info( "%s agent has been constructed" % agentName )
