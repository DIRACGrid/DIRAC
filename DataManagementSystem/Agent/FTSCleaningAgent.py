########################################################################
# $HeadURL $
# File: FTSCleaningAgent.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/06/23 10:11:29
########################################################################

""" :mod: FTSCleaningAgent
    =======================

    .. module: FTSCleaningAgent
    :synopsis: cleaning old FTS
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    Cleaning of the procesed requests in TransferDB.

    :deprecated:
"""

__RCSID__ = "$Id $"

# #
# @file FTSCleaningAgent.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/06/23 10:15:04
# @brief Definition of FTSCleaningAgent class.

# # imports
from DIRAC import S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB

AGENT_NAME = "DataManagement/FTSCleaningAgent"

########################################################################
class FTSCleaningAgent( AgentModule ):
  """
  .. class:: FTSCleaningAgent

  """
  # # placeholder fot TransferDB instance
  __transferDB = None
  # # 6 months grace period
  __gracePeriod = 180
  # # FTS requests per cycle
  __selectLimit = 50
  # # TransferDB table names
  __tblNames = [ "FTSReq", "FTSReqLogging", "FileToFTS", "FileToCat", "Channel", "ReplicationTree" ]

  def __init__( self, agentName, loadName, baseAgentName = False, properties = dict() ):
    """ c'tor

    :param self: self reference
    :param str agentName: name of agent
    :param bool baseAgentName: whatever
    :param dict properties: whatever else
    """
    AgentModule.__init__( self, agentName, loadName, baseAgentName, properties )

  def initialize( self ):
    """ Agent initialization.

    :param self: self reference
    """
    # # grace period
    self.__gracePeriod = self.am_getOption( "GraceDaysPeriod", self.__gracePeriod )
    self.log.info( "grace period        = %s days" % self.__gracePeriod )
    self.__selectLimit = self.am_getOption( "FTSRequestsPerCycle", self.__selectLimit )
    self.log.info( "FTS requests/cycle  = %s" % self.__selectLimit )
    # # shifterProxy
    self.am_setOption( "shifterProxy", "DataManager" )
    return S_OK()

  def transferDB( self ):
    """ TransferDB facade

    :param self: self reference
    """
    if not self.__transferDB:
      self.__transferDB = TransferDB()
    return self.__transferDB

  def execute( self ):
    """ execution in one cycle

    :param self: self reference
    """
    self.log.info( "will try to clean up %s FTS jobs and Channels older than %s days" % ( self.__selectLimit, self.__gracePeriod ) )
    # # do clean up
    cleanUp = self.transferDB().cleanUp( self.__gracePeriod, self.__selectLimit )
    if not cleanUp["OK"]:
      return cleanUp
    cleanUp = cleanUp["Value"]
    # # fill counters
    counters = dict.fromkeys( [ "DELETE FROM %s " % tblName for tblName in self.__tblNames ], 0 )
    for cmd, ret in cleanUp:
      for key in counters:
        if cmd.startswith( key ):
          counters[key] += ret
    # # print counters into logger
    for key, value in sorted( counters.items() ):
      self.log.info( "%stable" % key.replace( "DELETE FROM", "deleted %s records from" % value ) )
    return S_OK()
