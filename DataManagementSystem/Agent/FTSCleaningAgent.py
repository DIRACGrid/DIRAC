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
"""

__RCSID__ = "$Id $"

##
# @file FTSCleaningAgent.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/06/23 10:15:04
# @brief Definition of FTSCleaningAgent class.

## imports 
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB

AGENT_NAME = "DataManagement/FTSCleaningAgent"
  
########################################################################
class FTSCleaningAgent( AgentModule ):
  """
  .. class:: FTSCleaningAgent

  """
  ## placeholder fot TransferDB instance
  __transferDB = None

  ## one week grace period  
  __gracePeriod = 7
  
  def initialize( self ):
    """ Agent initialization.

    :param self: self reference
    """
    self.__gracePeriod = self.am_getOption( "GracePeriod", 7 )
    ## shifterProxy
    self.am_setOption( "shifterProxy", "DataManager" )
    
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

    obsoleteChannels = self.transferDB().selectObsoleteChannels( limit = 10 )


    older = datetime.datetime.now() - datetime.timedelta( days = self.__gracePeriod )
    ftsRequests = self.transferDB().selectFTSReq( older = older, limit = 50 )
    
    return S_OK()
