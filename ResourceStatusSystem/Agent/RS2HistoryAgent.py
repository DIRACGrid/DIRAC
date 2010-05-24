########################################################################
# $HeadURL:  $
########################################################################
""" RS2HistoryAgent is in charge of monitoring Resources and Sites
    DB tables, and update Resource Status history tables.
"""

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import RSSDBException
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *

__RCSID__ = "$Id:  $"

AGENT_NAME = 'ResourceStatus/RS2HistoryAgent'


class RS2HistoryAgent(AgentModule):

#############################################################################

  def initialize(self):
    """ RS2HistoryAgent initialization
    """
    
    try:

      self.rsDB = ResourceStatusDB()
      self.tablesWithHistory = self.rsDB.getTablesWithHistory()
      
      return S_OK()

    except Exception:
      errorStr = "RS2HistoryAgent initialization"
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)


#############################################################################

  def execute(self):
    """ The main RS2HistoryAgent execution method
    """

    
    try:
      
      for table in self.tablesWithHistory:
        res = self.rsDB.getEndings(table)
        
        for row in res:
          if not self.rsDB.unique(table, row):
            self.rsDB.transact2History(table, row)
        
      return S_OK()
    
    except Exception:
      errorStr = "RS2HistoryAgent execution"
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################
