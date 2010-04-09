########################################################################
# $HeadURL:  $
########################################################################
""" RS2HistoryAgent is in charge of monitoring Resources and Sites
    DB tables, and update Resource Status history tables.
"""

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import RSSDBException
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

__RCSID__ = "$Id:  $"

AGENT_NAME = 'ResourceStatus/RS2HistoryAgent'

class RS2HistoryAgent(AgentModule):


  def initialize(self):
    """ RS2HistoryAgent initialization
    """
    
    try:

      try:
        self.rsDB = ResourceStatusDB()
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      
      return S_OK()

#    except Exception, x:
#      errorStr = where(self, self.execute)
#      gLogger.exception(errorStr,lException=x)
#      return S_ERROR(errorStr)

    except Exception:
      errorStr = "RS2HistoryAgent initialization"
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)


  def execute(self):
    """ The main RS2HistoryAgent execution method
    """

    
    try:
      
      try:
        tablesWithHistory = self.rsDB.getTablesWithHistory()
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))

      for table in tablesWithHistory:
        try:
          res = self.rsDB.getEndings(table)
        except RSSDBException, x:
          gLogger.error(whoRaised(x))
        except RSSException, x:
          gLogger.error(whoRaised(x))
        
        for row in res:
          try:
            if not self.rsDB.unique(table, row):
              self.rsDB.transact2History(table, row)
          except RSSDBException, x:
            gLogger.error(whoRaised(x))
          except RSSException, x:
            gLogger.error(whoRaised(x))
        
      return S_OK()
    
#    except Exception, x:
#      errorStr = where(self, self.execute)
#      gLogger.exception(errorStr,lException=x)
#      return S_ERROR(errorStr)
  
    except Exception:
      errorStr = "RS2HistoryAgent execution"
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)
