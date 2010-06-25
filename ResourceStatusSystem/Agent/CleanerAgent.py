########################################################################
# $HeadURL:  $
########################################################################
""" CleanerAgent is in charge of cleaning history tables from rows older than 6 months
"""

from datetime import datetime, timedelta

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import *
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *

__RCSID__ = "$Id:  $"

AGENT_NAME = 'ResourceStatus/CleanerAgent'


class CleanerAgent(AgentModule):

#############################################################################

  def initialize(self):
    """ CleanerAgent initialization
    """
    
    try:

      self.rsDB = ResourceStatusDB()
      self.historyTables = [x+'History' for x in self.rsDB.getTablesWithHistory()]
      
      return S_OK()

    except Exception:
      errorStr = "CleanerAgent initialization"
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)


#############################################################################

  def execute(self):
    """ 
    The main CleanerAgent execution method
    
    - Cleans history tables from entries older than 6 months.
    
    - Cleans ClientsCache table from DownTimes older than a day. 
    """
    
    try:
      # Cleans history tables from entries older than 6 months.
      sixMonthsAgo = str((datetime.utcnow()).replace(microsecond = 0, 
                                                     second = 0) - timedelta(days = 180))
      
      for table in self.historyTables:
        req = "DELETE FROM %s WHERE DateEnd < '%s'" %(table, sixMonthsAgo)
        resDel = self.rsDB.db._update(req)
        if not resDel['OK']:
          raise RSSDBException, where(self, self.execute) + resDel['Message']       

      
      # Cleans ClientsCache table from DownTimes older than a day.
      aDayAgo = str((datetime.utcnow()).replace(microsecond = 0, 
                                                second = 0) - timedelta(days = 1))
      
      req = "SELECT Opt_ID FROM ClientsCache WHERE Value = 'EndDate' AND Result < '%s'" %aDayAgo
      resQuery = self.rsDB.db._query(req)
      if not resQuery['OK']:
        raise RSSDBException, where(self, self.execute) + resDel['Message']
      if resQuery['Value'] != ():
        DT_ID_to_remove = ','.join(['"'+str(x[0]).strip()+'"' for x in resQuery['Value']])

        req = "DELETE FROM ClientsCache WHERE Opt_ID IN (%s)" %DT_ID_to_remove
      
        resDel = self.rsDB.db._update(req)
        if not resDel['OK']:
          raise RSSDBException, where(self, self.execute) + resDel['Message']       

      return S_OK()
    
    except Exception:
      errorStr = "CleanerAgent execution"
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################
