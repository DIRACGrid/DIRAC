########################################################################
# $HeadURL$
########################################################################
""" CleanerAgent is in charge of different cleanings
"""

import datetime

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB, RSSDBException
from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB
from DIRAC.ResourceStatusSystem.Utilities.Utils import where

__RCSID__ = "$Id$"

AGENT_NAME = 'ResourceStatus/CleanerAgent'

class CleanerAgent( AgentModule ):

#############################################################################

  def initialize( self ):
    """ CleanerAgent initialization
    """
    
    try:

      self.rsDB = ResourceStatusDB()
      self.rmDB = ResourceManagementDB()  
      self.tablesWithHistory = self.rsDB.getTablesWithHistory()
      self.historyTables = [ x + 'History' for x in self.tablesWithHistory ]
      
      return S_OK()

    except Exception:
      errorStr = "CleanerAgent initialization"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

#############################################################################

  def execute( self ):
    """ 
    The main CleanerAgent execution method
    
    - Update Resource Status history tables.
    
    - Cleans history tables from entries older than 6 months.
    
    - Cleans ClientsCache table from DownTimes older than a day. 
    """
    
    try:
        
      # update Resource Status history tables.
      for table in self.tablesWithHistory:
          
        res = self.rsDB.getEndings( table )
        
        for row in res:
          if not self.rsDB.unique( table, row ):
            self.rsDB.transact2History( table, row )

      # Cleans history tables from entries older than 6 months.
      sixMonthsAgo = str( ( datetime.datetime.utcnow() ).replace( microsecond = 0, 
                                    second = 0 ) - datetime.timedelta( days = 180 ) )
      
      for table in self.historyTables:
        req = "DELETE FROM %s WHERE DateEnd < '%s'" % ( table, sixMonthsAgo )
        resDel = self.rsDB.db._update( req )
        if not resDel[ 'OK' ]:
          raise RSSDBException, where( self, self.execute ) + resDel[ 'Message' ]       

      
      # Cleans ClientsCache table from DownTimes older than a day.
      aDayAgo = str( ( datetime.datetime.utcnow() ).replace( microsecond = 0, 
                               second = 0 ) - datetime.timedelta( days = 1 ) )
      
      req = "SELECT Opt_ID FROM ClientsCache WHERE Value = 'EndDate' AND Result < '%s'" % aDayAgo
      resQuery = self.rmDB.db._query( req )
      if not resQuery[ 'OK' ]:
        raise RSSDBException, where( self, self.execute ) + resDel[ 'Message' ]
      if resQuery[ 'Value' ] != ():
        DT_ID_to_remove = ','.join( [ '"' + str( x[ 0 ] ).strip() + '"' for x in resQuery[ 'Value' ] ] )

        req = "DELETE FROM ClientsCache WHERE Opt_ID IN (%s)" % DT_ID_to_remove
      
        resDel = self.rmDB.db._update( req )
        if not resDel[ 'OK' ]:
          raise RSSDBException, where( self, self.execute ) + resDel[ 'Message' ]       

      # Cleans AccountingCache table from plots not updated nor checked in the last 30 mins 
      anHourAgo = str( ( datetime.datetime.utcnow() ).replace( microsecond = 0, 
                                 second = 0 ) - datetime.timedelta( minutes = 30 ) )
      
      req = "DELETE FROM AccountingCache WHERE LastCheckTime < '%s'" % anHourAgo
      resDel = self.rmDB.db._update( req )
      if not resDel[ 'OK' ]:
        raise RSSDBException, where( self, self.execute ) + resDel[ 'Message' ]


      return S_OK()
    
    except Exception:
      errorStr = "CleanerAgent execution"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

#############################################################################
      
