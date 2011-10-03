################################################################################
# $HeadURL$
################################################################################
""" CleanerAgent is in charge of different cleanings
"""

from datetime                                           import datetime,timedelta

from DIRAC                                              import S_OK, S_ERROR
from DIRAC                                              import gLogger

from DIRAC.Core.Base.AgentModule                        import AgentModule
from DIRAC.ResourceStatusSytem                          import ValidRes  
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB     import ResourceStatusDB, RSSDBException
from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB
from DIRAC.ResourceStatusSystem.Utilities.Utils         import where

__RCSID__ = "$Id$"

AGENT_NAME = 'ResourceStatus/CleanerAgent'

class CleanerAgent( AgentModule ):

################################################################################

  def initialize( self ):
    """ CleanerAgent initialization
    """
    
    try:

      self.rsDB              = ResourceStatusDB()
      self.rmDB              = ResourceManagementDB()  
      self.historyTables     = [ '%sHistory' % x for x in ValidRes ]
      
      return S_OK()

    except Exception:
      errorStr = "CleanerAgent initialization"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

################################################################################

  def execute( self ):
    """ 
    The main CleanerAgent execution method
    
    - Update Resource Status history tables.
    
    - Cleans history tables from entries older than 6 months.
    
    - Cleans ClientsCache table from DownTimes older than a day. 
    """
    
    try:
       
      #it is automatically done !  
      # update Resource Status history tables.
      #for table in self.tablesWithHistory:
      #    
      #  res = self.rsDB.getEndings( table )
      #  
      #  for row in res:
      #    if not self.rsDB.unique( table, row ):
      #      self.rsDB.transact2History( table, row )

      # Tidies up History tables, deleting  entries with same dateCreated
      # just keeps the last one

      # Cleans history tables from entries older than 6 months.
      sixMonthsAgo = datetime.utcnow().replace( microsecond = 0, 
                                    second = 0 ) - timedelta( days = 180 )
      
      for g in ValidRes:
        deleter = getattr( rsDB, 'delete%ssHistory' % g )
        kwargs = { 'minor' : { 'DateEnd' : sixMonthsAgo } }
        deleter( **kwargs )    

      
      # Cleans ClientsCache table from DownTimes older than a day.
      aDayAgo = str( datetime.utcnow().replace( microsecond = 0, 
                               second = 0 ) - timedelta( days = 1 ) )
      
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

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF      