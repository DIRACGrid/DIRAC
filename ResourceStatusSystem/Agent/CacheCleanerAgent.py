################################################################################
# $HeadURL$
################################################################################
__RCSID__  = "$Id$"
AGENT_NAME = 'ResourceStatus/CleanerAgent'

from datetime                                             import datetime,timedelta

from DIRAC                                                import S_OK, S_ERROR, gLogger
from DIRAC                                                import gLogger
from DIRAC.Core.Base.AgentModule                          import AgentModule

from DIRAC.ResourceStatusSystem                           import ValidRes  
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

class CacheCleanerAgent( AgentModule ):
  '''
  The CacheCleanerAgent tidies up the ResourceStatusDB, namely:
    o SiteHistory
    o ServiceHistory
    o ResourceHistory
    o StorageElementHistory
  older than 6 months.
  
  It also takes care of the ResourceManagementDB, more specifically:
    o ClientCache
    o AccountingCache
  older than 24 hours for the first one, and 30 minutes for the second one.
  
  If you want to know more about the CacheCleanerAgent, scroll down to the end of the 
  file.   
  '''

  def initialize( self ):
    
    try:
      
      self.rsClient         = ResourceStatusClient()
      self.rmClient         = ResourceManagementClient()  
      self.historyTables = [ '%sHistory' % x for x in ValidRes ]

      return S_OK()
      
    except Exception:
      errorStr = "CacheCleanerAgent initialization"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )
    
################################################################################
################################################################################    
    
  def execute( self ):
    
    try:
       
      # Cleans history tables from entries older than 6 months.
      now          = datetime.utcnow().replace( microsecond = 0, second = 0 )
      sixMonthsAgo = now - timedelta( days = 180 )
      
      for g in ValidRes:
        #deleter = getattr( self.rsClient, 'delete%sHistory' % g )
        
        kwargs = { 'meta' : { 'minor' : { 'DateEnd' : sixMonthsAgo } } }
        gLogger.info( 'Deleting %sHistory older than %s' % ( g, sixMonthsAgo ) )
        res = self.rsClient.deleteElementHistory( g, **kwargs )
        if not res[ 'OK' ]:
          gLogger.error( res[ 'Message' ] )            

      # Cleans ClientsCache table from DownTimes older than a day.
      aDayAgo = now - timedelta( days = 1 )
      
      kwargs = { 'meta' : {
                   'value'  : 'EndDate',
                   'columns': 'Opt_ID',
                   'minor'  : { 'Result' : aDayAgo }
                  } 
                }
      opt_IDs = self.rmClient.getClientCache( **kwargs )              
      opt_IDs = [ ID[ 0 ] for ID in opt_IDs[ 'Value' ] ]
      
      if opt_IDs:
        gLogger.info( 'Found %s ClientCache items to be deleted' % len( opt_IDs) )
        gLogger.debug( opt_IDs )
      
      res = self.rmClient.deleteClientCache( opt_ID = opt_IDs )
      if not res[ 'OK' ]:
        gLogger.error( res[ 'Message' ] )
      
      # Cleans AccountingCache table from plots not updated nor checked in the last 30 mins      
      anHourAgo = now - timedelta( minutes = 30 )
      gLogger.info( 'Deleting AccountingCache older than %s' % ( anHourAgo ) )
      res = self.rmClient.deleteAccountingCache( meta = {'minor': { 'LastCheckTime' : anHourAgo }} )
      if not res[ 'OK' ]:
        gLogger.error( res[ 'Message' ] )

      return S_OK()
    
    except Exception:
      errorStr = "CacheCleanerAgent execution"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF      