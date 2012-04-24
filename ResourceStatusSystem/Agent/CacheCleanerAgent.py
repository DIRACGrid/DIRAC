# $HeadURL$
''' CacheCleanerAgent

  This agent cleans the history tables, and the cache ones if entries older
  than a certan period.

'''

from datetime import datetime, timedelta

from DIRAC                                                      import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Utilities                       import RssConfiguration   

__RCSID__  = '$Id: $'
AGENT_NAME = 'ResourceStatus/CleanerAgent'

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
  
  # Too many public methods
  # pylint: disable-msg=R0904

  def initialize( self ):
    
    # Attribute defined outside __init__  
    # pylint: disable-msg=W0201
    
    try:
      
      self.rsClient      = ResourceStatusClient()
      self.rmClient      = ResourceManagementClient()  
      
      validElements      = RssConfiguration.getValidElements() 
      
      self.historyTables = [ '%sHistory' % x for x in validElements ]

      return S_OK()
      
    except Exception:
      errorStr = "CacheCleanerAgent initialization"
      self.log.exception( errorStr )
      return S_ERROR( errorStr )
    
################################################################################
################################################################################    
    
  def execute( self ):
    
    try:
       
      # Cleans history tables from entries older than 6 months.
      now          = datetime.utcnow().replace( microsecond = 0, second = 0 )
      sixMonthsAgo = now - timedelta( days = 180 )
      
      validElements = RssConfiguration.getValidElements()
      
      for granularity in validElements:
        #deleter = getattr( self.rsClient, 'delete%sHistory' % g )
        
        kwargs = { 'meta' : { 'minor' : { 'DateEnd' : sixMonthsAgo } } }
        self.log.info( 'Deleting %sHistory older than %s' % ( granularity, sixMonthsAgo ) )
        res = self.rsClient.deleteElementHistory( granularity, **kwargs )
        if not res[ 'OK' ]:
          self.log.error( res[ 'Message' ] )            

      # Cleans ClientsCache table from DownTimes older than a day.
      aDayAgo = now - timedelta( days = 1 )
      
      kwargs = { 'meta' : {
                   'value'  : 'EndDate',
                   'columns': 'Opt_ID',
                   'minor'  : { 'Result' : str( aDayAgo ) }
                  } 
                }
      opt_IDs = self.rmClient.getClientCache( **kwargs )              
      opt_IDs = [ ID[ 0 ] for ID in opt_IDs[ 'Value' ] ]
      
      if opt_IDs:
        self.log.info( 'Found %s ClientCache items to be deleted' % len( opt_IDs) )
        self.log.debug( opt_IDs )
      
      res = self.rmClient.deleteClientCache( opt_ID = opt_IDs )
      if not res[ 'OK' ]:
        self.log.error( res[ 'Message' ] )
      
      # Cleans AccountingCache table from plots not updated nor checked in the last 30 mins      
      anHourAgo = now - timedelta( minutes = 30 )
      self.log.info( 'Deleting AccountingCache older than %s' % ( anHourAgo ) )
      res = self.rmClient.deleteAccountingCache( meta = {'minor': { 'LastCheckTime' : anHourAgo }} )
      if not res[ 'OK' ]:
        self.log.error( res[ 'Message' ] )

      return S_OK()
    
    except Exception:
      errorStr = "CacheCleanerAgent execution"
      self.log.exception( errorStr )
      return S_ERROR( errorStr )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF      