# $HeadURL:  $
''' DatabaseCleanerAgent module
'''

from datetime import datetime, timedelta

from DIRAC                                                      import S_OK
from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

__RCSID__  = '$Id:  $'
AGENT_NAME = 'ResourceStatus/DatabaseCleanerAgent'

class DatabaseCleanerAgent( AgentModule ):
  '''
    Agent that cleans the tables that may grow. With other words, it cleans from
    old entries all the cache tables in ResourceManagementDB and the log and
    history tables on the ResourceStatusDB. 
  '''

  # Max number of minutes for the cache tables, 60 minutes by default
  __maxCacheLifetime = 60
  
  # Max number of days for the history tables
  __maxHistoryLifetime = 10

  # Max number of days for the log tables
  __maxLogLifetime = 20

  # List of caches to be processed
  __cacheNames =  ( 'DowntimeCache', 'GGUSTicketsCache', 'JobCache', 
                    'PilotCache', 'TransferCache', 'SpaceTokenOccupancyCache' )

  def __init__( self, agentName, loadName, baseAgentName = False, properties = {} ):
    
    AgentModule.__init__( self, agentName, loadName, baseAgentName, properties )
    
    self.maxCacheLifetime   = self.__maxCacheLifetime
    self.maxHistoryLifetime = self.__maxHistoryLifetime
    self.maxLogLifetime     = self.__maxLogLifetime
    
    self.rsClient = None
    self.rmClient = None

  def initialize( self ):
    
    self.maxCacheLifetime   = self.am_getOption( 'maxCacheLifetime', self.maxCacheLifetime ) 
    self.maxHistoryLifetime = self.am_getOption( 'maxHistoryLifetime', self.maxHistoryLifetime )
    self.maxLogLifetime     = self.am_getOption( 'maxLogLifetime', self.maxLogLifetime )
    
    self.rsClient = ResourceStatusClient()
    self.rmClient = ResourceManagementClient()
    
    return S_OK()
  
  def execute( self ):

#    TODO: uncomment when ResourceMonitoring is ready 
#    self._cleanCaches()
    self._cleanStatusTable( 'History', self.maxHistoryLifetime )
    self._cleanStatusTable( 'Log',     self.maxLogLifetime )
    
    return S_OK()
    
  ## Protected methods #########################################################
    
  def _cleanCaches( self ):
    '''
      Method that iterates over all the caches in ResourceManagemetnDB deleting
      entries with a LastCheckTime parameter older than now - X( hours ). On theory,
      there should not be any parameter to be deleted. If there are, it means that
      by some reason that entry has not been updated.
    '''    
    
    self.log.info( 'Cleaning cache entries older than %s minutes' % self.maxCacheLifetime )
    
    lastValidRecord = datetime.utcnow() - timedelta( minutes = self.maxCacheLifetime )
    
    for cache in self.__cacheNames:

      self.log.info( 'Inspecting %s' % cache )
      
      deleteCache = 'delete%s' % cache
      if not hasattr( self.rmClient, deleteCache ):
        self.log.warn( '%s not found' % deleteCache )
        continue
            
      deleteMethod = getattr( self.rmClient, deleteCache )      
      deleteResults = deleteMethod( meta = { 'older' : ( 'LastCheckTime', lastValidRecord ) } )
      if not deleteResults[ 'OK' ]:
        self.log.error( deleteResults[ 'Message' ] )
        continue
      if deleteResults[ 'Value' ]:
        self.log.info( 'Deleted %s entries' % deleteResults[ 'Value' ] )
      else:
        self.log.info( '... nothing to delete')
    
    return S_OK()

  def _cleanStatusTable( self, tableType, lifeTime ):
    '''
      Method that deletes all entries older than now - lifeTime ( days ) for all
      the elementType tables for a given tableType ( History / Log )
    '''
    
    self.log.info( 'Cleaning %s entries older than %s days' % ( tableType, lifeTime ) )
    
    #It is hard-coded, mainly because there are no more tables going to be added
    #to the schema for a long time.
    elements = ( 'Site', 'Resource', 'Node' )
    
    lastValidRecord = datetime.utcnow() - timedelta( days = lifeTime )
    meta            = { 'older' : ( 'LastCheckTime', lastValidRecord ) }
    
    for element in elements:
      
      self.log.info( 'Inspecting %s%s' % ( element, tableType )  )
      
      deleteResults = self.rsClient.deleteStatusElement( element, tableType, 
                                                         meta = meta )
      if not deleteResults[ 'OK' ]:
        self.log.error( deleteResults[ 'Message' ] )
        continue
      if deleteResults[ 'Value' ]:
        self.log.info( 'Deleted %s entries' % deleteResults[ 'Value' ] )
      else:
        self.log.info( '... nothing to delete')
    
    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF