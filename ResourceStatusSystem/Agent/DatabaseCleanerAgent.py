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

  # Max number of hours for the cache tables
  __maxCacheLifetime = 2
  
  # Max number of days for the history tables
  __maxHistoryLifetime = 60

  # Max number of days for the log tables
  __maxLogLifetime = 20

  def __init__( self, agentName, baseAgentName = False, properties = dict() ):
    
    AgentModule.__init__( self, agentName, baseAgentName, properties ) 
    
    self.rsClient = None
    self.rmClient = None

  def initialize( self ):
    
    self.rsClient = ResourceStatusClient()
    self.rmClient = ResourceManagementClient()
    
    return S_OK()
  
  def execute( self ):

    self._cleanCaches()
    self._cleanStatusTable( 'History', self.__maxHistoryLifetime )
    self._cleanStatusTable( 'Log', self.__maxLogLifetime )
    
    return S_OK()
    
  ## Protected methods #########################################################
    
  def _cleanCaches( self ):
    '''
      Method that iterates over all the caches in ResourceManagemetnDB deleting
      entries with a LastCheckTime parameter older than now - X( hours ). On theory,
      there should not be any parameter to be deleted. If there are, it means that
      by some reason that entry has not been updated.
    '''
        
    #FIXME: this two are special caches 'AccountingCache', 'DowntimeCache'     
    caches = ( 'JobCache', 'PilotCache', 'TransferCache', 'VOBOXCache', 
               'SpaceTokenOccupancyCache' )
    
    
    lastValidRecord = datetime.utcnow() - timedelta( hours = self.__maxCacheLifetime )
    
    for cache in caches:

      self.log.info( 'Inspecting %s' % cache )
      
      selectCache = 'select%s' % cache
      
      if not hasattr( self.rmClient, selectCache ):
        self.log.warn( '%s not found' % selectCache )
        continue
      
      selectMethod = getattr( self.rmClient, selectCache )
      
      selectResults = selectMethod( meta = { 'older' : ( 'LastCheckTime', lastValidRecord ) } )
      if not selectResults[ 'OK' ]:
        self.log.error( selectResults[ 'Message' ] )
        continue
      selectResults = selectResults[ 'Value' ]
      
      if selectResults:
        self.log.warn( 'It seems there are non updated records' )
        for selectResult in selectResults:
          self.log.warn( selectResult )
      
      deleteCache = 'delete%s' % cache
      if not hasattr( self.rmClient, deleteCache ):
        self.log.warn( '%s not found' % deleteCache )
        continue
            
      deleteMethod = getattr( self.rmClient, deleteCache )      
      deleteResults = deleteMethod( meta = { 'older' : ( 'LastCheckTime', lastValidRecord ) } )
      if not deleteResults[ 'OK' ]:
        self.log.error( deleteResults[ 'Message' ] )
        continue
      self.log.info( 'Deleted %s entries' % deleteResults[ 'Value' ] )
    
    return S_OK()

  def _cleanStatusTable( self, tableType, lifeTime ):
    '''
      Method that deletes all entries older than now - lifeTime ( days ) for all
      the elementType tables for a given tableType ( History / Log )
    '''
    
    #It is hard-coded, mainly because there are no more tables going to be added
    #to the schema for a long time.
    elements = ( 'Site', 'Resource', 'Node' )
    
    lastValidRecord = datetime.utcnow() - timedelta( days = lifeTime )
    
    for element in elements:
      
      deleteResults = self.rsClient.deleteStatusElement( element, tableType, 
                                         meta = { 'older' : ( 'LastCheckTime', lastValidRecord ) })
      if not deleteResults[ 'OK' ]:
        self.log.error( deleteResults[ 'Message' ] )
        continue
      self.log.info( 'Deleted %s entries' % deleteResults[ 'Value' ] )
    
    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF