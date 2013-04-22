# $HeadURL:  $
''' Synchronizer

  Module that keeps the database 

'''

__RCSID__ = '$Id:  $'

from DIRAC                                                 import gLogger, S_OK
from DIRAC.ResourceStatusSystem.Client                     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client                     import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Utilities                  import CSHelpers
from DIRAC.ResourceStatusSystem.Utilities.RssConfiguration import RssConfiguration

class Synchronizer( object ):
  '''
  Every time there is a successful write on the CS, Synchronizer().sync() is 
  executed. It updates the database with the values on the CS.
  
  '''
  
  def __init__( self, rStatus = None, rManagement = None ):
    
    # Warm up local CS
    CSHelpers.warmUp()
    
    if rStatus is None:
      self.rStatus     = ResourceStatusClient.ResourceStatusClient()
    if rManagement is None:   
      self.rManagement = ResourceManagementClient.ResourceManagementClient()
      
    self.rssConfig = RssConfiguration()  
  
  def sync( self, _eventName, _params ):
    '''
      Main synchronizer method. It syncs the three types of elements: Sites,
      Resources and Nodes.
    '''
    
    syncSites = self._syncSites()
    if not syncSites[ 'OK' ]:
      gLogger.error( syncSites[ 'Message' ] )
      
    syncResources = self._syncResources()
    if not syncResources[ 'OK' ]:
      gLogger.error( syncResources[ 'Message' ] )  
  
    syncNodes = self._syncNodes()
    if not syncNodes[ 'OK' ]:
      gLogger.error( syncNodes[ 'Message' ] )  
  
    #FIXME: also sync users
    
    return S_OK()
  
  ## Protected methods #########################################################
  
  def _syncSites( self ):
    '''
      Sync sites: compares CS with DB and does the necessary modifications.
    '''
    
    gLogger.info( '-- Synchronizing sites --')
    
    domainSitesCS = CSHelpers.getDomainSites()
    if not domainSitesCS[ 'OK' ]:
      return domainSitesCS
    domainSitesCS = domainSitesCS[ 'Value' ]
    
    for domainName, sitesCS in domainSitesCS.items():
    
      gLogger.verbose( '%s sites found in CS for %s domain' % ( len( sitesCS ), domainName ) )
    
      sitesDB = self.rStatus.selectStatusElement( 'Site', 'Status', elementType = domainName,
                                                  meta = { 'columns' : [ 'name' ] } ) 
      if not sitesDB[ 'OK' ]:
        return sitesDB    
      sitesDB = [ siteDB[0] for siteDB in sitesDB[ 'Value' ] ]
       
      # Sites that are in DB but not in CS
      toBeDeleted = list( set( sitesDB ).difference( set( sitesCS ) ) )
      gLogger.verbose( '%s sites to be deleted' % len( toBeDeleted ) )
    
      # Delete sites
      for siteName in toBeDeleted:
      
        deleteQuery = self.rStatus._extermineStatusElement( 'Site', siteName )
      
        gLogger.verbose( '... %s' % siteName )
        if not deleteQuery[ 'OK' ]:
          return deleteQuery         

      sitesTuple  = self.rStatus.selectStatusElement( 'Site', 'Status', elementType = domainName, 
                                                      meta = { 'columns' : [ 'name', 'statusType' ] } ) 
      if not sitesTuple[ 'OK' ]:
        return sitesTuple   
      sitesTuple = sitesTuple[ 'Value' ]

      statusTypes = self.rssConfig.getConfigStatusType( domainName )
    
      # For each ( site, statusType ) tuple not present in the DB, add it.
      siteStatusTuples = [ ( site, statusType ) for site in sitesCS for statusType in statusTypes ]     
      toBeAdded = list( set( siteStatusTuples ).difference( set( sitesTuple ) ) )
    
      gLogger.verbose( '%s site entries to be added' % len( toBeAdded ) )
  
      for siteTuple in toBeAdded:
      
        query = self.rStatus.addIfNotThereStatusElement( 'Site', 'Status', 
                                                         name = siteTuple[ 0 ], 
                                                         statusType = siteTuple[ 1 ], 
                                                         status = 'Unknown', 
                                                         elementType = domainName, 
                                                         reason = 'Synchronized' )
        if not query[ 'OK' ]:
          return query
      
    return S_OK()  
  
  def _syncResources( self ):
    '''
      Sync resources: compares CS with DB and does the necessary modifications.
      ( StorageElements, FTS, FileCatalogs and ComputingElements )
    '''
    
    gLogger.info( '-- Synchronizing Resources --' )
    
    gLogger.verbose( '-> StorageElements' )
    ses = self.__syncStorageElements()
    if not ses[ 'OK' ]:
      gLogger.error( ses[ 'Message' ] )
    
    gLogger.verbose( '-> FTS' )
    fts = self.__syncFTS()
    if not fts[ 'OK' ]:
      gLogger.error( fts[ 'Message' ] )
    
    gLogger.verbose( '-> FileCatalogs' )
    fileCatalogs = self.__syncFileCatalogs()
    if not fileCatalogs[ 'OK' ]:
      gLogger.error( fileCatalogs[ 'Message' ] ) 

    gLogger.verbose( '-> ComputingElements' )
    computingElements = self.__syncComputingElements()
    if not computingElements[ 'OK' ]:
      gLogger.error( computingElements[ 'Message' ] )

    #FIXME: VOMS

    return S_OK()

  def _syncNodes( self ):
    '''
      Sync resources: compares CS with DB and does the necessary modifications.
      ( Queues )
    '''
    gLogger.info( '-- Synchronizing Nodes --' )
  
    gLogger.verbose( '-> Queues' )
    queues = self.__syncQueues()
    if not queues[ 'OK' ]:
      gLogger.error( queues[ 'Message' ] )
    
    return S_OK()  

  ## Private methods ###########################################################

  def __syncComputingElements( self ): 
    '''
      Sync CEs: compares CS with DB and does the necessary modifications.
    '''
    
    cesCS = CSHelpers.getComputingElements()
    if not cesCS[ 'OK' ]:
      return cesCS
    cesCS = cesCS[ 'Value' ]        
    
    gLogger.verbose( '%s Computing elements found in CS' % len( cesCS ) )
    
    cesDB = self.rStatus.selectStatusElement( 'Resource', 'Status', 
                                                   elementType = 'ComputingElement',
                                                   meta = { 'columns' : [ 'name' ] } ) 
    if not cesDB[ 'OK' ]:
      return cesDB    
    cesDB = [ ceDB[0] for ceDB in cesDB[ 'Value' ] ]
       
    # ComputingElements that are in DB but not in CS
    toBeDeleted = list( set( cesDB ).difference( set( cesDB ) ) )
    gLogger.verbose( '%s Computing elements to be deleted' % len( toBeDeleted ) )
       
    # Delete storage elements
    for ceName in toBeDeleted:
      
      deleteQuery = self.rStatus._extermineStatusElement( 'Resource', ceName )
      
      gLogger.verbose( '... %s' % ceName )
      if not deleteQuery[ 'OK' ]:
        return deleteQuery            
    
    statusTypes = self.rssConfig.getConfigStatusType( 'ComputingElement' )

    cesTuple = self.rStatus.selectStatusElement( 'Resource', 'Status', 
                                                 elementType = 'ComputingElement', 
                                                 meta = { 'columns' : [ 'name', 'statusType' ] } ) 
    if not cesTuple[ 'OK' ]:
      return cesTuple   
    cesTuple = cesTuple[ 'Value' ]        
  
    # For each ( se, statusType ) tuple not present in the DB, add it.
    cesStatusTuples = [ ( se, statusType ) for se in cesCS for statusType in statusTypes ]     
    toBeAdded = list( set( cesStatusTuples ).difference( set( cesTuple ) ) )
    
    gLogger.debug( '%s Computing elements entries to be added' % len( toBeAdded ) )
  
    for ceTuple in toBeAdded:
      
      _name            = ceTuple[ 0 ]
      _statusType      = ceTuple[ 1 ]
      _status          = 'Unknown'
      _reason          = 'Synchronized'
      _elementType     = 'ComputingElement'
      
      query = self.rStatus.addIfNotThereStatusElement( 'Resource', 'Status', name = _name, 
                                                       statusType = _statusType,
                                                       status = _status,
                                                       elementType = _elementType, 
                                                       reason = _reason )
      if not query[ 'OK' ]:
        return query
      
    return S_OK()    
  
  def __syncFileCatalogs( self ): 
    '''
      Sync FileCatalogs: compares CS with DB and does the necessary modifications.
    '''
        
    catalogsCS = CSHelpers.getFileCatalogs()
    if not catalogsCS[ 'OK' ]:
      return catalogsCS
    catalogsCS = catalogsCS[ 'Value' ]        
    
    gLogger.verbose( '%s File catalogs found in CS' % len( catalogsCS ) )
    
    catalogsDB = self.rStatus.selectStatusElement( 'Resource', 'Status', 
                                                   elementType = 'Catalog',
                                                   meta = { 'columns' : [ 'name' ] } ) 
    if not catalogsDB[ 'OK' ]:
      return catalogsDB    
    catalogsDB = [ catalogDB[0] for catalogDB in catalogsDB[ 'Value' ] ]
       
    # StorageElements that are in DB but not in CS
    toBeDeleted = list( set( catalogsDB ).difference( set( catalogsCS ) ) )
    gLogger.verbose( '%s File catalogs to be deleted' % len( toBeDeleted ) )
       
    # Delete storage elements
    for catalogName in toBeDeleted:
      
      deleteQuery = self.rStatus._extermineStatusElement( 'Resource', catalogName )
      
      gLogger.verbose( '... %s' % catalogName )
      if not deleteQuery[ 'OK' ]:
        return deleteQuery            
    
    #statusTypes = RssConfiguration.getValidStatusTypes()[ 'Resource' ]
    statusTypes = self.rssConfig.getConfigStatusType( 'Catalog' )

    sesTuple = self.rStatus.selectStatusElement( 'Resource', 'Status', 
                                                 elementType = 'Catalog', 
                                                 meta = { 'columns' : [ 'name', 'statusType' ] } ) 
    if not sesTuple[ 'OK' ]:
      return sesTuple   
    sesTuple = sesTuple[ 'Value' ]        
  
    # For each ( se, statusType ) tuple not present in the DB, add it.
    catalogsStatusTuples = [ ( se, statusType ) for se in catalogsCS for statusType in statusTypes ]     
    toBeAdded = list( set( catalogsStatusTuples ).difference( set( sesTuple ) ) )
    
    gLogger.verbose( '%s File catalogs entries to be added' % len( toBeAdded ) )
  
    for catalogTuple in toBeAdded:
      
      _name            = catalogTuple[ 0 ]
      _statusType      = catalogTuple[ 1 ]
      _status          = 'Unknown'
      _reason          = 'Synchronized'
      _elementType     = 'Catalog'
      
      query = self.rStatus.addIfNotThereStatusElement( 'Resource', 'Status', name = _name, 
                                                       statusType = _statusType,
                                                       status = _status,
                                                       elementType = _elementType, 
                                                       reason = _reason )
      if not query[ 'OK' ]:
        return query
      
    return S_OK()      

  def __syncFTS( self ): 
    '''
      Sync FTS: compares CS with DB and does the necessary modifications.
    '''
        
    ftsCS = CSHelpers.getFTS()
    if not ftsCS[ 'OK' ]:
      return ftsCS
    ftsCS = ftsCS[ 'Value' ]        
    
    gLogger.verbose( '%s FTS endpoints found in CS' % len( ftsCS ) )
    
    ftsDB = self.rStatus.selectStatusElement( 'Resource', 'Status', 
                                              elementType = 'FTS',
                                              meta = { 'columns' : [ 'name' ] } ) 
    if not ftsDB[ 'OK' ]:
      return ftsDB    
    ftsDB = [ fts[0] for fts in ftsDB[ 'Value' ] ]
       
    # StorageElements that are in DB but not in CS
    toBeDeleted = list( set( ftsDB ).difference( set( ftsCS ) ) )
    gLogger.verbose( '%s FTS endpoints to be deleted' % len( toBeDeleted ) )
       
    # Delete storage elements
    for ftsName in toBeDeleted:
      
      deleteQuery = self.rStatus._extermineStatusElement( 'Resource', ftsName )
      
      gLogger.verbose( '... %s' % ftsName )
      if not deleteQuery[ 'OK' ]:
        return deleteQuery            
    
    statusTypes = self.rssConfig.getConfigStatusType( 'FTS' )
    #statusTypes = RssConfiguration.getValidStatusTypes()[ 'Resource' ]

    sesTuple = self.rStatus.selectStatusElement( 'Resource', 'Status', 
                                                 elementType = 'FTS', 
                                                 meta = { 'columns' : [ 'name', 'statusType' ] } ) 
    if not sesTuple[ 'OK' ]:
      return sesTuple   
    sesTuple = sesTuple[ 'Value' ]        
  
    # For each ( se, statusType ) tuple not present in the DB, add it.
    ftsStatusTuples = [ ( se, statusType ) for se in ftsCS for statusType in statusTypes ]     
    toBeAdded = list( set( ftsStatusTuples ).difference( set( sesTuple ) ) )
    
    gLogger.verbose( '%s FTS endpoints entries to be added' % len( toBeAdded ) )
  
    for ftsTuple in toBeAdded:
      
      _name            = ftsTuple[ 0 ]
      _statusType      = ftsTuple[ 1 ]
      _status          = 'Unknown'
      _reason          = 'Synchronized'
      _elementType     = 'FTS'
      
      query = self.rStatus.addIfNotThereStatusElement( 'Resource', 'Status', name = _name, 
                                                       statusType = _statusType,
                                                       status = _status,
                                                       elementType = _elementType, 
                                                       reason = _reason )
      if not query[ 'OK' ]:
        return query
      
    return S_OK()      
 
  def __syncStorageElements( self ): 
    '''
      Sync StorageElements: compares CS with DB and does the necessary modifications.
    '''
        
    sesCS = CSHelpers.getStorageElements()
    if not sesCS[ 'OK' ]:
      return sesCS
    sesCS = sesCS[ 'Value' ]        
    
    gLogger.verbose( '%s storage elements found in CS' % len( sesCS ) )
    
    sesDB = self.rStatus.selectStatusElement( 'Resource', 'Status', 
                                              elementType = 'StorageElement',
                                              meta = { 'columns' : [ 'name' ] } ) 
    if not sesDB[ 'OK' ]:
      return sesDB    
    sesDB = [ seDB[0] for seDB in sesDB[ 'Value' ] ]
       
    # StorageElements that are in DB but not in CS
    toBeDeleted = list( set( sesDB ).difference( set( sesCS ) ) )
    gLogger.verbose( '%s storage elements to be deleted' % len( toBeDeleted ) )
       
    # Delete storage elements
    for sesName in toBeDeleted:
      
      deleteQuery = self.rStatus._extermineStatusElement( 'Resource', sesName )
      
      gLogger.verbose( '... %s' % sesName )
      if not deleteQuery[ 'OK' ]:
        return deleteQuery            
    
    statusTypes = self.rssConfig.getConfigStatusType( 'StorageElement' )
    #statusTypes = RssConfiguration.getValidStatusTypes()[ 'Resource' ]

    sesTuple = self.rStatus.selectStatusElement( 'Resource', 'Status', 
                                                 elementType = 'StorageElement', 
                                                 meta = { 'columns' : [ 'name', 'statusType' ] } ) 
    if not sesTuple[ 'OK' ]:
      return sesTuple   
    sesTuple = sesTuple[ 'Value' ]        
  
    # For each ( se, statusType ) tuple not present in the DB, add it.
    sesStatusTuples = [ ( se, statusType ) for se in sesCS for statusType in statusTypes ]     
    toBeAdded = list( set( sesStatusTuples ).difference( set( sesTuple ) ) )
    
    gLogger.verbose( '%s storage element entries to be added' % len( toBeAdded ) )
  
    for seTuple in toBeAdded:
      
      _name            = seTuple[ 0 ]
      _statusType      = seTuple[ 1 ]
      _status          = 'Unknown'
      _reason          = 'Synchronized'
      _elementType     = 'StorageElement'
      
      query = self.rStatus.addIfNotThereStatusElement( 'Resource', 'Status', name = _name, 
                                                       statusType = _statusType,
                                                       status = _status,
                                                       elementType = _elementType, 
                                                       reason = _reason )
      if not query[ 'OK' ]:
        return query
      
    return S_OK()  

  def __syncQueues( self ):
    '''
      Sync Queues: compares CS with DB and does the necessary modifications.
    '''

    queuesCS = CSHelpers.getQueues()
    if not queuesCS[ 'OK' ]:
      return queuesCS
    queuesCS = queuesCS[ 'Value' ]        
    
    gLogger.verbose( '%s Queues found in CS' % len( queuesCS ) )
    
    queuesDB = self.rStatus.selectStatusElement( 'Node', 'Status', 
                                                 elementType = 'Queue',
                                                 meta = { 'columns' : [ 'name' ] } ) 
    if not queuesDB[ 'OK' ]:
      return queuesDB    
    queuesDB = [ queueDB[0] for queueDB in queuesDB[ 'Value' ] ]
       
    # ComputingElements that are in DB but not in CS
    toBeDeleted = list( set( queuesDB ).difference( set( queuesDB ) ) )
    gLogger.verbose( '%s Queues to be deleted' % len( toBeDeleted ) )
       
    # Delete storage elements
    for queueName in toBeDeleted:
      
      deleteQuery = self.rStatus._extermineStatusElement( 'Node', queueName )
      
      gLogger.verbose( '... %s' % queueName )
      if not deleteQuery[ 'OK' ]:
        return deleteQuery            
    
    statusTypes = self.rssConfig.getConfigStatusType( 'Queue' )
    #statusTypes = RssConfiguration.getValidStatusTypes()[ 'Node' ]

    queueTuple = self.rStatus.selectStatusElement( 'Node', 'Status', 
                                                   elementType = 'Queue', 
                                                   meta = { 'columns' : [ 'name', 'statusType' ] } ) 
    if not queueTuple[ 'OK' ]:
      return queueTuple   
    queueTuple = queueTuple[ 'Value' ]        
  
    # For each ( se, statusType ) tuple not present in the DB, add it.
    queueStatusTuples = [ ( se, statusType ) for se in queuesCS for statusType in statusTypes ]     
    toBeAdded = list( set( queueStatusTuples ).difference( set( queueTuple ) ) )
    
    gLogger.verbose( '%s Queue entries to be added' % len( toBeAdded ) )
  
    for queueTuple in toBeAdded:
      
      _name            = queueTuple[ 0 ]
      _statusType      = queueTuple[ 1 ]
      _status          = 'Unknown'
      _reason          = 'Synchronized'
      _elementType     = 'Queue'
      
      query = self.rStatus.addIfNotThereStatusElement( 'Node', 'Status', name = _name, 
                                                       statusType = _statusType,
                                                       status = _status,
                                                       elementType = _elementType, 
                                                       reason = _reason )
      if not query[ 'OK' ]:
        return query
      
    return S_OK()      
  
  def _syncUsers( self ):
    '''
      Sync Users: compares CS with DB and does the necessary modifications.
    '''    
    
    gLogger.verbose( '-- Synchronizing users --')
    
    usersCS = CSHelpers.getRegistryUsers()
    if not usersCS[ 'OK' ]:
      return usersCS
    usersCS = usersCS[ 'Value' ]
    
    gLogger.verbose( '%s users found in CS' % len( usersCS ) )
    
    usersDB = self.rManagement.selectUserRegistryCache( meta = { 'columns' : [ 'login' ] } ) 
    if not usersDB[ 'OK' ]:
      return usersDB    
    usersDB = [ userDB[0] for userDB in usersDB[ 'Value' ] ]
    
    # Users that are in DB but not in CS
    toBeDeleted = list( set( usersDB ).difference( set( usersCS.keys() ) ) )
    gLogger.verbose( '%s users to be deleted' % len( toBeDeleted ) )
    
    # Delete users
    # FIXME: probably it is not needed since there is a DatabaseCleanerAgent
    for userLogin in toBeDeleted:
      
      deleteQuery = self.rManagement.deleteUserRegistryCache( login = userLogin )
      
      gLogger.verbose( '... %s' % userLogin )
      if not deleteQuery[ 'OK' ]:
        return deleteQuery      
     
    # AddOrModify Users 
    for userLogin, userDict in usersCS.items():
      
      _name  = userDict[ 'DN' ].split( '=' )[ -1 ]
      _email = userDict[ 'Email' ]
      
      query = self.rManagement.addOrModifyUserRegistryCache( userLogin, _name, _email )
      gLogger.verbose( '-> %s' % userLogin )
      if not query[ 'OK' ]:
        return query     
  
    return S_OK()
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  