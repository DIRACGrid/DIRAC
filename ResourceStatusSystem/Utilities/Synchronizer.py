# $HeadURL:  $
''' Synchronizer

  Module that keeps the database 

'''

__RCSID__ = '$Id:  $'

from DIRAC                                import gLogger, S_OK
from DIRAC.ResourceStatusSystem.Client    import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client    import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Utilities import CSHelpers, RssConfiguration


class Synchronizer( object ):
  
  def __init__( self ):
    
    # Warm up local CS
    CSHelpers.warmUp()
    
    self.rStatus     = ResourceStatusClient.ResourceStatusClient() 
    self.rManagement = ResourceManagementClient.ResourceManagementClient()
  
  def _syncSites( self ):
    
    gLogger.debug( '-- Synchronizing sites --')
    
    sitesCS = CSHelpers.getSites()
    if not sitesCS[ 'OK' ]:
      return sitesCS
    sitesCS = sitesCS[ 'Value' ]
    
    gLogger.debug( '%s sites found in CS' % len( sitesCS ) )
    
    sitesDB = self.rStatus.selectStatusElement( 'Site', 'Status', 
                                                meta = { 'columns' : [ 'name' ] } ) 
    if not sitesDB[ 'OK' ]:
      return sitesDB    
    sitesDB = sitesDB[ 'Value' ]
       
    # Sites that are in DB but not in CS
    toBeDeleted = list( set( sitesDB.keys() ).intersection( set( sitesCS ) ) )
    gLogger.debug( '%s sites to be deleted' % len( toBeDeleted ) )
    
    # Delete sites
    for siteName in toBeDeleted:
      
      deleteQuery = self.rStatus._extermineStatusElement( 'Site', siteName )
      
      gLogger.debug( '... %s' % siteName )
      if not deleteQuery[ 'OK' ]:
        return deleteQuery         

    statusTypes = RssConfiguration.getValidStatusTypes()
    if not statusTypes[ 'OK' ]:
      return statusTypes
    statusTypes = statusTypes[ 'Value' ]

    sitesTuple = self.rStatus.selectStatusElement( 'Site', 'Status', 
                                                   meta = { 'columns' : [ 'name', 'statusType' ] } ) 
    if not sitesTuple[ 'OK' ]:
      return sitesTuple   
    sitesTuple = sitesTuple[ 'Value' ]
    
    # For each ( site, statusType ) tuple not present in the DB, add it.
    siteStatusTuples = [ ( site, statusType ) for site in sitesTuple for statusType in statusTypes ]     
    toBeAdded = list( set( siteStatusTuples ).difference( set( sitesTuple ) ) )
    
    gLogger.debug( '%s site entries to be added' % len( toBeAdded ) )
  
    for siteTuple in toBeAdded:
      
      _name            = siteTuple[ 0 ]
      _statusType      = siteTuple[ 1 ]
      _reason          = 'Synchronzed'
      
      query = self.rStatus.addOrModifyStatusElement( 'Site', 'Status', name = _name, 
                                                     statusType = _statusType, 
                                                     reason = _reason )
      if not query[ 'OK' ]:
        return query
      
    return S_OK()  
  
  def _syncResources( self ):
    
    gLogger.debug( '-- Synchronizing Resources --')
    gLogger.debug( '-> StorageElements' )
    
    sesCS = CSHelpers.getStorageElements()
    if not sesCS[ 'OK' ]:
      return sesCS
    sesCS = sesCS[ 'Value' ]        
    
    gLogger.debug( '%s storage elements found in CS' % len( sesCS ) )
    
    sesDB = self.rStatus.selectStatusElement( 'Resource', 'Status', 
                                              meta = { 'columns' : [ 'name' ] } ) 
    if not sesDB[ 'OK' ]:
      return sesDB    
    sesDB = sesDB[ 'Value' ]
       
    # StorageElements that are in DB but not in CS
    toBeDeleted = list( set( sesDB.keys() ).intersection( set( sesCS ) ) )
    gLogger.debug( '%s storage elements to be deleted' % len( toBeDeleted ) )
    
# WE CANNOT DELETE THIS WAY, as we cannot know what is what. We have to gather
# all resources and delete after.    
    
#    # Delete storage elements
#    for sesName in toBeDeleted:
#      
#      deleteQuery = self.rStatus._extermineStatusElement( 'Resource', sesName )
#      
#      gLogger.debug( '... %s' % sesName )
#      if not deleteQuery[ 'OK' ]:
#        return deleteQuery            
    
    statusTypes = RssConfiguration.getValidStatusTypes()
    if not statusTypes[ 'OK' ]:
      return statusTypes
    statusTypes = statusTypes[ 'Value' ]

    sesTuple = self.rStatus.selectStatusElement( 'Resource', 'Status', 
                                                   meta = { 'columns' : [ 'name', 'statusType' ] } ) 
    if not sesTuple[ 'OK' ]:
      return sesTuple   
    sesTuple = sesTuple[ 'Value' ]        
  
    # For each ( se, statusType ) tuple not present in the DB, add it.
    sesStatusTuples = [ ( se, statusType ) for se in sesTuple for statusType in statusTypes ]     
    toBeAdded = list( set( sesStatusTuples ).difference( set( sesTuple ) ) )
    
    gLogger.debug( '%s storage element entries to be added' % len( toBeAdded ) )
  
    for seTuple in toBeAdded:
      
      _name            = seTuple[ 0 ]
      _statusType      = seTuple[ 1 ]
      _reason          = 'Synchronzed'
      
      query = self.rStatus.addOrModifyStatusElement( 'Resource', 'Status', name = _name, 
                                                     statusType = _statusType, 
                                                     reason = _reason )
      if not query[ 'OK' ]:
        return query
      
    return S_OK()  
  
  def _syncNodes( self ):
    pass
  
  def _syncUsers( self ):
    
    gLogger.debug( '-- Synchronizing users --')
    
    usersCS = CSHelpers.getRegistryUsers()
    if not usersCS[ 'OK' ]:
      return usersCS
    usersCS = usersCS[ 'Value' ]
    
    gLogger.debug( '%s users found in CS' % len( usersCS ) )
    
    usersDB = self.rManagement.selectUserRegistryCache( meta = { 'columns' : [ 'login' ] } ) 
    if not usersDB[ 'OK' ]:
      return usersDB    
    usersDB = usersDB[ 'Value' ]
    
    # Users that are in DB but not in CS
    toBeDeleted = list( set( usersDB ).intersection( set( usersCS.keys() ) ) )
    gLogger.debug( '%s users to be deleted' % len( toBeDeleted ) )
    
    # Delete users
    for userLogin in toBeDeleted:
      
      deleteQuery = self.rManagement.deleteUserRegistryCache( login = userLogin )
      
      gLogger.debug( '... %s' % userLogin )
      if not deleteQuery[ 'OK' ]:
        return deleteQuery      
     
    # AddOrModify Users 
    for userLogin, userDict in usersCS.items():
      
      _name  = userDict[ 'DN' ].split( '=' )[ -1 ]
      _email = userDict[ 'Email' ]
      
      query = self.rManagement.addOrModifyUserRegistryCache( userLogin, _name, _email )
      gLogger.debug( '-> %s' % userLogin )
      if not query[ 'OK' ]:
        return query      
  
    return S_OK()
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  