# $HeadURL:  $
''' Synchronizer

  Module that updates the RSS database ( ResourceStatusDB ) with the information
  in the Resources section. If there are additions in the CS, those are incorporated
  to the DB. If there are deletions, entries in RSS tables for those elements are
  deleted ( except the Logs table ).

'''

__RCSID__ = '$Id:  $'

from DIRAC                                                 import gConfig, gLogger, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Operations   import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources    import Resources, RESOURCE_NODE_MAPPING
from DIRAC.Interfaces.API.DiracAdmin                       import DiracAdmin
from DIRAC.ResourceStatusSystem.Client                     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities.RssConfiguration import RssConfiguration

class Synchronizer( object ):
  '''
  Every time there is a successful write on the CS, Synchronizer().sync() is 
  executed. It updates the database with the values on the CS.
  '''
  
  def __init__( self ):
    """
    Constructor.
    
    examples:
      >>> s = Synchronizer()
    """
    
    self.log        = gLogger.getSubLogger( self.__class__.__name__ )
    self.operations = Operations()
    self.resources  = Resources()
    
    self.rStatus    = ResourceStatusClient.ResourceStatusClient()  
    self.rssConfig  = RssConfiguration()
  
    self.diracAdmin = DiracAdmin()
  
  def sync( self, _eventName, _params ):
    '''
    Main synchronizer method. It synchronizes the three types of elements: Sites,
    Resources and Nodes. Each _syncX method returns a dictionary with the additions
    and deletions.
    
    examples:
      >>> s.sync( None, None )
          S_OK()
    
    :Parameters:
      **_eventName** - any
        this parameter is ignored, but needed by caller function.
      **_params** - any
        this parameter is ignored, but needed by caller function.
    
    :return: S_OK
    '''
    
    defSyncResult = { 'added' : [], 'deleted' : [] }
    
    # Sites
    syncSites = self._syncSites()
    if not syncSites[ 'OK' ]:
      self.log.error( syncSites[ 'Message' ] )
    syncSites = ( syncSites[ 'OK' ] and syncSites[ 'Value' ] ) or defSyncResult
    
    # Resources
    syncResources = self._syncResources()
    if not syncResources[ 'OK' ]:
      self.log.error( syncResources[ 'Message' ] )
    syncResources = ( syncResources[ 'OK' ] and syncResources[ 'Value' ] ) or defSyncResult 
    
    # Nodes
    syncNodes = self._syncNodes()
    if not syncNodes[ 'OK' ]:
      self.log.error( syncNodes[ 'Message' ] )
    syncNodes = ( syncNodes[ 'OK' ] and syncNodes[ 'Value' ] ) or defSyncResult
      
    # Notify via email to :  
    self.notify( syncSites, syncResources, syncNodes )
    
    return S_OK()

  def notify( self, syncSites, syncResources, syncNodes ):
    """
    Method sending email notification with the result of the synchronization. Email
    is sent to Operations( EMail/Production ) email address.
    
    examples:
      >>> s.notify( {}, {}, {} )
      >>> s.notify( { 'Site' : { 'added' : [], 'deleted' : [ 'RubbishSite' ] }, {}, {} )
      >>> s.notify( { 'Site' : { 'added' : [], 'deleted' : [ 'RubbishSite' ] }, 
                    { 'Computing : { 'added' : [ 'newCE01', 'newCE02' ], 'deleted' : [] }}, {} )
    
    :Parameters:
      **syncSites** - dict() ( keys: added, deleted )
        dictionary with the sites added and deleted from the DB
      **syncResources** - dict() ( keys: added, deleted )
        dictionary with the resources added and deleted from the DB
      **syncNodes** - dict() ( keys: added, deleted )
        dictionary with the nodes added and deleted from the DB
      
    :return: S_OK
    """
    
    # Human readable summary
    msgBody = self.getBody( syncSites, syncResources, syncNodes ) 
    self.log.info( msgBody )
    
    # Email addresses
    toAddress   = self.operations.getValue( 'EMail/Production', '' )
    fromAddress = self.rssConfig.getConfigFromAddress( '' )
    
    if toAddress and fromAddress and msgBody:
      
      # Subject of the email
      setup   = gConfig.getValue( 'DIRAC/Setup' )
      subject = '[RSS](%s) CS Synchronization' % setup
      
      self.diracAdmin.sendMail( toAddress, subject, msgBody, fromAddress = fromAddress )
     
  def getBody( self, syncSites, syncResources, syncNodes ):
    """
    Method that given the outputs of the three synchronization methods builds a
    human readable string.
    
    examples:
      >>> s.getBody( {}, {}, {} )
          ''
      >>> s.getBody( { 'Site' : { 'added' : [], 'deleted' : [ 'RubbishSite' ] }, {}, {} )
          '''
          SITES:
          Site:
            deleted:1
              RubbishSite
          '''
      >>> s.getBody( { 'Site' : { 'added' : [], 'deleted' : [ 'RubbishSite' ] }, 
                     { 'Computing : { 'added' : [ 'newCE01', 'newCE02' ], 'deleted' : [] }}, {} )    
          '''
          SITES:
          Site:
            deleted:1
              RubbishSite
          RESOURCES:
          Computing:
            added:2
              newCE01
              newCE02    
          '''
          
    :Parameters:
      **syncSites** - dict() ( keys: added, deleted )
        dictionary with the sites added and deleted from the DB
      **syncResources** - dict() ( keys: added, deleted )
        dictionary with the resources added and deleted from the DB
      **syncNodes** - dict() ( keys: added, deleted )
        dictionary with the nodes added and deleted from the DB
      
    :return: str    
    """
        
    syncMsg = ''
       
    for element, syncResult in [ ( 'SITES', syncSites ), ( 'RESOURCES', syncResources ), 
                                 ( 'NODES', syncNodes ) ]:
    
      elementsMsg = ''
    
      for elementType, elements in syncResult.items():
    
        elementMsg = ''
        if elements[ 'added' ]:
          elementMsg += '\n  %s added: %d \n' % ( elementType, len( elements[ 'added' ] ) )
          elementMsg += '    ' + '\n    '.join( elements[ 'added' ] ) 
        if elements[ 'deleted' ]:
          elementMsg += '\n  %s deleted: %d \n' % ( elementType, len( elements[ 'deleted' ] ) )
          elementMsg += '    ' + '\n    '.join( elements[ 'deleted' ] )    
          
        if elementMsg:
          elementsMsg += '\n\n%s:\n' % elementType
          elementsMsg += elementMsg
        
      if elementsMsg:
        syncMsg += '\n\n%s:' % element + elementsMsg

    return syncMsg 

  #.............................................................................
  # Sync methods: Site, Resource & Node

  def _syncSites( self ):
    """
    Method that synchronizes sites ( using their canonical name: CERN.ch ) with
    elementType = 'Site'. It gets from the CS the eligible site names and then
    synchronizes them with the DB. If not on the DB, they are added. If in the DB
    but not on the CS, they are deleted.
    
    examples:
      >> s._syncSites()
         S_OK( { 'Site' : { 'added' : [], 'deleted' : [ 'RubbishSite' ] } } )
    
    :return: S_OK( { 'Site' : { 'added' : [], 'deleted' : [] }} ) | S_ERROR
    """
    
    # Get site names from the CS
    foundSites = self.resources.getEligibleSites()
    if not foundSites[ 'OK' ]:
      return foundSites
       
    sites = {}
    
    # Synchronize with the DB
    resSync = self.__dbSync( 'Site', 'Site', foundSites[ 'Value' ] )
    if not resSync[ 'OK' ]:
      self.log.error( 'Error synchronizing Sites' )
      self.log.error( resSync[ 'Message' ] )
    else:
      sites = resSync[ 'Value' ]  
  
    return S_OK( { 'Site' : sites } )
    
  def _syncResources( self ):
    """
    Method that synchronizes resources as defined on RESOURCE_NODE_MAPPING dictionary
    keys. It makes one sync round per key ( elementType ). Gets from the CS the 
    eligible Resource/<elementType> names and then synchronizes them with the DB. 
    If not on the DB, they are added. If in the DB but not on the CS, they are deleted.
    
    examples:
      >>> s._syncResources() 
          S_OK( { 'Computing' : { 'added' : [ 'newCE01', 'newCE02' ], 'deleted' : [] },
                  'Storage'   : { 'added' : [], 'deleted' : [] },
                  ... } ) 
    
    :return: S_OK( { 'RESOURCE_NODE_MAPPINGKey1' : { 'added' : [], 'deleted' : [] }, ...} )
    """
    
    resources = {}
    
    # Iterate over the different elementTypes for Resource ( Computing, Storage... )
    for elementType in RESOURCE_NODE_MAPPING.keys():
      
      # Get Resource / <elementType> names from CS
      foundResources = self.resources.getEligibleResources( elementType )
      if not foundResources[ 'OK' ]:
        self.log.error( foundResources[ 'Message' ] )
        continue
      
      # Translate CS result into a list
      foundResources = foundResources[ 'Value' ]
      
      # Synchronize with the DB
      resSync = self.__dbSync( 'Resource', elementType, foundResources )
      if not resSync[ 'OK' ]:
        self.log.error( 'Error synchronizing %s %s' % ( 'Resource', elementType ) )
        self.log.error( resSync[ 'Message' ] )
      else: 
        resources[ elementType ] = resSync[ 'Value' ] 
  
    return S_OK( resources )

  def _syncNodes( self ):
    """
    Method that synchronizes resources as defined on RESOURCE_NODE_MAPPING dictionary
    values. It makes one sync round per key ( elementType ). Gets from the CS the 
    eligible Node/<elementType> names and then synchronizes them with the DB. 
    If not on the DB, they are added. If in the DB but not on the CS, they are deleted.
    
    examples:
      >>> s._syncNodes() 
          S_OK( { 'Queue' : { 'added' : [], 'deleted' : [] },
                  ... } ) 
    
    :return: S_OK( { 'RESOURCE_NODE_MAPPINGValue1' : { 'added' : [], 'deleted' : [] }, ...} )
    """
    
    nodes = {}
    
    # Iterate over the different elementTypes for Node ( Queue, AccessProtocol... )
    for elementType in RESOURCE_NODE_MAPPING.values():
      
      # Get Node / <elementType> names from CS
      foundNodes = self.resources.getEligibleNodes( elementType )
      if not foundNodes[ 'OK' ]:
        self.log.error( foundNodes[ 'Value' ] )
        continue
      
      # Translate CS result into a list : maps NodeName to SiteName<>NodeName to 
      # avoid duplicates
      # Looong list comprehension, sorry !
      foundNodes = [ '%s<>%s' % ( key, item ) for key, subDict in foundNodes[ 'Value' ].items() 
                     for subList in subDict.values() for item in subList ]
             
      # Synchronize with the DB       
      resSync = self.__dbSync( 'Node', elementType, foundNodes )
      if not resSync[ 'OK' ]:
        self.log.error( 'Error synchronizing %s %s' % ( 'Node', elementType ) )
        self.log.error( resSync[ 'Message' ] )
      else: 
        nodes[ elementType ] = resSync[ 'Value' ] 
  
    return S_OK( nodes )

  #.............................................................................
  # DB sync actions
  
  def __dbSync( self, elementFamily, elementType, elementsCS ):
    """
    Method synchronizing CS and DB. Compares <elementsCS> with <elementsDB>
    given the elementFamily and elementType ( e.g. Resource / Computing ).
    If there are missing elements in the DB, are inserted. If are missing elements
    in the CS, are deleted from the DB. Note that the logs from the RSS DB
    are kept ! ( just in case ).
    
    :Parameters:
      **elementFamily** - str
        any of the valid element families : Site, Resource, Node
      **elementType** - str
        any of the valid element types for <elementFamily>
      **elementsCS** - list
        list with the elements for <elementFamily>/<elementType> found in the CS  
    
    :return: S_OK( { 'added' : [], 'deleted' : [] } ) | S_ERROR
    """ 
    
    # deleted, added default response
    syncRes = { 
                'deleted' : [],
                'added'   : [],
              }
    
    # Gets <elementFamily>/<elementType> elements from DB
    elementsDB = self.rStatus.selectStatusElement( elementFamily, 'Status', 
                                                   elementType = elementType,
                                                   meta = { 'columns' : [ 'name' ] } )
    if not elementsDB[ 'OK' ]:
      return elementsDB
    elementsDB = [ elementDB[ 0 ] for elementDB in elementsDB[ 'Value' ] ]      
    
    # Elements in DB but not in CS -> to be deleted
    toBeDeleted = list( set( elementsDB ).difference( set( elementsCS ) ) )
    if toBeDeleted:
      resDelete = self.__dbDelete( elementFamily, elementType, toBeDeleted )
      if not resDelete[ 'OK' ]:
        return resDelete  
      else:
        syncRes[ 'deleted' ] = toBeDeleted
    
    # Elements in CS but not in DB -> to be added
    toBeAdded = list( set( elementsCS ).difference( set( elementsDB ) ) )
    if toBeAdded:
      resInsert = self.__dbInsert( elementFamily, elementType, toBeAdded )
      if not resInsert[ 'OK' ]:
        return resInsert
      else:
        syncRes[ 'added' ] = toBeAdded
           
    return S_OK( syncRes )
  
  def __dbDelete( self, elementFamily, elementType, toBeDeleted ):
    """
    Method that given the elementFamily and elementType, deletes all entries
    in the History and Status tables for the given elements in toBeDeleted ( all
    their status Types ).

    :Parameters:
      **elementFamily** - str
        any of the valid element families : Site, Resource, Node
      **elementType** - str
        any of the valid element types for <elementFamily>, just used for logging
        purposes.
      **toBeDeleted** - list
        list with the elements to be deleted  
    
    :return: S_OK | S_ERROR    
    """
    
    self.log.info( 'Deleting %s %s:' % ( elementFamily, elementType ) )
    self.log.info( toBeDeleted )
    
    return self.rStatus._extermineStatusElement( elementFamily, toBeDeleted )
  
  def __dbInsert( self, elementFamily, elementType, toBeAdded ):  
    """
    Method that given the elementFamily and elementType, adds all elements in
    toBeAdded with their respective statusTypes, obtained from the CS. They 
    are synchronized with status 'Unknown' and reason 'Synchronized'.

    :Parameters:
      **elementFamily** - str
        any of the valid element families : Site, Resource, Node
      **elementType** - str
        any of the valid element types for <elementFamily>
      **toBeDeleted** - list
        list with the elements to be added  
    
    :return: S_OK | S_ERROR    
    """
    
    self.log.info( 'Adding %s %s:' % ( elementFamily, elementType ) )
    self.log.info( toBeAdded )
    
    statusTypes = self.rssConfig.getConfigStatusType( elementType )

    for element in toBeAdded:
      
      for statusType in statusTypes:
  
        resInsert = self.rStatus.addIfNotThereStatusElement( elementFamily, 'Status', 
                                                             name        = element, 
                                                             statusType  = statusType, 
                                                             status      = 'Unknown', 
                                                             elementType = elementType, 
                                                             reason      = 'Synchronized')

        if not resInsert[ 'OK' ]:
          return resInsert
    
    return S_OK()
    
#...............................................................................    
 
#  
#  def _syncUsers( self ):
#    '''
#      Sync Users: compares CS with DB and does the necessary modifications.
#    '''    
#    
#    gLogger.verbose( '-- Synchronizing users --')
#    
#    usersCS = CSHelpers.getRegistryUsers()
#    if not usersCS[ 'OK' ]:
#      return usersCS
#    usersCS = usersCS[ 'Value' ]
#    
#    gLogger.verbose( '%s users found in CS' % len( usersCS ) )
#    
#    usersDB = self.rManagement.selectUserRegistryCache( meta = { 'columns' : [ 'login' ] } ) 
#    if not usersDB[ 'OK' ]:
#      return usersDB    
#    usersDB = [ userDB[0] for userDB in usersDB[ 'Value' ] ]
#    
#    # Users that are in DB but not in CS
#    toBeDeleted = list( set( usersDB ).difference( set( usersCS.keys() ) ) )
#    gLogger.verbose( '%s users to be deleted' % len( toBeDeleted ) )
#    
#    # Delete users
#    # FIXME: probably it is not needed since there is a DatabaseCleanerAgent
#    for userLogin in toBeDeleted:
#      
#      deleteQuery = self.rManagement.deleteUserRegistryCache( login = userLogin )
#      
#      gLogger.verbose( '... %s' % userLogin )
#      if not deleteQuery[ 'OK' ]:
#        return deleteQuery      
#     
#    # AddOrModify Users 
#    for userLogin, userDict in usersCS.items():
#      
#      _name  = userDict[ 'DN' ].split( '=' )[ -1 ]
#      _email = userDict[ 'Email' ]
#      
#      query = self.rManagement.addOrModifyUserRegistryCache( userLogin, _name, _email )
#      gLogger.verbose( '-> %s' % userLogin )
#      if not query[ 'OK' ]:
#        return query     
#  
#    return S_OK()
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  