# $HeadURL:  $
''' ResourceStatusClient

  Client to interact with the ResourceStatusDB.

'''

from DIRAC                                           import gLogger, S_OK, S_ERROR
#from DIRAC.Core.DISET.RPCClient                      import RPCClient                   
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB  import ResourceStatusDB 
from DIRAC.ResourceStatusSystem.Utilities            import RssConfiguration  

__RCSID__ = '$Id:  $'
       
class ResourceStatusClient( object ):
  '''
  The :class:`ResourceStatusClient` class exposes the :mod:`DIRAC.ResourceStatus` 
  API. All functions you need are on this client.
  
  It has the 'direct-db-access' functions, the ones of the type:
   - insert
   - update
   - select
   - delete 
    
  that return parts of the RSSConfiguration stored on the CS, and used everywhere
  on the RSS module. Finally, and probably more interesting, it exposes a set
  of functions, badly called 'boosters'. They are 'home made' functions using the
  basic database functions that are interesting enough to be exposed.  
  
  The client will ALWAYS try to connect to the DB, and in case of failure, to the
  XML-RPC server ( namely :class:`ResourceStatusDB` and :class:`ResourceStatusHancler` ).

  You can use this client on this way

   >>> from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
   >>> rsClient = ResourceStatusClient()
   
  All functions calling methods exposed on the database or on the booster are 
  making use of some syntactic sugar, in this case a decorator that simplifies
  the client considerably.    
  '''

  def __init__( self , serviceIn = None ):
    '''
      The client tries to connect to :class:ResourceStatusDB by default. If it 
      fails, then tries to connect to the Service :class:ResourceStatusHandler.
    '''
    
    if not serviceIn:
      self.gate = ResourceStatusDB()
      # FIXME: commented out duriing development           
      # self.gate = RPCClient( "ResourceStatus/ResourceStatus" )
    else:
      self.gate = serviceIn 

    self.validElements = RssConfiguration.getValidElements()

  ################################################################################
  # Element status methods - enjoy ! 
  
  def insertStatusElement( self, element, tableType, name, statusType, status, 
                           elementType, reason, dateEffective, lastCheckTime, 
                           tokenOwner, tokenExpiration, meta = None ): 
    '''
    Inserts on <element><tableType> a new row with the arguments given.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]  
      **name** - `string`
        name of the individual of class element  
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **elementType** - `string`
        column to distinguish between the diferent elements in the same element
        table.  
      **reason** - `string`
        decision that triggered the assigned status
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership  
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'insert', locals() )
  def updateStatusElement( self, element, tableType, name, statusType, status, 
                           elementType, reason, dateEffective, lastCheckTime, 
                           tokenOwner, tokenExpiration, meta = None ):
    '''
    Updates <element><tableType> with the parameters given. 
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]          
      **name** - `string`
        name of the individual of class element  
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **elementType** - `string`
        column to distinguish between the diferent elements in the same element
        table.  
      **reason** - `string`
        decision that triggered the assigned status
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership  
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'update', locals() )
  def selectStatusElement( self, element, tableType, name = None, statusType = None, 
                           status = None, elementType = None, reason = None, 
                           dateEffective = None, lastCheckTime = None, 
                           tokenOwner = None, tokenExpiration = None, meta = None ):
    '''
    Gets from <element><tableType> all rows that match the parameters given.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]          
      **name** - `[, string, list]`
        name of the individual of class element  
      **statusType** - `[, string, list]`
        it has to be a valid status type for the element class
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **elementType** - `[, string, list]`
        column to distinguish between the diferent elements in the same element
        table.  
      **reason** - `[, string, list]`
        decision that triggered the assigned status
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `[, string, list]`
        token assigned to the site & status type
      **tokenExpiration** - `[, datetime, list]`
        time-stamp setting validity of token ownership  
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'select', locals() )
  def deleteStatusElement( self, element, tableType, name = None, statusType = None, 
                           status = None, elementType = None, reason = None, 
                           dateEffective = None, lastCheckTime = None, 
                           tokenOwner = None, tokenExpiration = None, meta = None ):
    '''
    Deletes from <element><tableType> all rows that match the parameters given.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]          
      **name** - `[, string, list]`
        name of the individual of class element  
      **statusType** - `[, string, list]`
        it has to be a valid status type for the element class
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **elementType** - `[, string, list]`
        column to distinguish between the diferent elements in the same element
        table.  
      **reason** - `[, string, list]`
        decision that triggered the assigned status
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `[, string, list]`
        token assigned to the site & status type
      **tokenExpiration** - `[, datetime, list]`
        time-stamp setting validity of token ownership  
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'delete', locals() )
  def addOrModifyStatusElement( self, element, tableType, name = None, 
                                statusType = None, status = None, 
                                elementType = None, reason = None, 
                                dateEffective = None, lastCheckTime = None, 
                                tokenOwner = None, tokenExpiration = None, 
                                meta = None ):
    '''
    Adds or updates-if-duplicated from <element><tableType> and also adds a log 
    if flag is active.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]          
      **name** - `string`
        name of the individual of class element  
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **elementType** - `string`
        column to distinguish between the diferent elements in the same element
        table.  
      **reason** - `string`
        decision that triggered the assigned status
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership  
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', locals() )
  def modifyStatusElement( self, element, tableType, name = None, statusType = None, 
                           status = None, elementType = None, reason = None, 
                           dateEffective = None, lastCheckTime = None, tokenOwner = None, 
                           tokenExpiration = None, meta = None ):
    '''
    Updates from <element><tableType> and also adds a log if flag is active. 
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]          
      **name** - `string`
        name of the individual of class element  
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **elementType** - `string`
        column to distinguish between the diferent elements in the same element
        table.  
      **reason** - `string`
        decision that triggered the assigned status
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership  
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'modify', locals() )  
  def addIfNotThereStatusElement( self, element, tableType, name = None, 
                                  statusType = None, status = None, 
                                  elementType = None, reason = None, 
                                  dateEffective = None, lastCheckTime = None, 
                                  tokenOwner = None, tokenExpiration = None, 
                                  meta = None ):
    '''
    Adds if-not-duplicated from <element><tableType> and also adds a log if flag 
    is active. 
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]          
      **name** - `string`
        name of the individual of class element  
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **elementType** - `string`
        column to distinguish between the diferent elements in the same element
        table.  
      **reason** - `string`
        decision that triggered the assigned status
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership  
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addIfNotThere', locals() )

  ##############################################################################
  # Protected methods - Use carefully !!

  def _extermineStatusElement( self, element, name, keepLogs = True ):
    '''
    Deletes from <element>Status,
                 <element>History              
                 <element>Log  
     all rows with `elementName`. It removes all the entries, logs, etc..
    Use with common sense !
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElements ), any of the defaults: \ 
          `Site` | `Resource` | `Node`
      **name** - `[, string, list]`
        name of the individual of class element  
      **keepLogs** - `bool`
        if active, logs are kept in the database  
    
    :return: S_OK() || S_ERROR()
    '''
    return self.__extermineStatusElement( element, name, keepLogs )
  
  def _query( self, queryType, parameters ):
    '''
    It is a simple helper, this way inheriting classes can use it.
    '''
    return self.__query( queryType, parameters )
  
  ##############################################################################
  # Private methods - where magic happens ;)

  def __query( self, queryType, parameters ):
    '''
      This method is a rather important one. It will format the input for the DB
      queries, instead of doing it on a decorator. Two dictionaries must be passed
      to the DB. First one contains 'columnName' : value pairs, being the key
      lower camel case. The second one must have, at lease, a key named 'table'
      with the right table name. 
    '''
    # Functions we can call, just a light safety measure.
    _gateFunctions = [ 'insert', 'update', 'select', 'delete', 'addOrModify', 'modify', 'addIfNotThere' ] 
    if not queryType in _gateFunctions:
      return S_ERROR( '"%s" is not a proper gate call' % queryType )
    
    gateFunction = getattr( self.gate, queryType )
    
    # If meta is None, we set it to {}
    meta = ( True and parameters.pop( 'meta' ) ) or {}
    # Remove self, added by locals()
    del parameters[ 'self' ]     
        
    # This is an special case with the Element tables.
    #if tableName.startswith( 'Element' ):
    element   = parameters.pop( 'element' )
    if not element in self.validElements:
      gLogger.debug( '"%s" is not a valid element like %s' % ( element, self.validElements ) )
      return S_ERROR( '"%s" is not a valid element like %s' % ( element, self.validElements ) )
    
    tableType = parameters.pop( 'tableType' )
    #tableName = tableName.replace( 'Element', element )
    tableName = '%s%s' % ( element, tableType )
          
    meta[ 'table' ] = tableName
    
    gLogger.debug( 'Calling %s, with \n params %s \n meta %s' % ( queryType, parameters, meta ) )  
    userRes = gateFunction( parameters, meta )
    
    return userRes    

  def __extermineStatusElement( self, element, name, keepLogs ):
    '''
      This method iterates over the three ( or four ) table types - depending
      on the value of keepLogs - deleting all matches of `name`.
    '''
  
    tableTypes = [ 'Status', 'History' ]
    if keepLogs == False:
      tableTypes.append( 'Log' )
    
    for table in tableTypes:
      
      deleteQuery = self.deleteStatusElement( element, table, name = name )
      if not deleteQuery[ 'OK' ]:
        return deleteQuery

    return S_OK()
        
#################################################################################
## EXTENDED FUNCTIONS

#  def getServiceStats( self, siteName, statusType = None ):
#    '''
#    Computes simple statistics of `Active`, `Bad`, `Probing` and `Banned` 
#    Services of a Site;
#    
#    :Parameters:
#      **siteName** - `string`
#        name of the site
#      **statusType** - `[, string, list]`
#        it has to be a valid status type for the 'Site' granularity
#        
#    :return: S_OK() || S_ERROR()
#    '''
#    presentDict = { 'siteName' : siteName }
#    if statusType is not None:
##      self.__validateElementStatusTypes( 'Service', statusType )
#      presentDict[ 'statusType' ] = statusType
#    
#    kwargs   = { 'meta' : { 'columns' : [ 'Status'], 
#                            'count' : True, 
#                            'group' : 'Status' } }
#    presentDict.update( kwargs )
#    sqlQuery = self._getElement( 'ServicePresent', presentDict )
#    return self.__getStats( sqlQuery )
#
#  def getResourceStats( self, element, name, statusType = None ):
#    '''
#    Computes simple statistics of `Active`, `Bad`, `Probing` and `Banned` 
#    Resources of a Site or a Service;
#    
#    :Parameters:
#      **element** - `string`
#        it has to be either `Site` or `Service`
#      **name** - `string`
#        name of the individual of element class
#      **statusType** - `[, string, list]`
#        it has to be a valid status type for the element class
#        
#    :return: S_OK() || S_ERROR()
#    '''
#    # VALIDATION ??
#    presentDict = {}
#
#    if statusType is not None:
##      self.rsVal.validateElementStatusTypes( 'Service', statusType )
#      presentDict[ 'statusType'] = statusType    
#
#    rDict = { 'serviceType'  : None, 
#              'siteName'     : None, 
#              'gridSiteName' : None
#            }
#
#    if element == 'Site':
#      rDict[ 'siteName' ] = name
#
#    elif element == 'Service':
#
#      serviceType, siteName = name.split( '@' )
#      rDict[ 'serviceType' ] = serviceType
#      
#      if serviceType == 'Computing':
#        rDict[ 'siteName' ] = siteName
#        
#      else:
#        kwargs = { 'meta' : {'columns' : [ 'GridSiteName' ] }, 'siteName' : siteName }
#        gridSiteName = [ gs[0] for gs in \
#                         self._getElement( 'Site', kwargs )[ 'Value' ] ]
#        
#        rDict[ 'gridSiteName' ] = gridSiteName
#        
#    else:
#      message = '%s is non accepted element. Only Site or Service' % element
#      return S_ERROR( message )
#
#    resourceNames = [ re[0] for re in \
#                          self._getElement( 'Resource', rDict )[ 'Value' ] ]
#    
#    kwargs   = { 'meta' : { 'columns' : [ 'Status'], 'count' : True, 'group' : 'Status' } }
#    presentDict[ 'resourceName' ] = resourceNames
#    presentDict.update( kwargs )
#    
#    sqlQuery = self._getElement( 'ResourcePresent', presentDict )
#    return self.__getStats( sqlQuery )
# 
#  def getStorageElementStats( self, element, name, statusType = None ):
#    '''
#    Computes simple statistics of `Active`, `Bad`, `Probing` and `Banned` 
#    StorageElements of a Site or a Resource;
#    
#    :Parameters:
#      **element** - `string`
#        it has to be either `Site` or `Resource`
#      **name** - `string`
#        name of the individual of element class
#      **statusType** - `[, string, list]`
#        it has to be a valid status type for the element class
#        
#    :return: S_OK() || S_ERROR()
#    '''
#    # VALIDATION ??
#    presentDict = {}
#
#    if statusType is not None:
##      self.rsVal.validateElementStatusTypes( 'StorageElement', statusType )
#      presentDict[ 'statusType'] = statusType
#
#    rDict = { 'resourceName' : None,
#              'gridSiteName' : None }
#    
#    if element == 'Site':
#
#      kwargs = { 'meta' : { 'columns' : [ 'GridSiteName' ] }, 'siteName' : name  }
#      gridSiteNames = [ gs[0] for gs in \
#                             self._getElement( 'Site', kwargs )[ 'Value' ] ]
#      rDict[ 'gridSiteName' ] = gridSiteNames
#
#    elif element == 'Resource':
#
#      rDict[ 'resourceName' ] = name
#
#    else:
#      message = '%s is non accepted element. Only Site or Resource' % element
#      return S_ERROR( message )
#
#    storageElementNames = [ se[0] for se in \
#                    self._getElement( 'StorageElement', rDict )[ 'Value' ] ]
#
#    kwargs   = { 'meta' : { 'columns' : [ 'Status'], 'count' : True, 'group' : 'Status' } }
#    presentDict[ 'storageElementName' ] = storageElementNames
#    presentDict.update( kwargs )
#    
#    sqlQuery = self._getElement( 'StorageElementPresent', presentDict )
#    return self.__getStats( sqlQuery )  
#  
#  def getGeneralName( self, from_element, name, to_element ):
#    '''
#    Get name of a individual of granularity `from_g`, to the name of the
#    individual with granularity `to_g`
#
#    For a StorageElement, get either the Site, Service or the Resource name.
#    For a Resource, get either the Site name or Service name.
#    For a Service name, get the Site name
#
#    :Parameters:
#      **from_element** - `string`
#        granularity of the element named name
#      **name** - `string`
#        name of the element
#      **to_element** - `string`
#        granularity of the desired name
#        
#    :return: S_OK() || S_ERROR()        
#    '''
##    self.rsVal.validateElement( from_element )
##    self.rsVal.validateElement( to_element )
#
#    if from_element == 'Service':
#      kwargs = { 'meta' : { 'columns' : [ 'SiteName' ] }, 'serviceName' : name }
#      resQuery = self._getElement( 'Service', kwargs ) 
#
#    elif from_element == 'Resource':
#      kwargs = { 'meta' : { 'columns' : [ 'ServiceType' ] }, 'resourceName' : name }
#      resQuery = self._getElement( 'Resource', kwargs )    
#      serviceType = resQuery[ 'Value' ][ 0 ][ 0 ]
#
#      if serviceType == 'Computing':
#        kwargs = { 'meta' : { 'columns' : [ 'SiteName' ] }, 'resourceName' : name }
#        resQuery = self._getElement( 'Resource', kwargs )  
#      else:
#        kwargs = { 'meta' : { 'columns' : [ 'GridSiteName' ] }, 'resourceName' : name }    
#        gridSiteNames = self._getElement( 'Resource', kwargs )
#        kwargs = { 
#                   'meta' : { 'columns'      : [ 'SiteName' ] }, 
#                   'gridSiteName' : list( gridSiteNames[ 'Value' ] ) 
#                 }  
#        resQuery = self._getElement( 'Site', kwargs )
#        
#    elif from_element == 'StorageElement':
#
#      if to_element == 'Resource':
#        kwargs = { 'meta' : { 'columns' : [ 'ResourceName' ] }, 'storageElementName' : name }   
#        resQuery = self._getElement( 'StorageElement', kwargs )
#      else:
#        kwargs = { 'meta' : { 'columns' : [ 'GridSiteName' ] }, 'storageElementName' : name }  
#        gridSiteNames = self._getElement( 'StorageElement', kwargs )
#        kwargs = { 
#                   'meta' : { 'columns'      : [ 'SiteName' ] }, 
#                   'gridSiteName' : list( gridSiteNames[ 'Value' ] ) 
#                 }
#        resQuery = self._getElement( 'Site', kwargs )
#
#        if to_element == 'Service':
#          serviceType = 'Storage'
#
#    else:
#      return S_ERROR( 'Expected from_element either Service, Resource or StorageElement' )
#
#    if not resQuery[ 'Value' ]:
#      return resQuery
#
#    newNames = [ x[0] for x in resQuery[ 'Value' ] ]
#
#    if to_element == 'Service':
#      return S_OK( [ serviceType + '@' + x for x in newNames ] )
#    else:
#      return S_OK( newNames )
#
#  def getGridSiteName( self, granularity, name ):
#    '''
#    Get grid site name for the given individual.
#    
#    :Parameters:
#      **granularity** - `string`
#        it has to be a valid element ( ValidElements ), any of the defaults: `Site` \
#        | `Service` | `Resource` | `StorageElement`
#      **name** - `string`
#        name of the element
#    
#    :return: S_OK() | S_ERROR()      
#    '''
##    self.rsVal.validateElement( granularity )
#
#    elementName = '%sName' % ( granularity[0].lower() + granularity[1:] ) 
#
#    rDict  = { elementName : name }
#    kwargs = { 'meta' : { 'columns' : [ 'GridSiteName' ] } }
#    
#    kwargs.update( rDict )
#    return self._getElement( granularity, kwargs )
#
#  def getTokens( self, granularity, name = None, tokenExpiration = None, 
#                 statusType = None, **kwargs ):
#    '''
#    Get tokens for the given parameters. If `tokenExpiration` given, will select
#    the ones with tokenExpiration older than the given one.
#
#    :Parameters:
#      **granularity** - `string`
#        it has to be a valid element ( ValidElements ), any of the defaults: `Site` \
#        | `Service` | `Resource` | `StorageElement`
#      **name** - `[, string, list]`
#        name of the element  
#      **tokenExpiration** - `[, datetime]`
#        time-stamp with the token expiration time
#      **statusType** - `[, string, list]`
#        it has to be a valid status type for the granularity class
#    
#    :return: S_OK || S_ERROR
#    '''
##    self.rsVal.validateElement( granularity )  
#
#    rDict = { 'element' : granularity }
#    if name is not None:
#      rDict[ 'elementName' ] = name
#      
#    if statusType is not None:
##      self.rsVal.validateElementStatusTypes( granularity, statusType )
#      rDict[ 'statusType' ] = statusType
#
#    kw = { 'meta' : {}}
#    kw[ 'meta' ][ 'columns' ] = kwargs.pop( 'columns', None )
#    if tokenExpiration is not None:
#      kw[ 'meta' ][ 'minor' ]   = { 'TokenExpiration' : tokenExpiration }
#    kw.update( rDict )
#     
#    return self._getElement( 'ElementStatus', kw ) 
#
#  def setToken( self, granularity, name, statusType, reason, tokenOwner, 
#                tokenExpiration ):
#    '''
#    Updates <granularity>Status with the parameters given, using `name` and 
#    `statusType` to select the row.
#        
#    :Parameters:
#      **granularity** - `string`
#        it has to be a valid element ( ValidElements ), any of the defaults: `Site` \
#        | `Service` | `Resource` | `StorageElement`
#      **name** - `string`
#        name of the element  
#      **statusType** - `string`
#        it has to be a valid status type for the granularity class
#      **reason** - `string`
#        decision that triggered the assigned status
#      **tokenOwner** - `string`
#        token assigned to the element & status type    
#      **tokenExpiration** - `datetime`
#        time-stamp with the token expiration time
#    
#    :return: S_OK || S_ERROR
#    '''
##    self.rsVal.validateElement( granularity )
##    self.rsVal.validateElementStatusTypes( granularity, statusType )
#    
#    rDict = { 
#             'elementName'         : name,
#             'statusType'          : statusType,
#             'reason'              : reason,
#             'tokenOwner'          : tokenOwner,
#             'tokenExpiration'     : tokenExpiration
#             }
#  
#    return self.modifyElementStatus( granularity, **rDict )
#  
#  def setReason( self, granularity, name, statusType, reason ):
#    '''
#    Updates <granularity>Status with the parameters given, using `name` and 
#    `statusType` to select the row.
#    
#    :Parameters:
#      **granularity** - `string`
#        it has to be a valid element ( ValidElements ), any of the defaults: `Site` \
#        | `Service` | `Resource` | `StorageElement`
#      **name** - `string`
#        name of the element  
#      **statusType** - `string`
#        it has to be a valid status type for the granularity class
#      **reason** - `string`
#        decision that triggered the assigned status
#    
#    :return: S_OK || S_ERROR
#    '''           
##    self.rsVal.validateElement( granularity )
#    
#    rDict = {        
#             'elementName': name,
#             'statusType' : statusType,
#             'reason'     : reason,
#             }
#     
#    return self.modifyElementStatus( granularity, **rDict ) 
#
#  def setDateEnd( self, granularity, name, statusType, dateEffective ):
#    '''
#    Updates <granularity>Status with the parameters given, using `name` and 
#    `statusType` to select the row.
#    
#    :Parameters:
#      **granularity** - `string`
#        it has to be a valid element ( ValidElements ), any of the defaults: `Site` \
#        | `Service` | `Resource` | `StorageElement`
#      **name** - `string`
#        name of the element  
#      **statusType** - `string`
#        it has to be a valid status type for the granularity class
#      **dateEffective** - `datetime`
#        time-stamp from which the status & status type are effective
#    
#    :return: S_OK || S_ERROR
#    '''    
#    #self.rsVal.validateElement( granularity )   
#    rDict = { 
#             'elementName'   : name,
#             'statusType'    : statusType,
#             'dateEffective' : dateEffective,
#             }
#    
#    return self.modifyElementStatus( granularity, **rDict )
#    
#  def whatIs( self, name ):
#    '''
#    Finds which is the granularity of the given name.
#    
#    :Parameters:
#      **name** - `string`
#        name of the element  
#    
#    :return: S_OK || S_ERROR
#    '''
#    
#    validElements = RssConfiguration.getValidElements()
#    
#    for g in validElements:
#
#      elementName = '%sName' % (g[0].lower() + g[1:])
#
#      rDict  = { elementName : name }
#      resQuery = self._getElement( g, rDict )
#           
#      if not resQuery[ 'Value' ]:
#        continue
#      else:
#        return S_OK( g )
#
#    return S_OK( 'Unknown' )  
#
#  def getStuffToCheck( self, granularity, checkFrequency, **kwargs ):
#    '''
#    Gets from the <granularity>Present view all elements that match the 
#    chechFrequency values ( lastCheckTime(j) < now - checkFrequencyTime(j) ). 
#    
#    :Parameters:
#      **granularity** - `string`
#        it has to be a valid element ( ValidElements ), any of the defaults: `Site` \
#        | `Service` | `Resource` | `StorageElement`
#      **checkFrequency** - `dict`
#        used to set chech frequency depending on state and type
#      **\*\*kwargs** - `[, dict]`
#        meta-data for the MySQL query. 
#    
#    :return: S_OK() || S_ERROR()    
#    '''
#
#    toCheck = {}
#
#    now = datetime.utcnow().replace( microsecond = 0 )
#
#    for freqName, freq in checkFrequency.items():
#      toCheck[ freqName ] = ( now - timedelta( minutes = freq ) )#.isoformat(' ')
#
#    if not kwargs.has_key( 'meta' ):
#      kwargs[ 'meta' ] = {}
#    if not kwargs['meta'].has_key( 'sort' ): 
#      kwargs[ 'meta' ][ 'sort' ] = 'LastCheckTime'
#
#    kwargs[ 'meta' ][ 'or' ] = []
#        
#    for k,v in toCheck.items():
#          
#      siteType, status = k.replace( '_CHECK_FREQUENCY', '' ).split( '_' )
#      status = status[0] + status[1:].lower()
#        
#      dict = { 'Status' : status, 'SiteType' : siteType }
#      kw   = { 'minor' : { 'LastCheckTime' : v } }
#                
#      orDict = { 'dict': dict, 'kwargs' : kw }          
#                
#      kwargs[ 'meta' ][ 'or' ].append( orDict )          
#
#    return self._getElement( '%sPresent' % granularity, kwargs )
#
#  def getTopology( self ):
#    '''
#    Gets all elements in the database and returns a node tree with all the
#    relations between them.
#    
#    :Parameters: `None`
#    
#    :return: S_OK() || S_ERROR()
#    '''     
#    
#    tree = Node( 'DIRAC', 'Topology', 'Site', '' )
#    
#    sites = self.getSite()
#    if not sites[ 'OK' ]:
#      return sites
#    
#    for site in sites['Value']:
#      s = Node( site[0], 'Site', 'Service', 'Topology' )
#      s.setAttr( 'SiteType',     site[1] )
#      s.setAttr( 'GridSiteType', site[2] )
#      tree.setChildren( s )
#      
#    services = self.getService()  
#    if not services[ 'OK' ]:
#      return services
#    
#    for service in services['Value']:
#      
#      siteName = service[2].replace( '.', '_' ).replace( '-', '_' )
#      
#      site = tree._levels[ 'Site' ][ siteName ]
#      se = Node( service[0], 'Service', 'Resource', 'Site' )  
#      se.setAttr( 'ServiceType', service[ 1 ])
#      site.setChildren( se )
#
#    resources = self.getResource()
#    if not resources[ 'OK' ]:
#      return resources
#    
#    for resource in resources['Value']:
#      
#      resourceType = resource[ 1 ]
#      serviceType  = resource[ 2 ]
#      
#      if resource[ 3 ] != 'NULL':
#        siteName = resource[ 3 ].replace( '.', '_' ).replace( '-', '_' )
#        serviceName = [ '%s_%s' % ( serviceType, siteName ) ]
#      else:
#        serviceName = [ '%s_%s' % ( serviceType, site.name ) for site in tree._levels[ 'Site' ].values() if site.attr[ 'GridSiteType' ] == resource[ 4 ] ]  
#      
#      re = Node( resource[0], 'Resource', 'StorageElement', 'Service' )
#      re.setAttr( 'ResourceType', resourceType )
#      re.setAttr( 'ServiceType', serviceType )
#      
#      for sName in serviceName:
#        service = tree._levels[ 'Service' ][ sName ]
#        service.setChildren( re )
#      
#    storageElements = self.getStorageElement()
#    if not storageElements[ 'OK' ]:
#      return storageElements
#    
#    for storageElement in storageElements[ 'Value' ]:
#      
#      resourceName = storageElement[ 1 ].replace( '.', '_' ).replace( '-', '_' )
#      gridSiteName = storageElement[ 2 ]
#      
#      se = Node( storageElement[ 0 ], 'StorageElement', '', 'Resource' )
#      se.setAttr( 'ResourceName', resourceName )
#      se.setAttr( 'GridSiteName', gridSiteName )
#      
#      resource = tree._levels[ 'Resource' ][ resourceName ]
#      resource.setChildren( se )
#      
#    return S_OK( tree )  
#
#  def getSESitesList( self ):
#       
#    kwargs = { 'statusType' : 'Read', 'meta' : { 'columns' : 'GridSiteName' } }
#    elements = self._getElement( 'StorageElementPresent', kwargs )
#
#    from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getDIRACSiteName
#    res = set( [] )
#    if not elements[ 'OK' ]:
#      return S_OK( [] )
#    
#    elements = elements[ 'Value' ]
#    for gSite in elements:
#      
#      dSite = getDIRACSiteName( gSite[ 0 ] ).setdefault( 'Value', [] )
#      for ds in dSite:
#        res.add( ds )
#    
#    res = list( res )
#    return S_OK( res )        
#
#  def getMonitoredStatus( self, granularity, name ):
#    '''
#    Gets from <granularity>Present the present status of name.
#    
#    :Parameters:
#      **granularity** - `string`
#        it has to be a valid element ( ValidElements ), any of the defaults: `Site` \
#        | `Service` | `Resource` | `StorageElement`
#      **name** - `string`
#        name of the element
#     
#    :return: S_OK() || S_ERROR()      
#    '''   
#    elementName = '%sName' % ( granularity[0].lower() + granularity[1:] ) 
#    kwargs = { elementName : name, 'meta' : { 'columns' : [ 'Status' ] }}
#    
#    return self._getElement( '%sPresent' % granularity, kwargs )
#
#  def getMonitoredsStatusWeb( self, granularity, selectDict, startItem, 
#                              maxItems ):
#    '''
#    Get present sites status list, for the web.
#    
#    :Parameters:
#      **granularity** - `string`
#        it has to be a valid element ( ValidElements ), any of the defaults: `Site` \
#        | `Service` | `Resource` | `StorageElement`
#      **selectDict** - `dict`
#        meta-data for the MySQL query
#      **startItem** - `integer`
#        first item index of the slice returned
#      **maxItems** - `integer`
#        length of the slice returned
#        
#    :return: S_OK() || S_ERROR()          
#    '''
##    self.rsVal.validateElement( granularity )
#
#    if granularity == 'Site':
#      paramNames = [ 'SiteName', 'Tier', 'GridType', 'Country', 'StatusType',
#                     'Status', 'DateEffective', 'FormerStatus', 'Reason' ]
#      paramsList = [ 'SiteName', 'SiteType', 'StatusType','Status', 
#                     'DateEffective', 'FormerStatus', 'Reason' ]
#    elif granularity == 'Service':
#      paramNames = [ 'ServiceName', 'ServiceType', 'Site', 'Country', 
#                     'StatusType','Status', 'DateEffective', 'FormerStatus', 
#                     'Reason' ]
#      paramsList = [ 'ServiceName', 'ServiceType', 'SiteName', 'StatusType',
#                     'Status', 'DateEffective', 'FormerStatus', 'Reason' ]
#    elif granularity == 'Resource':
#      paramNames = [ 'ResourceName', 'ServiceType', 'SiteName', 'ResourceType',
#                     'Country', 'StatusType','Status', 'DateEffective', 
#                     'FormerStatus', 'Reason' ]
#      paramsList = [ 'ResourceName', 'ServiceType', 'SiteName', 'GridSiteName', 
#                     'ResourceType', 'StatusType','Status', 'DateEffective', 
#                     'FormerStatus', 'Reason' ]
#    elif granularity == 'StorageElement':
#      paramNames = [ 'StorageElementName', 'ResourceName', 'SiteName', 
#                     'Country', 'StatusType','Status', 'DateEffective', 
#                     'FormerStatus', 'Reason' ]
#      paramsList = [ 'StorageElementName', 'ResourceName', 'GridSiteName', 
#                     'StatusType','Status', 'DateEffective', 'FormerStatus', 
#                     'Reason' ]
#    else:
#      return S_ERROR( '%s is not a valid granularity' % granularity )
#    
#
#    records                = []
#
#    rDict = { 'SiteName'                    : None,
#              'ServiceName'                 : None,
#              'ResourceName'                : None,
#              'StorageElementName'          : None,
#              'StatusType'                  : None,
#              'Status'                      : None,
#              'SiteType'                    : None,
#              'ServiceType'                 : None,
#              'ResourceType'                : None,
##              'Countries'                   : None,
#              'ExpandSiteHistory'           : None,
#              'ExpandServiceHistory'        : None,
#              'ExpandResourceHistory'       : None,
#              'ExpandStorageElementHistory' : None }
#
#
#    for k in rDict.keys():
#      if selectDict.has_key( k ):
#        rDict[ k ] = selectDict[ k ]
#        if not isinstance( rDict[ k ], list ):
#          rDict[ k ] = [ rDict[ k ] ]
#
#    if selectDict.has_key( 'Expanded%sHistory' % granularity ):
#      paramsList = [ '%sName', 'StatusType', 'Status', 'Reason', 
#                     'DateEffective' ]
#      elements   = rDict[ 'Expanded%sHistory' % granularity ]
#      #hgetter    = getattr( self.rsClient, 'get%ssHhistory' )
#      kwargs     = { '%sName' % granularity : elements, 'columns' : paramsList, 'element' : granularity }  
#      #elementsH  = hgetter( **kwargs )
#      elementsH = self._getElement( 'ElementHistory', kwargs )
#      #elementsH  = self.getMonitoredsHistory( granularity, paramsList = paramsList,
#      #                                        name = elements )
#
#      for elementH in elementsH[ 'Value' ]:
#        record = []
#        record.append( elementH[ 0 ] )  #%sName % granularity
#        record.append( None )           #Tier
#        record.append( None )           #GridType
#        record.append( None )           #Country
#        record.append( elementH[ 1 ] )  #StatusType 
#        record.append( elementH[ 2 ] )  #Status
#        record.append( elementH[ 4 ].isoformat(' ') ) #DateEffective
#        record.append( None )           #FormerStatus
#        record.append( elementH[ 3 ] )  #Reason
#        records.append( record )        
#
#    else:
#      kwargs = { 'meta' : { 'columns' : paramsList }}  
#      if granularity == 'Site':
#        
#        kwargs[ 'siteName' ]   = rDict[ 'SiteName' ]
#        kwargs[ 'statusType' ] = rDict[ 'StatusType' ]
#        kwargs[ 'status' ]     = rDict[ 'Status' ]
#        kwargs[ 'siteType' ]   = rDict[ 'SiteType' ]
#        
#        sitesList = self._getElement( 'SitePresent', kwargs )  
#
#        for site in sitesList[ 'Value' ]:
#          record   = []
#          gridType = ( site[ 0 ] ).split( '.' ).pop(0)
#          country  = ( site[ 0 ] ).split( '.' ).pop()
#
#          record.append( site[ 0 ] ) #SiteName
#          record.append( site[ 1 ] ) #Tier
#          record.append( gridType ) #GridType
#          record.append( country ) #Country
#          record.append( site[ 2 ] ) #StatusType
#          record.append( site[ 3 ] ) #Status
#          record.append( site[ 4 ].isoformat(' ') ) #DateEffective
#          record.append( site[ 5 ] ) #FormerStatus
#          record.append( site[ 6 ] ) #Reason
#          records.append( record )
#
#      elif granularity == 'Service':
#        
#        kwargs[ 'serviceName' ] = rDict[ 'ServiceName' ]
#        kwargs[ 'siteName' ]    = rDict[ 'SiteName' ]
#        kwargs[ 'statusType' ]  = rDict[ 'StatusType' ]
#        kwargs[ 'status' ]      = rDict[ 'Status' ]
#        kwargs[ 'siteType' ]    = rDict[ 'SiteType' ]
#        kwargs[ 'serviceType' ] = rDict[ 'ServiceType' ]
#        
#        servicesList = self._getElement( 'ServicePresent', kwargs )
#
#        for service in servicesList[ 'Value' ]:
#          record  = []
#          country = ( service[ 0 ] ).split( '.' ).pop()
#
#          record.append( service[ 0 ] ) #ServiceName
#          record.append( service[ 1 ] ) #ServiceType
#          record.append( service[ 2 ] ) #Site
#          record.append( country ) #Country
#          record.append( service[ 3 ] ) #StatusType
#          record.append( service[ 4 ] ) #Status
#          record.append( service[ 5 ].isoformat(' ') ) #DateEffective
#          record.append( service[ 6 ] ) #FormerStatus
#          record.append( service[ 7 ] ) #Reason
#          records.append( record )
#
#      elif granularity == 'Resource':
#        if rDict[ 'SiteName' ] == None:
#          kw = { 'meta' : { 'columns' : [ 'SiteName' ] } }
#          #sites_select = self.rsClient.getSitePresent( **kw )
#          sites_select = self._getElement( 'SitePresent', kw )
#          #sites_select = self.getMonitoredsList( 'Site',
#          #                                       paramsList = [ 'SiteName' ] )
#          rDict[ 'SiteName' ] = [ x[ 0 ] for x in sites_select[ 'Value' ] ] 
#          
#        kw = { 'meta' : { 'columns' : [ 'GridSiteName' ] }, 'siteName' : rDict[ 'SiteName'] }
#        
#        #gridSites_select = self.rsClient.getSitePresent( siteName = rDict[ 'SiteName'], **kw )
#        gridSites_select = self._getElement( 'SitePresent', kw )
#        #gridSites_select = self.getMonitoredsList( 'Site',
#        #                                           paramsList = [ 'GridSiteName' ],
#        #                                           siteName = rDict[ 'SiteName' ] )
#        
#        gridSites_select = [ x[ 0 ] for x in gridSites_select[ 'Value' ] ]
#
#        kwargs[ 'resourceName' ] = rDict[ 'ResourceName' ]
#        kwargs[ 'statusType' ]   = rDict[ 'StatusType' ]
#        kwargs[ 'status' ]       = rDict[ 'Status' ]
#        kwargs[ 'siteType' ]     = rDict[ 'SiteType' ]
#        kwargs[ 'resourceType' ] = rDict[ 'ResourceType' ]
#        kwargs[ 'gridSiteName' ] = gridSites_select
#
#        resourcesList = self._getElement( 'ResourcePresent', kwargs )
#
#        for resource in resourcesList[ 'Value' ]:
#          DIRACsite = resource[ 2 ]
#
#          if DIRACsite == 'NULL':
#            GridSiteName = resource[ 3 ]  #self.getGridSiteName(granularity, resource[0])
#            DIRACsites = getDIRACSiteName( GridSiteName )
#            if not DIRACsites[ 'OK' ]:
#              return S_ERROR( 'Error executing getDIRACSiteName' )
#            DIRACsites = DIRACsites[ 'Value' ]
#            DIRACsite_comp = ''
#            for DIRACsite in DIRACsites:
#              if DIRACsite not in rDict[ 'SiteName' ]:#sites_select:
#                continue
#              DIRACsite_comp = DIRACsite + ' ' + DIRACsite_comp
#
#            record  = []
#            country = ( resource[ 0 ] ).split( '.' ).pop()
#
#            record.append( resource[ 0 ] ) #ResourceName
#            record.append( resource[ 1 ] ) #ServiceType
#            record.append( DIRACsite_comp ) #SiteName
#            record.append( resource[ 4 ] ) #ResourceType
#            record.append( country ) #Country
#            record.append( resource[ 5 ] ) #StatusType
#            record.append( resource[ 6 ] ) #Status
#            record.append( resource[ 7 ].isoformat(' ') ) #DateEffective
#            record.append( resource[ 8 ] ) #FormerStatus
#            record.append( resource[ 9 ] ) #Reason
#            records.append( record )
#
#          else:
#            if DIRACsite not in rDict[ 'SiteName' ]: #sites_select:
#              continue
#            record  = []
#            country = ( resource[ 0 ] ).split( '.' ).pop()
#
#            record.append( resource[ 0 ] ) #ResourceName
#            record.append( resource[ 1 ] ) #ServiceType
#            record.append( DIRACsite ) #SiteName
#            record.append( resource[ 4 ] ) #ResourceType
#            record.append( country ) #Country
#            record.append( resource[ 5 ] ) #StatusType
#            record.append( resource[ 6 ] ) #Status
#            record.append( resource[ 7 ].isoformat(' ') ) #DateEffective
#            record.append( resource[ 8 ] ) #FormerStatus
#            record.append( resource[ 9 ] ) #Reason
#            records.append( record )
#
#
#      elif granularity == 'StorageElement':
#        if rDict[ 'SiteName' ] == [] or rDict[ 'SiteName' ] is None:#sites_select == []:
#          kw = { 'meta' : { 'columns' : [ 'SiteName' ] } }
#          #sites_select = self.rsClient.getSitePresent( **kw )
#          sites_select = self._getElement( 'SitePresent', kw )
#          #sites_select = self.getMonitoredsList( 'Site',
#          #                                      paramsList = [ 'SiteName' ] )
#          rDict[ 'SiteName' ] = [ x[ 0 ] for x in sites_select[ 'Value' ] ]
#
#        kw = { 
#               'meta' : { 'columns'  : [ 'GridSiteName' ] }, 
#               'siteName' : rDict[ 'SiteName' ] 
#              }
#        #gridSites_select = self.rsClient.getSitePresent( siteName = rDict[ 'SiteName' ], **kw )
#        gridSites_select = self._getElement( 'SitePresent', kw )
#        #gridSites_select = self.getMonitoredsList( 'Site',
#        #                                           paramsList = [ 'GridSiteName' ],
#        #                                           siteName = rDict[ 'SiteName' ] )
#        gridSites_select = [ x[ 0 ] for x in gridSites_select[ 'Value' ] ]
#
#        kwargs[ 'storageElementName' ] = rDict[ 'StorageElementName' ]
#        kwargs[ 'statusType' ]         = rDict[ 'StatusType' ]
#        kwargs[ 'status' ]             = rDict[ 'Status' ]
#        kwargs[ 'gridSiteName']        = gridSites_select 
#
#        storageElementsList = self._getElement( 'StorageElementPresent', kwargs )
#
#        for storageElement in storageElementsList[ 'Value' ]:
#          DIRACsites = getDIRACSiteName( storageElement[ 2 ] )
#          if not DIRACsites[ 'OK' ]:
#            return S_ERROR( 'Error executing getDIRACSiteName' )
#          DIRACsites = DIRACsites[ 'Value' ]
#          DIRACsite_comp = ''
#          for DIRACsite in DIRACsites:
#            if DIRACsite not in rDict[ 'SiteName' ]:
#              continue
#            DIRACsite_comp = DIRACsite + ' ' + DIRACsite_comp
#          record  = []
#          country = ( storageElement[ 1 ] ).split( '.' ).pop()
#
#          record.append( storageElement[ 0 ] ) #StorageElementName
#          record.append( storageElement[ 1 ] ) #ResourceName
#          record.append( DIRACsite_comp ) #SiteName
#          record.append( country ) #Country
#          record.append( storageElement[ 3 ] ) #StatusType
#          record.append( storageElement[ 4 ] ) #Status
#          record.append( storageElement[ 5 ].isoformat(' ') ) #DateEffective
#          record.append( storageElement[ 6 ] ) #FormerStatus
#          record.append( storageElement[ 7 ] ) #Reason
#          records.append( record )
#
#    finalDict = {}
#    finalDict[ 'TotalRecords' ]   = len( records )
#    finalDict[ 'ParameterNames' ] = paramNames
#
#    # Return all the records if maxItems == 0 or the specified number otherwise
#    if maxItems:
#      finalDict[ 'Records' ] = records[ startItem:startItem+maxItems ]
#    else:
#      finalDict[ 'Records' ] = records
#
#    finalDict[ 'Extras' ] = None
#
#    return S_OK( finalDict )  
#  
#################################################################################
#
#################################################################################
## addOrModify PRIVATE FUNCTIONS
#
#
#  def __addOrModifyElement( self, element, kwargs ):
#
#    del kwargs[ 'self' ]
#       
#    kwargs[ 'meta' ] = { 'onlyUniqueKeys' : True }
#    sqlQuery = self._getElement( element, kwargs )
#    if not sqlQuery[ 'OK' ]:
#      return sqlQuery
#         
#    del kwargs[ 'meta' ] 
#       
#    if sqlQuery[ 'Value' ]:      
#      return self._updateElement( element, kwargs )
#    else: 
#      sqlQuery = self._insertElement( element, kwargs )
#      if sqlQuery[ 'OK' ]:       
#        res = self.__setElementInitStatus( element, **kwargs )
#        if not res[ 'OK' ]:
#          return res
#        
#      return sqlQuery  
#
#  def __setElementInitStatus( self, element, **kwargs ):
#    
#    defaultStatus  = 'Banned'
#    defaultReasons = [ 'Added to DB', 'Init' ]
#
#    # This three lines make not much sense, but sometimes statusToSet is '',
#    # and we need it as a list to work properly
#    validStatusTypes = RssConfiguration.getValidStatusTypes()
#    statusToSet = validStatusTypes[ element ][ 'StatusType' ]
#    
#    elementName = '%sName' % ( element[0].lower() + element[1:] )
#    
#    if not isinstance( statusToSet, list ):
#      statusToSet = [ statusToSet ]
#    
#    for statusType in statusToSet:
#
#      # Trick to populate ElementHistory table with one entry. This allows
#      # us to use PresentElement views ( otherwise they do not work ).
#      for defaultReason in defaultReasons:
#
#        rDict = {}
#        rDict[ 'elementName' ] = kwargs[ elementName ]
#        rDict[ 'statusType' ]  = statusType
#        rDict[ 'status']       = defaultStatus
#        rDict[ 'reason' ]      = defaultReason
#        
#        sqlQuery = self.__addOrModifyElementStatus( element, rDict  )        
#                
#        if not sqlQuery[ 'OK' ]:
#          return sqlQuery
#        
#    return S_OK()     
#
#  def __addOrModifyElementStatus( self, element, rDict ):
#
#    rDict.update( self.__setStatusDefaults())
#    kwargs = { 
#               'element'        : element,
#               'elementName'    : rDict[ 'elementName' ], 
#               'statusType'     : rDict[ 'statusType' ], 
#               'meta'           : { 'onlyUniqueKeys' : True } 
#             }
#
#    sqlQuery = self._getElement( 'ElementStatus', kwargs )
#    if not sqlQuery[ 'OK' ]:
#      return sqlQuery
#
#    rDict[ 'element' ] = element
#
#    if not sqlQuery[ 'Value' ]:
#      return self._insertElement( 'ElementStatus', rDict )
#    
#    updateSQLQuery = self._updateElement( 'ElementStatus', rDict )
#    if not updateSQLQuery[ 'OK' ]:
#      return updateSQLQuery 
#
#    sqlQ      = list( sqlQuery[ 'Value' ][ 0 ] )[1:]
#        
#    sqlDict = {}
#    sqlDict[ 'elementName' ]     = sqlQ[ 0 ]
#    sqlDict[ 'statusType' ]      = sqlQ[ 1 ]
#    sqlDict[ 'status']           = sqlQ[ 2 ]
#    sqlDict[ 'reason' ]          = sqlQ[ 3 ]
#    sqlDict[ 'dateCreated' ]     = sqlQ[ 4 ]
#    sqlDict[ 'dateEffective' ]   = sqlQ[ 5 ]   
#    sqlDict[ 'dateEnd' ]         = rDict[ 'dateEffective' ]
#    sqlDict[ 'lastCheckTime' ]   = sqlQ[ 7 ]
#    sqlDict[ 'tokenOwner' ]      = sqlQ[ 8 ]
#    sqlDict[ 'tokenExpiration' ] = sqlQ[ 9 ]   
#           
#    sqlDict[ 'element' ] = element       
#           
#    return self._insertElement( 'ElementHistory', sqlDict )    
#
#  def __setStatusDefaults( self ):
#     
#    now    = datetime.utcnow().replace( microsecond = 0 )
#    never  = datetime( 9999, 12, 31, 23, 59, 59 ).replace( microsecond = 0 )
#
#    iDict = {}
#    iDict[ 'dateCreated'] = now
#    iDict[ 'dateEffective'] = now
#    iDict[ 'dateEnd'] = never
#    iDict[ 'lastCheckTime'] = now
#    iDict[ 'tokenOwner'] = 'RS_SVC'
#    iDict[ 'tokenExpiration'] = never
#
#    return iDict
#
#################################################################################
## Modify PRIVATE FUNCTIONS
#  
#  def __modifyElementStatus( self,kwargs ):
#      
#    del kwargs[ 'self' ]  
#    
#    kwargs[ 'meta' ] = { 'onlyUniqueKeys' : True }
#    sqlQuery         = self._getElement( 'ElementStatus', kwargs )
#
#    del kwargs[ 'meta' ]
#
#    if not sqlQuery[ 'OK' ]:
#      return sqlQuery
#    if not sqlQuery[ 'Value' ]:
#      _msg = 'Impossible to modify, %s (%s) is not on the DB' 
#      _msg = _msg % ( kwargs[ 'elementName' ],kwargs[ 'statusType' ] )
#      return S_ERROR( _msg )
#
#    #DateEffective
#    if kwargs[ 'dateEffective' ] is None:
#      kwargs[ 'dateEffective' ] = datetime.utcnow().replace( microsecond = 0 )
#
#    #LastCheckTime
#    if kwargs[ 'lastCheckTime' ] is None:
#      kwargs[ 'lastCheckTime' ] = datetime.utcnow().replace( microsecond = 0 )
#    
#    #We give the token owner one day
#    tOwner = kwargs[ 'tokenOwner' ] is not None and kwargs[ 'tokenOwner' ] != 'RS_SVC' 
#    if tOwner and kwargs[ 'tokenExpiration' ] is None:
#      _tomorrow = datetime.utcnow().replace( microsecond = 0 ) + timedelta( days = 1 )
#      kwargs[ 'tokenExpiration' ] = _tomorrow
#    
#    updateSQLQuery = self._updateElement( 'ElementStatus', kwargs ) 
#    
#    if not updateSQLQuery[ 'OK' ]:
#      return updateSQLQuery 
#
#    sqlQ      = list( sqlQuery[ 'Value' ][ 0 ] )[1:]
#
#    sqlDict = {}
#    sqlDict[ 'elementName' ]     = sqlQ[ 0 ]
#    sqlDict[ 'statusType' ]      = sqlQ[ 1 ]
#    sqlDict[ 'status']           = sqlQ[ 2 ]
#    sqlDict[ 'reason' ]          = sqlQ[ 3 ]
#    sqlDict[ 'dateCreated' ]     = sqlQ[ 4 ]
#    sqlDict[ 'dateEffective' ]   = sqlQ[ 5 ]   
#    sqlDict[ 'dateEnd' ]         = kwargs[ 'dateEffective' ]
#    sqlDict[ 'lastCheckTime' ]   = sqlQ[ 7 ]
#    sqlDict[ 'tokenOwner' ]      = sqlQ[ 8 ]
#    sqlDict[ 'tokenExpiration' ] = sqlQ[ 9 ]  
#    
#    sqlDict[ 'element' ] = kwargs[ 'element' ]
#       
#    res = self._insertElement( 'ElementHistory', sqlDict )
#    if not res[ 'OK' ]:
#      return res
#    
#    return updateSQLQuery  
#
#################################################################################
## stats PRIVATE FUNCTIONS      
#     
#  def __getStats( self, sqlQuery ):
#    
#    if not sqlQuery[ 'OK' ]:
#      return sqlQuery 
#
#    validStatuses = RssConfiguration.getValidStatus()
#
#    count = { 'Total' : 0 }
#    for validStatus in validStatuses:
#      count[ validStatus ] = 0
#
#    for x in sqlQuery[ 'Value' ]:
#      count[ x[0] ] = int( x[1] )
#
#    count['Total'] = sum( count.values() )
#    return S_OK( count ) 

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    