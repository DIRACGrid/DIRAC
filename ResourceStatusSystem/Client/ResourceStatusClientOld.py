## $HeadURL:  $
#''' ResourceStatusClient
#
#  Client to interact with the ResourceStatusDB.
#
#'''
#
#from datetime import datetime, timedelta
#
#from DIRAC                                           import S_OK, S_ERROR, gLogger
#from DIRAC.Core.DISET.RPCClient                      import RPCClient        
#from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping     import getDIRACSiteName            
#from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB  import ResourceStatusDB 
#from DIRAC.ResourceStatusSystem.Utilities.NodeTree   import Node      
#from DIRAC.ResourceStatusSystem.Utilities            import RssConfiguration  
#
#__RCSID__ = '$Id:  $'
#       
#class ResourceStatusClient:
#  '''
#  The :class:`ResourceStatusClient` class exposes the :mod:`DIRAC.ResourceStatus` 
#  API. All functions you need are on this client.
#  
#  It has the 'direct-db-access' functions, the ones of the type:
#   - insert
#   - update
#   - get
#   - delete 
#    
#  that return parts of the RSSConfiguration stored on the CS, and used everywhere
#  on the RSS module. Finally, and probably more interesting, it exposes a set
#  of functions, badly called 'boosters'. They are 'home made' functions using the
#  basic database functions that are interesting enough to be exposed.  
#  
#  The client will ALWAYS try to connect to the DB, and in case of failure, to the
#  XML-RPC server ( namely :class:`ResourceStatusDB` and :class:`ResourceStatusHancler` ).
#
#  You can use this client on this way
#
#   >>> from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
#   >>> rsClient = ResourceStatusClient()
#   
#  All functions calling methods exposed on the database or on the booster are 
#  making use of some syntactic sugar, in this case a decorator that simplifies
#  the client considerably.    
#  '''
#
#  def __init__( self , serviceIn = None ):
#    '''
#      The client tries to connect to :class:ResourceStatusDB by default. If it 
#      fails, then tries to connect to the Service :class:ResourceStatusHandler.
#    '''
#    if not serviceIn:
#      try:
#        self.gate = ResourceStatusDB()
#      except SystemExit:
#        self.gate = RPCClient( "ResourceStatus/ResourceStatus" )
#      except ImportError:
#        # Pilots will connect here, as MySQLdb is not installed for them
#        self.gate = RPCClient( "ResourceStatus/ResourceStatus" )  
#    else:
#      self.gate = serviceIn
#
#  def __query( self, queryType, tableName, kwargs ):
#    '''
#      This method is a rather important one. It will format the input for the DB
#      queries, instead of doing it on a decorator. Two dictionaries must be passed
#      to the DB. First one contains 'columnName' : value pairs, being the key
#      lower camel case. The second one must have, at lease, a key named 'table'
#      with the right table name. 
#    '''
#    # Functions we can call, just a light safety measure.
#    _gateFunctions = [ 'insert', 'update', 'get', 'delete' ] 
#    if not queryType in _gateFunctions:
#      return S_ERROR( '"%s" is not a proper gate call' % queryType )
#    
#    gateFunction = getattr( self.gate, queryType )
#    
#    # If meta is None, we set it to {}
#    meta   = ( True and kwargs.pop( 'meta' ) ) or {}
#    params = kwargs
#    del params[ 'self' ]     
#        
#    # This is an special case with the Element tables.
#    if tableName.startswith( 'Element' ):
#      element   = params.pop( 'element' )
#      tableName = tableName.replace( 'Element', element )
#      params[ '%sName' % element ] = params.pop( 'elementName' )
#          
#    meta[ 'table' ] = tableName
#    
#    gLogger.debug( 'Calling %s, with \n params %s \n meta %s' % ( queryType, params, meta ) )  
#    return gateFunction( params, meta )    
#
#################################################################################
## SITE FUNCTIONS
#      
#  def insertSite( self, siteName, siteType, gridSiteName, meta = None ):
#    '''
#    Inserts on Site a new row with the arguments given.
#    
#    :Parameters:
#      **siteName** - `string`
#        name of the site 
#      **siteType** - `string`
#        it has to be a valid site type, any of the defaults: `T0` | `T1` | `T2`\
#         | `T3`
#      **gridSiteName** - `string`
#        name of the  grid site the site belongs ( if any )
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'insert', 'Site', locals() )
#  def updateSite( self, siteName, siteType, gridSiteName, meta = None ):
#    '''
#    Updates Site with the parameters given. By default, `siteName` will be the \
#    parameter used to select the row. 
#    
#    :Parameters:
#      **siteName** - `string`
#        name of the site 
#      **siteType** - `string`
#        it has to be a valid site type, any of the defaults: `T0` | `T1` | `T2`\
#         | `T3`
#      **gridSiteName** - `string`
#        name of the  grid site the site belongs ( if any )
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument   
#    # pylint: disable-msg=W0613
#    return self.__query( 'update', 'Site', locals() )
#  def getSite( self, siteName = None, siteType = None, gridSiteName = None, 
#               meta = None ):
#    '''
#    Gets from Site all rows that match the parameters given.
#    
#    :Parameters:
#      **siteName** - `[, string, list]`
#        name of the site 
#      **siteType** - `[, string, list]`
#        it has to be a valid site type, any of the defaults: `T0` | `T1` | `T2`\
#         | `T3`
#      **gridSiteName** - `[, string, list]`
#        name of the  grid site the site belongs ( if any )
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''   
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'get', 'Site', locals() )
#  def deleteSite( self, siteName = None, siteType = None, gridSiteName = None, 
#                  meta = None ):
#    '''
#    Deletes from Site all rows that match the parameters given.
#    
#    :Parameters:
#      **siteName** - `[, string, list]`
#        name of the site 
#      **siteType** - `[, string, list]`
#        it has to be a valid site type, any of the defaults: `T0` | `T1` | `T2`\
#         | `T3`
#      **gridSiteName** - `[, string, list]`
#        name of the  grid site the site belongs ( if any )
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''   
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'delete', 'Site', locals() )
#  def getSitePresent( self, siteName = None, siteType = None, 
#                      gridSiteName = None, gridTier = None, statusType = None, 
#                      status = None, dateEffective = None, reason = None, 
#                      lastCheckTime = None, tokenOwner = None, 
#                      tokenExpiration = None, formerStatus = None, meta = None ):
#    '''
#    Gets from the view composed by Site, SiteStatus and SiteHistory all rows 
#    that match the parameters given ( not necessarily returns the same number 
#    of rows as are there on Site or SiteStatus ).
#    
#    :Parameters:
#      **siteName** - `[, string, list]`
#        name of the site 
#      **siteType** - `[, string, list]`
#        it has to be a valid site type, any of the defaults: `T0` | `T1` | `T2`\
#         | `T3`
#      **gridSiteName** - `[, string, list]`
#        name of the  grid site the site belongs ( if any )
#      **gridTier** - `[, string, list]`
#        grid tier of the associated grid site ( if any )   
#      **statusType** - `[, string, list]`
#        it has to be a valid status type for the `Site` granularity
#      **status** - `[, string, list]`
#        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
#        `Probing` | `Banned`  
#      **dateEffective** - `[, datetime, list]`
#        time-stamp from which the status & status type are effective
#      **reason** - `[, string, list]`
#        decision that triggered the assigned status  
#      **lastCheckTime** - `[, datetime, list]`
#        time-stamp setting last time the status & status were checked
#      **tokenOwner** - `[, string, list]`
#        token assigned to the site & status type
#      **tokenExpiration** - `[, datetime, list]`
#        time-stamp setting validity of token ownership    
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument   
#    # pylint: disable-msg=W0613
#    return self.__query( 'get', 'SitePresent', locals() )
#
#################################################################################
## SERVICE FUNCTIONS
#
#  def insertService( self, serviceName, serviceType, siteName, meta = None ):
#    '''
#    Inserts on Service a new row with the arguments given.
#    
#    :Parameters:
#      **serviceName** - `string`
#        name of the service 
#      **serviceType** - `string`
#        it has to be a valid service type, any of the defaults: `Computing` |\
#         `Storage` ...
#      **siteName** - `string`
#        name of the site the service belongs
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''   
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'insert', 'Service', locals() )
#  def updateService( self, serviceName, serviceType, siteName, meta = None ):
#    '''
#    Updates Service with the parameters given. By default, `serviceName` will \
#    be the parameter used to select the row.
#    
#    :Parameters:
#      **serviceName** - `string`
#        name of the service 
#      **serviceType** - `string`
#        it has to be a valid service type, any of the defaults: `Computing` | \
#        `Storage` ...
#      **siteName** - `string`
#        name of the site the service belongs
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''   
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'update', 'Service', locals() )
#  def getService( self, serviceName = None, serviceType = None, siteName = None, 
#                  meta = None ):
#    '''
#    Gets from Service all rows that match the parameters given.
#    
#    :Parameters:
#      **serviceName** - `[, string, list]`
#        name of the service 
#      **serviceType** - `[, string, list]`
#        it has to be a valid service type, any of the defaults: `Computing` | \
#        `Storage` ...
#      **siteName** - `[, string, list]`
#        name of the site the service belongs
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''   
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'get', 'Service', locals() )
#  def deleteService( self, serviceName = None, serviceType = None, 
#                     siteName = None, meta = None ):
#    '''
#    Deletes from Service all rows that match the parameters given.
#    
#    :Parameters:
#      **serviceName** - `[, string, list]`
#        name of the service 
#      **serviceType** - `[, string, list]`
#        it has to be a valid service type, any of the defaults: `Computing` | \
#        `Storage` ...
#      **siteName** - `[, string, list]`
#        name of the site the service belongs
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'delete', 'Service', locals() )
#  def getServicePresent( self, serviceName = None, siteName = None, 
#                         siteType = None, serviceType = None, statusType = None, 
#                         status = None, dateEffective = None, reason = None, 
#                         lastCheckTime = None, tokenOwner = None, 
#                         tokenExpiration = None, formerStatus = None, 
#                         meta = None ):
#    '''
#    Gets from the view composed by Service, ServiceStatus and ServiceHistory all 
#    rows that match the parameters given ( not necessarily returns the same 
#    number of rows as are there on Service or ServiceStatus ).
#    
#    :Parameters:
#      **serviceName** - `[, string, list]`
#        name of the service 
#      **siteName** - `[, string, list]`
#        name of the site the service belongs
#      **siteType** - `[, string, list]`
#        it has to be a valid site type, any of the defaults: `T0` | `T1` | `T2`\
#         | `T3`
#      **serviceType** - `[, string, list]`
#        it has to be a valid service type, any of the defaults: `Computing` | \
#        `Storage` ... 
#      **statusType** - `[, string, list]`
#        it has to be a valid status type for the `Service` granularity
#      **status** - `[, string, list]`
#        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
#        `Probing` | `Banned`  
#      **dateEffective** - `[, datetime, list]`
#        time-stamp from which the status & status type are effective
#      **reason** - `[, string, list]`
#        decision that triggered the assigned status  
#      **lastCheckTime** - `[, datetime, list]`
#        time-stamp setting last time the status & status were checked
#      **tokenOwner** - `[, string, list]`
#        token assigned to the site & status type
#      **tokenExpiration** - `[, datetime, list]`
#        time-stamp setting validity of token ownership    
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'get', 'ServicePresent', locals() )
#
#################################################################################
## RESOURCE FUNCTIONS
#
#  def insertResource( self, resourceName, resourceType, serviceType, siteName,
#                      gridSiteName, meta = None ):
#    '''
#    Inserts on Resource a new row with the arguments given.
#    
#    :Parameters:
#      **resourceName** - `string`
#        name of the resource 
#      **resourceType** - `string`
#        it has to be a valid resource type, any of the defaults: `CE` | \
#        `CREAMCE` ...
#      **serviceType** - `string`
#        type of the service it belongs, defaults are: `Computing` | `Storage` ..
#      **siteName** - `string`
#        name of the site the resource belongs ( if any )
#      **gridSiteName** - `string`
#        name of the grid site the resource belongs ( if any )  
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'insert', 'Resource', locals() )
#  def updateResource( self, resourceName, resourceType, serviceType, siteName,
#                      gridSiteName, meta = None ):
#    '''
#    Updates Resource with the parameters given. By default, `resourceName` will 
#    be the parameter used to select the row.
#    
#    :Parameters:
#      **resourceName** - `string`
#        name of the resource 
#      **resourceType** - `string`
#        it has to be a valid resource type, any of the defaults: `CE` | \
#        `CREAMCE` ...
#      **serviceType** - `string`
#        type of the service it belongs, defaults are: `Computing` | `Storage` ..
#      **siteName** - `string`
#        name of the site the resource belongs ( if any )
#      **gridSiteName** - `string`
#        name of the grid site the resource belongs ( if any )
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''   
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'update', 'Resource', locals() )
#  def getResource( self, resourceName = None, resourceType = None, 
#                   serviceType = None, siteName = None, gridSiteName = None, 
#                   meta = None ):
#    '''
#    Gets from Resource all rows that match the parameters given.
#    
#    :Parameters:
#      **resourceName** - `[, string, list]`
#        name of the resource 
#      **resourceType** - `[, string, list]`
#        it has to be a valid resource type, any of the defaults: `CE` | \
#        `CREAMCE` ...
#      **serviceType** - `[, string, list]`
#        type of the service it belongs, defaults are: `Computing` | `Storage` ..
#      **siteName** - `[, string, list]`
#        name of the site the resource belongs ( if any )
#      **gridSiteName** - `[, string, list]`
#        name of the grid site the resource belongs ( if any )
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'get', 'Resource', locals() )
#  def deleteResource( self, resourceName = None, resourceType = None, 
#                      serviceType = None, siteName = None, gridSiteName = None, 
#                      meta = None ):
#    '''
#    Deletes from Resource all rows that match the parameters given.
#    
#    :Parameters:
#      **resourceName** - `[, string, list]`
#        name of the resource 
#      **resourceType** - `[, string, list]`
#        it has to be a valid resource type, any of the defaults: `CE` | \
#        `CREAMCE` ...
#      **serviceType** - `[, string, list]`
#        type of the service it belongs, defaults are: `Computing` | `Storage` ...
#      **siteName** - `[, string, list]`
#        name of the site the resource belongs ( if any )   
#      **gridSiteName** - `[, string, list]`
#        name of the grid site the resource belongs ( if any )
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'delete', 'Resource', locals() )
#  def getResourcePresent( self, resourceName = None, siteName = None, 
#                          serviceType = None, gridSiteName = None, 
#                          siteType = None, resourceType = None, 
#                          statusType = None, status = None, 
#                          dateEffective = None, reason = None, 
#                          lastCheckTime = None, tokenOwner = None, 
#                          tokenExpiration = None, formerStatus = None, 
#                          meta = None ):
#    '''
#    Gets from the view composed by Resource, ResourceStatus and ResourceHistory 
#    all rows that match the parameters given ( not necessarily returns the same 
#    number of rows as are there on Resource or ResourceStatus ).
#    
#    :Parameters:
#      **resourceName** - `[, string, list]`
#        name of the resource
#      **siteName** - `[, string, list]`
#        name of the site the resource belongs ( if any )
#      **serviceType** - `[, string, list]`
#        it has to be a valid service type, any of the defaults: `Computing` | \
#        `Storage` ...
#      **gridSiteName** - `[, string, list]`
#        name of the grid site the resource belongs ( if any )      
#      **siteType** - `[, string, list]`
#        it has to be a valid site type, any of the defaults: `T0` | `T1` | `T2`\
#         | `T3`
#      **resourceType** - `[, string, list]`
#        it has to be a valid resource type, any of the defaults: `CE` | \
#        `CREAMCE` ...     
#      **statusType** - `[, string, list]`
#        it has to be a valid status type for the `Resource` granularity
#      **status** - `[, string, list]`
#        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
#        `Probing` | `Banned`  
#      **dateEffective** - `[, datetime, list]`
#        time-stamp from which the status & status type are effective
#      **reason** - `[, string, list]`
#        decision that triggered the assigned status  
#      **lastCheckTime** - `[, datetime, list]`
#        time-stamp setting last time the status & status were checked
#      **tokenOwner** - `[, string, list]`
#        token assigned to the site & status type
#      **tokenExpiration** - `[, datetime, list]`
#        time-stamp setting validity of token ownership    
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'get', 'ResourcePresent', locals() )
#
#################################################################################
## STORAGE ELEMENT FUNCTIONS
#
#  def insertStorageElement( self, storageElementName, resourceName, 
#                            gridSiteName, meta = None ):
#    '''
#    Inserts on StorageElement a new row with the arguments given.
#    
#    :Parameters:
#      **storageElementName** - `string`
#        name of the storage element 
#      **resourceName** - `string`
#        name of the resource the storage element belongs
#      **gridSiteName** - `string`
#        name of the grid site the storage element belongs
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''  
#    # Unused argument
#    # pylint: disable-msg=W0613  
#    return self.__query( 'insert', 'StorageElement', locals() )
#  def updateStorageElement( self, storageElementName, resourceName, 
#                            gridSiteName, meta = None ):
#    '''
#    Updates StorageElement with the parameters given. By default, 
#    `storageElementName` will be the parameter used to select the row.
#    
#    :Parameters:
#      **storageElementName** - `string`
#        name of the storage element 
#      **resourceName** - `string`
#        name of the resource the storage element belongs
#      **gridSiteName** - `string`
#        name of the grid site the storage element belongs
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'update', 'StorageElement', locals() )
#  def getStorageElement( self, storageElementName = None, resourceName = None, 
#                         gridSiteName = None, meta = None ):
#    '''
#    Gets from StorageElement all rows that match the parameters given.
#    
#    :Parameters:
#      **storageElementName** - `[, string, list]`
#        name of the storage element 
#      **resourceName** - `[, string, list]`
#        name of the resource the storage element belongs
#      **gridSiteName** - `[, string, list]`
#        name of the grid site the storage element belongs
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''   
#    # Unused argument
#    # pylint: disable-msg=W0613 
#    return self.__query( 'get', 'StorageElement', locals() )
#  def deleteStorageElement( self, storageElementName = None, 
#                            resourceName = None, gridSiteName = None, 
#                            meta = None ):
#    '''
#    Deletes from StorageElement all rows that match the parameters given.
#    
#    :Parameters:
#      **storageElementName** - `[, string, list]`
#        name of the storage element 
#      **resourceName** - `[, string, list]`
#        name of the resource the storage element belongs
#      **gridSiteName** - `[, string, list]`
#        name of the grid site the storage element belongs
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'delete', 'StorageElement', locals() )    
#  def getStorageElementPresent( self, storageElementName = None, 
#                                resourceName = None, gridSiteName = None, 
#                                siteType = None, statusType = None, 
#                                status = None, dateEffective = None, 
#                                reason = None, lastCheckTime = None, 
#                                tokenOwner = None, tokenExpiration = None, 
#                                formerStatus = None, meta = None ):
#    '''
#    Gets from the view composed by StorageElement, StorageElementStatus and 
#    StorageElementHistory all rows that match the parameters given ( not 
#    necessarily returns the same number of rows as are there on StorageElement 
#    or StorageElementStatus ).
#    
#    :Parameters:
#      **storageElementName** - `[, string, list]`
#        name of the storage element
#      **resourceName** - `[, string, list]`
#        name of the resource
#      **gridSiteName** - `[, string, list]`
#        name of the grid site the storage element belongs ( if any )
#      **siteType** - `[, string, list]`
#        it has to be a valid site type, any of the defaults: `T0` | `T1` | `T2`\
#         | `T3`     
#      **statusType** - `[, string, list]`
#        it has to be a valid status type for the `StorageElement` granularity
#      **status** - `[, string, list]`
#        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
#        `Probing` | `Banned`  
#      **dateEffective** - `[, datetime, list]`
#        time-stamp from which the status & status type are effective
#      **reason** - `[, string, list]`
#        decision that triggered the assigned status  
#      **lastCheckTime** - `[, datetime, list]`
#        time-stamp setting last time the status & status were checked
#      **tokenOwner** - `[, string, list]`
#        token assigned to the site & status type
#      **tokenExpiration** - `[, datetime, list]`
#        time-stamp setting validity of token ownership    
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument    
#    # pylint: disable-msg=W0613
#    return self.__query( 'get', 'StorageElementPresent', locals() )
#
#################################################################################
## GRID SITE FUNCTIONS
#
#  def insertGridSite( self, gridSiteName, gridTier, meta = None ):
#    '''
#    Inserts on GridSite a new row with the arguments given.
#    
#    :Parameters:
#      **gridSiteName** - `string`
#        name of the grid site
#      **gridTier** - `string`
#        grid tier of the grid site
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument    
#    # pylint: disable-msg=W0613
#    return self.__query( 'insert', 'GridSite', locals() )
#  def updateGridSite( self, gridSiteName, gridTier, meta = None ):
#    '''
#    Updates GridSite with the parameters given. By default, 
#    `gridSiteName` will be the parameter used to select the row.
#    
#    :Parameters:
#      **gridSiteName** - `string`
#        name of the grid site
#      **gridTier** - `string`
#        grid tier of the grid site
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'update', 'GridSite', locals() )   
#  def getGridSite( self, gridSiteName = None, gridTier = None, meta = None ):
#    '''
#    Gets from GridSite all rows that match the parameters given.
#
#    :Parameters:
#      **gridSiteName** - `[, string, list]`
#        name of the grid site
#      **gridTier** - `[, string, list]`
#        grid tier of the grid site
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'get', 'GridSite', locals() )
#  def deleteGridSite( self, gridSiteName = None, gridTier = None, meta = None ): 
#    '''
#    Deletes from GridSite all rows that match the parameters given.
#    
#    :Parameters:
#      **gridSiteName** - `[, string, list]`
#        name of the grid site
#      **gridTier** - `[, string, list]`
#        grid tier of the grid site
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''     
#    # Unused argument
#    # pylint: disable-msg=W0613      
#    return self.__query( 'delete', 'GridSite', locals() )
#
#################################################################################
## ELEMENT STATUS FUNCTIONS
#
#  def insertElementStatus( self, element, elementName, statusType, status, 
#                           reason, dateCreated, dateEffective, dateEnd, 
#                           lastCheckTime, tokenOwner, tokenExpiration, 
#                           meta = None ): 
#    return self.__query( 'insert', 'ElementStatus', locals() )
#  def updateElementStatus( self, element, elementName, statusType, status, 
#                           reason, dateCreated, dateEffective, dateEnd, 
#                           lastCheckTime, tokenOwner, tokenExpiration, 
#                           meta = None ):
#    return self.__query( 'update', 'ElementStatus', locals() )
#  def getElementStatus( self, element, elementName = None, statusType = None, 
#                        status = None, reason = None, dateCreated = None, 
#                        dateEffective = None, dateEnd = None, 
#                        lastCheckTime = None, tokenOwner = None, 
#                        tokenExpiration = None, meta = None ):
#    return self.__query( 'get', 'ElementStatus', locals() )
#  def deleteElementStatus( self, element, elementName = None, statusType = None, 
#                           status = None, reason = None, dateCreated = None, 
#                           dateEffective = None, dateEnd = None, 
#                           lastCheckTime = None, tokenOwner = None, 
#                           tokenExpiration = None, meta = None ):
#    return self.__query( 'delete', 'ElementStatus', locals() )
#
#################################################################################
## ELEMENT SCHEDULED STATUS FUNCTIONS
#
#  def insertElementScheduledStatus( self, element, elementName, statusType, 
#                                    status, reason, dateCreated, dateEffective, 
#                                    dateEnd, lastCheckTime, tokenOwner, 
#                                    tokenExpiration, meta = None ): 
#    return self.__query( 'insert', 'ElementScheduledStatus', locals() )
#  def updateElementScheduledStatus( self, element, elementName, statusType, 
#                                    status, reason, dateCreated, dateEffective, 
#                                    dateEnd, lastCheckTime, tokenOwner, 
#                                    tokenExpiration, meta = None ):
#    return self.__query( 'update', 'ElementScheduledStatus', locals() )
#  def getElementScheduledStatus( self, element, elementName = None, 
#                                 statusType = None, status = None, 
#                                 reason = None, dateCreated = None, 
#                                 dateEffective = None, dateEnd = None, 
#                                 lastCheckTime = None, tokenOwner = None, 
#                                 tokenExpiration = None, meta = None ):
#    return self.__query( 'get', 'ElementScheduledStatus', locals() )
#  def deleteElementScheduledStatus( self, element, elementName = None, 
#                                    statusType = None, status = None, 
#                                    reason = None, dateCreated = None,
#                                    dateEffective = None, dateEnd = None, 
#                                    lastCheckTime = None, tokenOwner = None, 
#                                    tokenExpiration = None, meta = None ):
#    return self.__query( 'delete', 'ElementScheduledStatus', locals() )
#
#################################################################################
## ELEMENT HISTORY FUNCTIONS
#
#  def insertElementHistory( self, element, elementName, statusType, status, 
#                            reason, dateCreated, dateEffective, dateEnd, 
#                            lastCheckTime, tokenOwner, tokenExpiration, 
#                            meta = None ): 
#    return self.__query( 'insert', 'ElementHistory', locals() )
#  def updateElementHistory( self, element, elementName, statusType, status, 
#                            reason, dateCreated, dateEffective, dateEnd, 
#                            lastCheckTime, tokenOwner, tokenExpiration, 
#                            meta = None ):
#    return self.__query( 'update', 'ElementHistory', locals() )
#  def getElementHistory( self, element, elementName = None, statusType = None, 
#                         status = None, reason = None, dateCreated = None, 
#                         dateEffective = None, dateEnd = None, 
#                         lastCheckTime = None, tokenOwner = None, 
#                         tokenExpiration = None, meta = None ):
#    return self.__query( 'get', 'ElementHistory', locals() )
#  def deleteElementHistory( self, element, elementName = None, 
#                            statusType = None, status = None, reason = None, 
#                            dateCreated = None, dateEffective = None, 
#                            dateEnd = None, lastCheckTime = None, 
#                            tokenOwner = None, tokenExpiration = None, 
#                            meta = None ):
#    return self.__query( 'delete', 'ElementHistory', locals() ) 
#
#################################################################################
## EXTENDED FUNCTIONS
#
#  def addOrModifySite( self, siteName, siteType, gridSiteName ):
#    return self.__addOrModifyElement( 'Site', locals() )
#
#  def addOrModifyService( self, serviceName, serviceType, siteName ):
#    return self.__addOrModifyElement( 'Service', locals() )
#
#  def addOrModifyResource( self, resourceName, resourceType, serviceType, 
#                           siteName, gridSiteName ):
#    return self.__addOrModifyElement( 'Resource', locals() )
#
#  def addOrModifyStorageElement( self, storageElementName, resourceName, 
#                                 gridSiteName ):
#    return self.__addOrModifyElement( 'StorageElement', locals() )
#
#  def addOrModifyGridSite( self, gridSiteName, gridTier ):
#    '''
#    Using `gridSiteName` to query the database, decides whether to insert or 
#    update the table.
#    
#    :Parameters:
#      **gridSiteName** - `string`
#        name of the grid site
#      **gridTier** - `string`
#        grid tier of the grid site
#
#    :return: S_OK() || S_ERROR()
#    '''
#
#    args = ( gridSiteName, gridTier )
#    kwargs = { 'gridSiteName' : gridSiteName, 'gridTier' : gridTier, 
#               'meta' : { 'onlyUniqueKeys' : True } }
#      
#    sqlQuery = self.getGridSite( **kwargs )
#   
#    if sqlQuery[ 'Value' ]:
#      return self.updateGridSite( *args )      
#    else:
#      return self.insertGridSite( *args )   
#
#  def modifyElementStatus( self, element, elementName, statusType, 
#                           status = None, reason = None, dateCreated = None, 
#                           dateEffective = None, dateEnd = None,
#                           lastCheckTime = None, tokenOwner = None, 
#                           tokenExpiration = None ):
#    return self.__modifyElementStatus( locals() )
#
#  def removeElement( self, element, elementName ):
#    '''
#    Deletes from <element>, <element>Status, <element>ScheduledStatus and 
#    <element>History all rows with `elementName`.
#    
#    :Parameters:
#      **element** - `string`
#        it has to be a valid element ( ValidElements ), any of the defaults: `Site` \
#        | `Service` | `Resource` | `StorageElement`
#      **elementName** - `[, string, list]`
#        name of the individual of class element  
#    
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument       
#    # pylint: disable-msg=W0613
#    return self.__removeElement( element, elementName )
#
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
## remove PRIVATE FUNCTIONS
#  
#  def __removeElement( self, element, elementName ):
#  
#    tables = [ 'ScheduledStatus', 'Status', 'History' ]
#    for table in tables:
#      
#      rDict = { 'elementName' : elementName, 'element' : element }
#      
#      sqlQuery = self._deleteElement( 'Element%s' % table, rDict )
#      if not sqlQuery[ 'OK' ]:
#        return sqlQuery
#    
#    _elementName = '%sName' % ( element[0].lower() + element[1:])
#    rDict = { _elementName : elementName }
#    sqlQuery = self._deleteElement( element, rDict )
#
#    return sqlQuery   
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
#
#################################################################################
## Getter functions
#
#  def _insertElement( self, elementTable, paramsDict ):
#    '''
#      Method that executes the insert method of the given element.
#    '''    
#    fname = 'insert%s' % elementTable
#    fElem = getattr( self, fname )
#    return fElem( **paramsDict )
#
#  def _updateElement( self, elementTable, paramsDict ):
#    '''
#      Method that executes the update method of the given element.
#    '''        
#    fname = 'update%s' % elementTable
#    fElem = getattr( self, fname )
#    return fElem( **paramsDict )
#
#  def _getElement( self, elementTable, paramsDict ):
#    '''
#      Method that executes the get method of the given element.
#    '''
#    fname = 'get%s' % elementTable
#    fElem = getattr( self, fname )
#    return fElem( **paramsDict )
#  
#  def _deleteElement( self, elementTable, paramsDict ): 
#    '''
#      Method that executes the delete method of the given element.
#    '''        
#    fname = 'delete%s' % elementTable
#    fElem = getattr( self, fname )
#    return fElem( **paramsDict )     
#    
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    