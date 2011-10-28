################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC.ResourceStatusSystem.API.private.ResourceStatusExtendedBaseAPI \
  import ResourceStatusExtendedBaseAPI
from DIRAC.ResourceStatusSystem.Utilities.Decorators import APIDecorator

class ResourceStatusAPI( object ):
  '''  
  The :class:`ResourceStatusAPI` class exposes all methods needed by RSS to
  interact with the database. This includes methods that interact directly with
  the database, and methods that actually do some processing using the outputs
  of the first ones.
  
  The methods that `directly` ( though the client ) access the database follow
  this convention:
  
    - insert + <TableName>||ElementStatus||ElementScheduledStatus||\
    ElementHistory
    - udpate + <TableName>||ElementStatus||ElementScheduledStatus||\
    ElementHistory
    - get + <TableName>||ElementStatus||ElementScheduledStatus||ElementHistory
    - delete + <TableName>||ElementStatus||ElementScheduledStatus||\
    ElementHistory
    
  If you want to use it, you can do it as follows:
  
   >>> from DIRAC.ResourceStatusSystem.API.ResourceStatusAPI import \
   ResourceStatusAPI
   >>> rmAPI = ResourceStatusAPI()
   >>> rmAPI.getSite()
   
  All `direct database access` functions have the possibility of using keyword 
  arguments to tune the SQL queries.
  '''
  
  def __init__( self ):  
    self.eBaseAPI = ResourceStatusExtendedBaseAPI()
    
  '''  
  ##############################################################################    
  # BASE API METHODS
  ##############################################################################
  '''
  
  @APIDecorator
  def insertSite( self, siteName, siteType, gridSiteName, **kwargs ):
    '''
    Inserts on Site a new row with the arguments given.
    
    :Parameters:
      **siteName** - `string`
        name of the site 
      **siteType** - `string`
        it has to be a valid site type, any of the defaults: `T0` | `T1` | `T2`\
         | `T3`
      **gridSiteName** - `string`
        name of the  grid site the site belongs ( if any )
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def updateSite( self, siteName, siteType, gridSiteName, **kwargs ):
    '''
    Updates Site with the parameters given. By default, `siteName` will be the \
    parameter used to select the row. 
    
    :Parameters:
      **siteName** - `string`
        name of the site 
      **siteType** - `string`
        it has to be a valid site type, any of the defaults: `T0` | `T1` | `T2`\
         | `T3`
      **gridSiteName** - `string`
        name of the  grid site the site belongs ( if any )
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def getSite( self, siteName = None, siteType = None, gridSiteName = None, 
               **kwargs ):
    '''
    Gets from Site all rows that match the parameters given.
    
    :Parameters:
      **siteName** - `string`
        name of the site 
      **siteType** - `string`
        it has to be a valid site type, any of the defaults: `T0` | `T1` | `T2`\
         | `T3`
      **gridSiteName** - `string`
        name of the  grid site the site belongs ( if any )
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def deleteSite( self, siteName = None, siteType = None, gridSiteName = None, 
                  **kwargs ):
    '''
    Deletes from Site all rows that match the parameters given.
    
    :Parameters:
      **siteName** - `[, string, list]`
        name of the site 
      **siteType** - `[, string, list]`
        it has to be a valid site type, any of the defaults: `T0` | `T1` | `T2`\
         | `T3`
      **gridSiteName** - `[, string, list]`
        name of the  grid site the site belongs ( if any )
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass     
  @APIDecorator
  def getSitePresent( self, siteName = None, siteType = None, 
                      gridSiteName = None, gridTier = None, statusType = None, 
                      status = None, dateEffective = None, reason = None, 
                      lastCheckTime = None, tokenOwner = None, 
                      tokenExpiration = None, formerStatus = None, **kwargs ):
    '''
    Gets from the view composed by Site, SiteStatus and SiteHistory all rows 
    that match the parameters given ( not necessarily returns the same number 
    of rows as are there on Site or SiteStatus ).
    
    :Parameters:
      **siteName** - `[, string, list]`
        name of the site 
      **siteType** - `[, string, list]`
        it has to be a valid site type, any of the defaults: `T0` | `T1` | `T2`\
         | `T3`
      **gridSiteName** - `[, string, list]`
        name of the  grid site the site belongs ( if any )
      **gridTier** - `[, string, list]`
        grid tier of the associated grid site ( if any )   
      **statusType** - `[, string, list]`
        it has to be a valid status type for the `Site` granularity
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`  
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the status & status type are effective
      **reason** - `[, string, list]`
        decision that triggered the assigned status  
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `[, string, list]`
        token assigned to the site & status type
      **tokenExpiration** - `[, datetime, list]`
        time-stamp setting validity of token ownership    
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass  
  @APIDecorator
  def insertService( self, serviceName, serviceType, siteName, **kwargs ):
    '''
    Inserts on Service a new row with the arguments given.
    
    :Parameters:
      **serviceName** - `string`
        name of the service 
      **serviceType** - `string`
        it has to be a valid service type, any of the defaults: `Computing` |\
         `Storage` ...
      **siteName** - `string`
        name of the site the service belongs
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def updateService( self, serviceName, serviceType, siteName, **kwargs ):
    '''
    Updates Service with the parameters given. By default, `serviceName` will \
    be the parameter used to select the row.
    
    :Parameters:
      **serviceName** - `string`
        name of the service 
      **serviceType** - `string`
        it has to be a valid service type, any of the defaults: `Computing` | \
        `Storage` ...
      **siteName** - `string`
        name of the site the service belongs
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def getService( self, serviceName = None, serviceType = None, siteName = None, 
                  **kwargs ):
    '''
    Gets from Service all rows that match the parameters given.
    
    :Parameters:
      **serviceName** - `string`
        name of the service 
      **serviceType** - `string`
        it has to be a valid service type, any of the defaults: `Computing` | \
        `Storage` ...
      **siteName** - `string`
        name of the site the service belongs
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def deleteService( self, serviceName = None, serviceType = None, 
                     siteName = None, **kwargs ):
    '''
    Deletes from Service all rows that match the parameters given.
    
    :Parameters:
      **serviceName** - `[, string, list]`
        name of the service 
      **serviceType** - `[, string, list]`
        it has to be a valid service type, any of the defaults: `Computing` | \
        `Storage` ...
      **siteName** - `[, string, list]`
        name of the site the service belongs
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator  
  def getServicePresent( self, serviceName = None, siteName = None, 
                         siteType = None, serviceType = None, statusType = None, 
                         status = None, dateEffective = None, reason = None, 
                         lastCheckTime = None, tokenOwner = None, 
                         tokenExpiration = None, formerStatus = None, 
                         **kwargs ):
    '''
    Gets from the view composed by Service, ServiceStatus and ServiceHistory all 
    rows that match the parameters given ( not necessarily returns the same 
    number of rows as are there on Service or ServiceStatus ).
    
    :Parameters:
      **serviceName** - `[, string, list]`
        name of the service 
      **siteName** - `[, string, list]`
        name of the site the service belongs
      **siteType** - `[, string, list]`
        it has to be a valid site type, any of the defaults: `T0` | `T1` | `T2`\
         | `T3`
      **serviceType** - `[, string, list]`
        it has to be a valid service type, any of the defaults: `Computing` | \
        `Storage` ... 
      **statusType** - `[, string, list]`
        it has to be a valid status type for the `Service` granularity
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`  
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the status & status type are effective
      **reason** - `[, string, list]`
        decision that triggered the assigned status  
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `[, string, list]`
        token assigned to the site & status type
      **tokenExpiration** - `[, datetime, list]`
        time-stamp setting validity of token ownership    
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def insertResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName, **kwargs ):
    '''
    Inserts on Resource a new row with the arguments given.
    
    :Parameters:
      **resourceName** - `string`
        name of the resource 
      **resourceType** - `string`
        it has to be a valid resource type, any of the defaults: `CE` | \
        `CREAMCE` ...
      **serviceType** - `string`
        type of the service it belongs, defaults are: `Computing` | `Storage` ..
      **siteName** - `string`
        name of the site the resource belongs ( if any )
      **gridSiteName** - `string`
        name of the grid site the resource belongs ( if any )  
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def updateResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName, **kwargs ):
    '''
    Updates Resource with the parameters given. By default, `resourceName` will 
    be the parameter used to select the row.
    
    :Parameters:
      **resourceName** - `string`
        name of the resource 
      **resourceType** - `string`
        it has to be a valid resource type, any of the defaults: `CE` | \
        `CREAMCE` ...
      **serviceType** - `string`
        type of the service it belongs, defaults are: `Computing` | `Storage` ..
      **siteName** - `string`
        name of the site the resource belongs ( if any )
      **gridSiteName** - `string`
        name of the grid site the resource belongs ( if any )
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def getResource( self, resourceName = None, resourceType = None, 
                   serviceType = None, siteName = None, gridSiteName = None, 
                   **kwargs ):
    '''
    Gets from Resource all rows that match the parameters given.
    
    :Parameters:
      **resourceName** - `[, string, list]`
        name of the resource 
      **resourceType** - `[, string, list]`
        it has to be a valid resource type, any of the defaults: `CE` | \
        `CREAMCE` ...
      **serviceType** - `[, string, list]`
        type of the service it belongs, defaults are: `Computing` | `Storage` ..
      **siteName** - `[, string, list]`
        name of the site the resource belongs ( if any )
      **gridSiteName** - `[, string, list]`
        name of the grid site the resource belongs ( if any )
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def deleteResource( self, resourceName = None, resourceType = None, 
                      serviceType = None, siteName = None, gridSiteName = None, 
                      **kwargs ):
    '''
    Deletes from Resource all rows that match the parameters given.
    
    :Parameters:
      **resourceName** - `[, string, list]`
        name of the resource 
      **resourceType** - `[, string, list]`
        it has to be a valid resource type, any of the defaults: `CE` | \
        `CREAMCE` ...
      **serviceType** - `[, string, list]`
        type of the service it belongs, defaults are: `Computing` | `Storage` ...
      **siteName** - `[, string, list]`
        name of the site the resource belongs ( if any )   
      **gridSiteName** - `[, string, list]`
        name of the grid site the resource belongs ( if any )
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator      
  def getResourcePresent( self, resourceName = None, siteName = None, 
                          serviceType = None, gridSiteName = None, 
                          siteType = None, resourceType = None, 
                          statusType = None, status = None, 
                          dateEffective = None, reason = None, 
                          lastCheckTime = None, tokenOwner = None, 
                          tokenExpiration = None, formerStatus = None, 
                          **kwargs ):
    '''
    Gets from the view composed by Resource, ResourceStatus and ResourceHistory 
    all rows that match the parameters given ( not necessarily returns the same 
    number of rows as are there on Resource or ResourceStatus ).
    
    :Parameters:
      **resourceName** - `[, string, list]`
        name of the resource
      **siteName** - `[, string, list]`
        name of the site the resource belongs ( if any )
      **serviceType** - `[, string, list]`
        it has to be a valid service type, any of the defaults: `Computing` | \
        `Storage` ...
      **gridSiteName** - `[, string, list]`
        name of the grid site the resource belongs ( if any )      
      **siteType** - `[, string, list]`
        it has to be a valid site type, any of the defaults: `T0` | `T1` | `T2`\
         | `T3`
      **resourceType** - `[, string, list]`
        it has to be a valid resource type, any of the defaults: `CE` | \
        `CREAMCE` ...     
      **statusType** - `[, string, list]`
        it has to be a valid status type for the `Resource` granularity
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`  
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the status & status type are effective
      **reason** - `[, string, list]`
        decision that triggered the assigned status  
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `[, string, list]`
        token assigned to the site & status type
      **tokenExpiration** - `[, datetime, list]`
        time-stamp setting validity of token ownership    
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def insertStorageElement( self, storageElementName, resourceName, 
                            gridSiteName, **kwargs ):
    '''
    Inserts on StorageElement a new row with the arguments given.
    
    :Parameters:
      **storageElementName** - `string`
        name of the storage element 
      **resourceName** - `string`
        name of the resource the storage element belongs
      **gridSiteName** - `string`
        name of the grid site the storage element belongs
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def updateStorageElement( self, storageElementName, resourceName, 
                            gridSiteName, **kwargs ):
    '''
    Updates StorageElement with the parameters given. By default, 
    `storageElementName` will be the parameter used to select the row.
    
    :Parameters:
      **storageElementName** - `string`
        name of the storage element 
      **resourceName** - `string`
        name of the resource the storage element belongs
      **gridSiteName** - `string`
        name of the grid site the storage element belongs
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator       
  def getStorageElement( self, storageElementName = None, resourceName = None, 
                         gridSiteName = None, **kwargs ):
    '''
    Gets from StorageElement all rows that match the parameters given.
    
    :Parameters:
      **storageElementName** - `[, string, list]`
        name of the storage element 
      **resourceName** - `[, string, list]`
        name of the resource the storage element belongs
      **gridSiteName** - `[, string, list]`
        name of the grid site the storage element belongs
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator       
  def deleteStorageElement( self, storageElementName = None, 
                            resourceName = None, gridSiteName = None, 
                            **kwargs ):
    '''
    Deletes from StorageElement all rows that match the parameters given.
    
    :Parameters:
      **storageElementName** - `[, string, list]`
        name of the storage element 
      **resourceName** - `[, string, list]`
        name of the resource the storage element belongs
      **gridSiteName** - `[, string, list]`
        name of the grid site the storage element belongs
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass    
  @APIDecorator      
  def getStorageElementPresent( self, storageElementName = None, 
                                resourceName = None, gridSiteName = None, 
                                siteType = None, statusType = None, 
                                status = None, dateEffective = None, 
                                reason = None, lastCheckTime = None, 
                                tokenOwner = None, tokenExpiration = None, 
                                formerStatus = None, **kwargs ):
    '''
    Gets from the view composed by StorageElement, StorageElementStatus and 
    StorageElementHistory all rows that match the parameters given ( not 
    necessarily returns the same number of rows as are there on StorageElement 
    or StorageElementStatus ).
    
    :Parameters:
      **storageElementName** - `[, string, list]`
        name of the storage element
      **resourceName** - `[, string, list]`
        name of the resource
      **gridSiteName** - `[, string, list]`
        name of the grid site the storage element belongs ( if any )
      **siteType** - `[, string, list]`
        it has to be a valid site type, any of the defaults: `T0` | `T1` | `T2`\
         | `T3`     
      **statusType** - `[, string, list]`
        it has to be a valid status type for the `StorageElement` granularity
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`  
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the status & status type are effective
      **reason** - `[, string, list]`
        decision that triggered the assigned status  
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `[, string, list]`
        token assigned to the site & status type
      **tokenExpiration** - `[, datetime, list]`
        time-stamp setting validity of token ownership    
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def insertGridSite( self, gridSiteName, gridTier, **kwargs ):
    '''
    Inserts on GridSite a new row with the arguments given.
    
    :Parameters:
      **gridSiteName** - `string`
        name of the grid site
      **gridTier** - `string`
        grid tier of the grid site
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def updateGridSite( self, gridSiteName, gridTier, **kwargs ):
    '''
    Updates GridSite with the parameters given. By default, 
    `gridSiteName` will be the parameter used to select the row.
    
    :Parameters:
      **gridSiteName** - `string`
        name of the grid site
      **gridTier** - `string`
        grid tier of the grid site
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator    
  def getGridSite( self, gridSiteName = None, gridTier = None, **kwargs ):
    '''
    Gets from GridSite all rows that match the parameters given.

    :Parameters:
      **gridSiteName** - `[, string, list]`
        name of the grid site
      **gridTier** - `[, string, list]`
        grid tier of the grid site
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator    
  def deleteGridSite( self, gridSiteName = None, gridTier = None, **kwargs ):        
    '''
    Deletes from GridSite all rows that match the parameters given.
    
    :Parameters:
      **gridSiteName** - `[, string, list]`
        name of the grid site
      **gridTier** - `[, string, list]`
        grid tier of the grid site
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def insertElementStatus( self, element, elementName, statusType, status, 
                           reason, dateCreated, dateEffective, dateEnd, 
                           lastCheckTime, tokenOwner, tokenExpiration, 
                           **kwargs ): 
    '''
    Inserts on <element>Status a new row with the arguments given.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **elementName** - `string`
        name of the individual of class element  
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **reason** - `string`
        decision that triggered the assigned status
      **dateCreated** - `datetime`
        time-stamp setting status assignment    
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **dateEnd** - `datetime`
        time-stamp setting end of status validity    
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership  
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def updateElementStatus( self, element, elementName, statusType, status, 
                           reason, dateCreated, dateEffective, dateEnd, 
                           lastCheckTime, tokenOwner, tokenExpiration, 
                           **kwargs ):
    '''
    Updates <element>Status with the parameters given. By default, 
    `elementName` and 'statusType' will be the parameters used to select the row.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **elementName** - `string`
        name of the individual of class element  
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **reason** - `string`
        decision that triggered the assigned status
      **dateCreated** - `datetime`
        time-stamp setting status assignment    
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **dateEnd** - `datetime`
        time-stamp setting end of status validity    
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership  
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
  @APIDecorator
  def getElementStatus( self, element, elementName = None, statusType = None, 
                        status = None, reason = None, dateCreated = None, 
                        dateEffective = None, dateEnd = None, 
                        lastCheckTime = None, tokenOwner = None, 
                        tokenExpiration = None, **kwargs ):
    '''
    Gets from <element>Status all rows that match the parameters given.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **elementName** - `[, string, list]`
        name of the individual of class element  
      **statusType** - `[, string, list]`
        it has to be a valid status type for the element class
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **reason** - `[, string, list]`
        decision that triggered the assigned status
      **dateCreated** - `[, datetime, list]`
        time-stamp setting status assignment    
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the status & status type are effective
      **dateEnd** - `[, datetime, list]`
        time-stamp setting end of status validity    
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `[, string, list]`
        token assigned to the site & status type
      **tokenExpiration** - `[, datetime, list]`
        time-stamp setting validity of token ownership  
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def deleteElementStatus( self, element, elementName = None, statusType = None, 
                           status = None, reason = None, dateCreated = None, 
                           dateEffective = None, dateEnd = None, 
                           lastCheckTime = None, tokenOwner = None, 
                           tokenExpiration = None, **kwargs ):
    '''
    Deletes from <element>Status all rows that match the parameters given.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **elementName** - `[, string, list]`
        name of the individual of class element  
      **statusType** - `[, string, list]`
        it has to be a valid status type for the element class
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **reason** - `[, string, list]`
        decision that triggered the assigned status
      **dateCreated** - `[, datetime, list]`
        time-stamp setting status assignment    
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the status & status type are effective
      **dateEnd** - `[, datetime, list]`
        time-stamp setting end of status validity    
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `[, string, list]`
        token assigned to the site & status type
      **tokenExpiration** - `[, datetime, list]`
        time-stamp setting validity of token ownership  
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def insertElementScheduledStatus( self, element, elementName, statusType, 
                                    status, reason, dateCreated, dateEffective, 
                                    dateEnd, lastCheckTime, tokenOwner, 
                                    tokenExpiration, **kwargs ): 
    '''
    Inserts on <element>ScheduledStatus a new row with the arguments given.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **elementName** - `string`
        name of the individual of class element  
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **reason** - `string`
        decision that triggered the assigned status
      **dateCreated** - `datetime`
        time-stamp setting status assignment    
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **dateEnd** - `datetime`
        time-stamp setting end of status validity    
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership  
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def updateElementScheduledStatus( self, element, elementName, statusType, 
                                    status, reason, dateCreated, dateEffective, 
                                    dateEnd, lastCheckTime, tokenOwner, 
                                    tokenExpiration, **kwargs ):
    '''
    Updates <element>ScheduledStatus with the parameters given. By default, 
    `elementName`, 'statusType' and `dateEffective` will be the parameters used 
    to select the row.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **elementName** - `string`
        name of the individual of class element  
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **reason** - `string`
        decision that triggered the assigned status
      **dateCreated** - `datetime`
        time-stamp setting status assignment    
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **dateEnd** - `datetime`
        time-stamp setting end of status validity    
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership  
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def getElementScheduledStatus( self, element, elementName = None, 
                                 statusType = None, status = None, 
                                 reason = None, dateCreated = None, 
                                 dateEffective = None, dateEnd = None, 
                                 lastCheckTime = None, tokenOwner = None, 
                                 tokenExpiration = None, **kwargs ):
    '''
    Gets from <element>ScheduledStatus all rows that match the parameters given.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **elementName** - `[, string, list]`
        name of the individual of class element  
      **statusType** - `[, string, list]`
        it has to be a valid status type for the element class
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **reason** - `[, string, list]`
        decision that triggered the assigned status
      **dateCreated** - `[, datetime, list]`
        time-stamp setting status assignment    
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the status & status type are effective
      **dateEnd** - `[, datetime, list]`
        time-stamp setting end of status validity    
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `[, string, list]`
        token assigned to the site & status type
      **tokenExpiration** - `[, datetime, list]`
        time-stamp setting validity of token ownership  
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def deleteElementScheduledStatus( self, element, elementName = None, 
                                    statusType = None, status = None, 
                                    reason = None, dateCreated = None,
                                    dateEffective = None, dateEnd = None, 
                                    lastCheckTime = None, tokenOwner = None, 
                                    tokenExpiration = None, **kwargs ):
    '''
    Deletes from <element>ScheduledStatus all rows that match the parameters 
    given.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **elementName** - `[, string, list]`
        name of the individual of class element  
      **statusType** - `[, string, list]`
        it has to be a valid status type for the element class
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **reason** - `[, string, list]`
        decision that triggered the assigned status
      **dateCreated** - `[, datetime, list]`
        time-stamp setting status assignment    
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the status & status type are effective
      **dateEnd** - `[, datetime, list]`
        time-stamp setting end of status validity    
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `[, string, list]`
        token assigned to the site & status type
      **tokenExpiration** - `[, datetime, list]`
        time-stamp setting validity of token ownership  
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def insertElementHistory( self, element, elementName, statusType, status, 
                            reason, dateCreated, dateEffective, dateEnd, 
                            lastCheckTime, tokenOwner, tokenExpiration, 
                            **kwargs ): 
    '''
    Inserts on <element>History a new row with the arguments given.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **elementName** - `string`
        name of the individual of class element  
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **reason** - `string`
        decision that triggered the assigned status
      **dateCreated** - `datetime`
        time-stamp setting status assignment    
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **dateEnd** - `datetime`
        time-stamp setting end of status validity    
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership  
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def updateElementHistory( self, element, elementName, statusType, status, 
                            reason, dateCreated, dateEffective, dateEnd, 
                            lastCheckTime, tokenOwner, tokenExpiration, 
                            **kwargs ):
    '''
    Updates <element>History with the parameters given. By default, 
    `elementName`, 'statusType', `reason` and `dateEnd` will be the parameters 
    used to select the row.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **elementName** - `string`
        name of the individual of class element  
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **reason** - `string`
        decision that triggered the assigned status
      **dateCreated** - `datetime`
        time-stamp setting status assignment    
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **dateEnd** - `datetime`
        time-stamp setting end of status validity    
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership  
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def getElementHistory( self, element, elementName = None, statusType = None, 
                         status = None, reason = None, dateCreated = None, 
                         dateEffective = None, dateEnd = None, 
                         lastCheckTime = None, tokenOwner = None, 
                         tokenExpiration = None, **kwargs ):
    '''
    Gets from <element>History all rows that match the parameters given.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **elementName** - `string`
        name of the individual of class element  
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **reason** - `string`
        decision that triggered the assigned status
      **dateCreated** - `datetime`
        time-stamp setting status assignment    
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **dateEnd** - `datetime`
        time-stamp setting end of status validity    
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership  
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def deleteElementHistory( self, element, elementName = None, 
                            statusType = None, status = None, 
                            reason = None, dateCreated = None,
                            dateEffective = None, dateEnd = None, 
                            lastCheckTime = None, tokenOwner = None,
                            tokenExpiration = None, **kwargs ):
    '''
    Deletes from <element>History all rows that match the parameters given.


    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **elementName** - `string`
        name of the individual of class element  
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **reason** - `string`
        decision that triggered the assigned status
      **dateCreated** - `datetime`
        time-stamp setting status assignment    
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **dateEnd** - `datetime`
        time-stamp setting end of status validity    
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership  
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass  
  @APIDecorator
  def getValidElements( self ):
    '''
    Gets ValidRes from `CS/Operations/RSSConfiguration/GeneralConfig/Resources`
    
    :Parameters: `None`
    
    :return: S_OK()
    '''
    pass
  @APIDecorator
  def getValidStatuses( self ):
    '''
    Gets ValidRes from `CS/Operations/RSSConfiguration/GeneralConfig/Status`
    
    :Parameters: `None`
    
    :return: S_OK()
    '''
    pass
  @APIDecorator
  def getValidStatusTypes( self ):  
    '''
    Gets ValidRes from `CS/Operations/RSSConfiguration/GeneralConfig/Resources`
    
    :Parameters: `None`
    
    :return: S_OK()
    '''
    pass
  @APIDecorator
  def getValidSiteTypes( self ):
    '''
    Gets ValidRes from `CS/Operations/RSSConfiguration/GeneralConfig/SiteType`
    
    :Parameters: `None`
    
    :return: S_OK()
    '''
    pass
  @APIDecorator
  def getValidServiceTypes( self ):
    '''
    Gets ValidRes from `CS/Operations/RSSConfiguration/GeneralConfig/ServiceType`
    
    :Parameters: `None`
    
    :return: S_OK()
    '''
    pass 
  @APIDecorator
  def getValidResourceTypes( self ):
    '''
    Gets ValidRes from `CS/Operations/RSSConfiguration/GeneralConfig/Resource\
    Type`
    
    :Parameters: `None`
    
    :return: S_OK()
    '''
    pass

  '''
  ##############################################################################
  # EXTENDED BASE API METHODS
  ##############################################################################
  '''
  
  @APIDecorator
  def addOrModifySite( self, siteName, siteType, gridSiteName ):
    '''
    Using `siteName` to query the database, decides whether to insert or update
    the table.
    
    :Parameters:
      **siteName** - `string`
        name of the site 
      **siteType** - `string`
        it has to be a valid site type, any of the defaults: `T0` | `T1` | `T2`\
         | `T3`
      **gridSiteName** - `string`
        name of the  grid site the site belongs ( if any )

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def addOrModifyService( self, serviceName, serviceType, siteName ):
    '''
    Using `serviceName` to query the database, decides whether to insert or 
    update the table.
    
    :Parameters:
      **serviceName** - `string`
        name of the service 
      **serviceType** - `string`
        it has to be a valid service type, any of the defaults: `Computing` | \
        `Storage` ...
      **siteName** - `string`
        name of the site the service belongs

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def addOrModifyResource( self, resourceName, resourceType, serviceType, 
                           siteName, gridSiteName ):
    '''
    Using `resourceName` to query the database, decides whether to insert or 
    update the table.
    
    :Parameters:
      **resourceName** - `string`
        name of the resource 
      **resourceType** - `string`
        it has to be a valid resource type, any of the defaults: `CE` | \
        `CREAMCE` ...
      **serviceType** - `string`
        type of the service it belongs, defaults are: `Computing` | `Storage` ..
      **siteName** - `string`
        name of the site the resource belongs ( if any )
      **gridSiteName** - `string`
        name of the grid site the resource belongs ( if any )  

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def addOrModifyStorageElement( self, storageElementName, resourceName, 
                                 gridSiteName ):
    '''
    Using `storageElementName` to query the database, decides whether to insert 
    or update the table.
    
    :Parameters:
      **storageElementName** - `string`
        name of the storage element 
      **resourceName** - `string`
        name of the resource the storage element belongs
      **gridSiteName** - `string`
        name of the grid site the storage element belongs

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def addOrModifyGridSite( self, gridSiteName, gridTier ):
    '''
    Using `gridSiteName` to query the database, decides whether to insert or 
    update the table.
    
    :Parameters:
      **gridSiteName** - `string`
        name of the grid site
      **gridTier** - `string`
        grid tier of the grid site

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def modifyElementStatus( self, element, elementName, statusType, 
                           status = None, reason = None, dateCreated = None, 
                           dateEffective = None, dateEnd = None,
                           lastCheckTime = None, tokenOwner = None, 
                           tokenExpiration = None ):
    '''
    Updates <element>Status with the parameters given. By default, 
    `elementName` and 'statusType' will be the parameters used to select the 
    row.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **elementName** - `string`
        name of the individual of class element  
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `[, string]`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`
      **reason** - `[, string]`
        decision that triggered the assigned status
      **dateCreated** - `[, datetime]`
        time-stamp setting status assignment    
      **dateEffective** - `[, datetime]`
        time-stamp from which the status & status type are effective
      **dateEnd** - `[, datetime]`
        time-stamp setting end of status validity    
      **lastCheckTime** - `[, datetime]`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `[, string]`
        token assigned to the site & status type
      **tokenExpiration** - `[, datetime]`
        time-stamp setting validity of token ownership  

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def removeElement( self, element, elementName ):
    '''
    Deletes from <element>, <element>Status, <element>ScheduledStatus and 
    <element>History all rows with `elementName`.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **elementName** - `string`
        name of the individual of class element  
    
    :return: S_OK() || S_ERROR()
    '''    
    pass
  @APIDecorator
  def getServiceStats( self, siteName, statusType = None ):
    '''
    Computes simple statistics of `Active`, `Bad`, `Probing` and `Banned` 
    Services of a Site;
    
    :Parameters:
      **siteName** - `string`
        name of the site
      **statusType** - `[, string]`
        it has to be a valid status type for the 'Site' granularity
        
    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def getResourceStats( self, element, name, statusType = None ):
    '''
    Computes simple statistics of `Active`, `Bad`, `Probing` and `Banned` 
    Resources of a Site or a Service;
    
    :Parameters:
      **element** - `string`
        it has to be either `Site` or `Service`
      **name** - `string`
        name of the individual of element class
      **statusType** - `[, string]`
        it has to be a valid status type for the element class
        
    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def getStorageElementStats( self, element, name, statusType = None ):
    '''
    Computes simple statistics of `Active`, `Bad`, `Probing` and `Banned` 
    StorageElements of a Site or a Resource;
    
    :Parameters:
      **element** - `string`
        it has to be either `Site` or `Resource`
      **name** - `string`
        name of the individual of element class
      **statusType** - `[, string]`
        it has to be a valid status type for the element class
        
    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def getGeneralName( self, from_element, name, to_element ):
    '''
    Get name of a individual of granularity `from_g`, to the name of the
    individual with granularity `to_g`

    For a StorageElement, get either the Site, Service or the Resource name.
    For a Resource, get either the Site name or Service name.
    For a Service name, get the Site name

    :Parameters:
      **from_element** - `string`
        granularity of the element named name
      **name** - `string`
        name of the element
      **to_element** - `string`
        granularity of the desired name
        
    :return: S_OK() || S_ERROR()        
    '''
    pass
  @APIDecorator
  def getGridSiteName( self, granularity, name ):
    '''
    Get grid site name for the given individual.
    
    :Parameters:
      **granularity** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **name** - `string`
        name of the element
    
    :return: S_OK() | S_ERROR()      
    '''
    pass
  @APIDecorator
  def getTokens( self, granularity, name = None, tokenExpiration = None, 
                 statusType = None, **kwargs ):
    '''
    Get tokens for the given parameters. If `tokenExpiration` given, will select
    the ones with tokenExpiration older than the given one.

    :Parameters:
      **granularity** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **name** - `[, string, list]`
        name of the element  
      **tokenExpiration** - `[, datetime]`
        time-stamp with the token expiration time
      **statusType** - `[, string, list]`
        it has to be a valid status type for the granularity class
    
    :return: S_OK || S_ERROR
    '''
    pass
  @APIDecorator
  def setToken( self, granularity, name, statusType, reason, tokenOwner, 
                tokenExpiration ):
    '''
    Updates <granularity>Status with the parameters given, using `name` and 
    `statusType` to select the row.
        
    :Parameters:
      **granularity** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **name** - `string`
        name of the element  
      **statusType** - `string`
        it has to be a valid status type for the granularity class
      **reason** - `string`
        decision that triggered the assigned status
      **tokenOwner** - `string`
        token assigned to the element & status type    
      **tokenExpiration** - `datetime`
        time-stamp with the token expiration time
    
    :return: S_OK || S_ERROR
    '''
    pass
  @APIDecorator
  def setReason( self, granularity, name, statusType, reason ):
    '''
    Updates <granularity>Status with the parameters given, using `name` and 
    `statusType` to select the row.
    
    :Parameters:
      **granularity** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **name** - `string`
        name of the element  
      **statusType** - `string`
        it has to be a valid status type for the granularity class
      **reason** - `string`
        decision that triggered the assigned status
    
    :return: S_OK || S_ERROR
    '''    
    pass
  @APIDecorator
  def setDateEnd( self, granularity, name, statusType, dateEffective ):
    '''
    Updates <granularity>Status with the parameters given, using `name` and 
    `statusType` to select the row.
    
    :Parameters:
      **granularity** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **name** - `string`
        name of the element  
      **statusType** - `string`
        it has to be a valid status type for the granularity class
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
    
    :return: S_OK || S_ERROR
    '''
    pass
  @APIDecorator
  def whatIs( self, name ):
    '''
    Finds which is the granularity of the given name.
    
    :Parameters:
      **name** - `string`
        name of the element  
    
    :return: S_OK || S_ERROR
    '''
    pass
  @APIDecorator
  def getStuffToCheck( self, granularity, checkFrequency, **kwargs ):
    '''
    Gets from the <granularity>Present view all elements that match the 
    chechFrequency values ( lastCheckTime(j) < now - checkFrequencyTime(j) ). 
    
    :Parameters:
      **granularity** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **checkFrequency** - `dict`
        used to set chech frequency depending on state and type
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. 
    
    :return: S_OK() || S_ERROR()    
    '''
    pass
  @APIDecorator
  def getMonitoredStatus( self, granularity, name ):
    '''
    Gets from <granularity>Present the present status of name.
    
    :Parameters:
      **granularity** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **name** - `string`
        name of the element
     
    :return: S_OK() || S_ERROR()      
    '''
    pass
  @APIDecorator
  def getMonitoredsStatusWeb( self, granularity, selectDict, startItem, 
                              maxItems ):
    '''
    Get present sites status list, for the web.
    
    :Parameters:
      **granularity** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`
      **selectDict** - `dict`
        meta-data for the MySQL query
      **startItem** - `integer`
        first item index of the slice returned
      **maxItems** - `integer`
        length of the slice returned
        
    :return: S_OK() || S_ERROR()          
    '''
    pass        
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF                                                     