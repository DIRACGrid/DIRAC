"""
The ResourcesStatusDB module contains a couple of exception classes, and a
class to interact with the ResourceStatus DB.
"""

from datetime import datetime

from DIRAC import S_OK

from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getDIRACSiteName

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException, InvalidRes, InvalidStatus

from DIRAC.ResourceStatusSystem.Utilities.Utils import where, convertTime
from DIRAC.ResourceStatusSystem import ValidRes, ValidStatus, ValidSiteType, \
    ValidResourceType, ValidServiceType

#############################################################################

class RSSDBException( RSSException ):
  """
  DB exception
  """

  def __init__( self, message = "" ):
    self.message = message
    RSSException.__init__( self, message )

  def __str__( self ):
    return "Exception in the RSS DB: " + repr( self.message )

#############################################################################

class NotAllowedDate( RSSException ):
  """
  Exception that signals a not allowed date
  """

  def __init__( self, message = "" ):
    self.message = message
    RSSException.__init__( self, message )

  def __str__( self ):
    return "Not allowed date in the RSS DB: " + repr( self.message )

#############################################################################

class ResourceStatusDB:
  """
  The ResourcesStatusDB class is a front-end to the Resource Status Database.

  The simplest way to instantiate an object of type :class:`ResourceStatusDB`
  is simply by calling

   >>> rsDB = ResourceStatusDB()

  This way, it will use the standard :mod:`DIRAC.Core.Base.DB`.
  But there's the possibility to use other DB classes.
  For example, we could pass custom DB instantiations to it,
  provided the interface is the same exposed by :mod:`DIRAC.Core.Base.DB`.

   >>> AnotherDB = AnotherDBClass()
   >>> rsDB = ResourceStatusDB(DBin = AnotherDB)

  Alternatively, for testing purposes, you could do:

   >>> from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
   >>> mockDB = Mock()
   >>> rsDB = ResourceStatusDB(DBin = mockDB)

  Or, if you want to work with a local DB, providing it's mySQL:

   >>> rsDB = ResourceStatusDB(DBin = ['UserName', 'Password'])

  """

  ValidStatusTypes = { 'Site'           : [''],
                       'Service'        : [''],
                       'Resource'       : [''],
                       'StorageElement' : [ 'Read','Write' ] }


  def __init__( self, *args, **kwargs ):

    if len( args ) == 1:
      if isinstance( args[ 0 ], str ):
        maxQueueSize = 10
      if isinstance( args[ 0 ], int ):
        maxQueueSize = args[ 0 ]
    elif len( args ) == 2:
      maxQueueSize = args[ 1 ]
    elif len( args ) == 0:
      maxQueueSize = 10

    if 'DBin' in kwargs.keys():
      DBin = kwargs[ 'DBin' ]
      if isinstance( DBin, list ):
        from DIRAC.Core.Utilities.MySQL import MySQL
        self.db = MySQL( 'localhost', DBin[ 0 ], DBin[ 1 ], 'ResourceStatusDB' )
      else:
        self.db = DBin
    else:
      from DIRAC.Core.Base.DB import DB
      self.db = DB( 'ResourceStatusDB', 'ResourceStatus/ResourceStatusDB', maxQueueSize )

  '''
  ##############################################################################
  # ELEMENT FUNCTIONS
  ##############################################################################
  '''

  def __validateStatus( self, status ):
    
    if not status in ValidStatus:              
      message = '%s is not a valid status' % status
      raise RSSDBException, where( self, self.__validateStatus ) + message

  def __validateRes( self, res ):
    
    if not res in ValidRes:              
      message = '%s is not a valid res' % res
      raise InvalidRes, where( self, self.__validateRes ) + message

  def __validateSiteType( self, siteType ):
    
    if not siteType in ValidSiteType:
      message = '%s is not a valid site type' % siteType
      raise InvalidRes, where( self, self.__validateSiteType ) + message
          
  def __validateServiceType( self, serviceType ):
    
    if not serviceType in ValidServiceType:
      message = '%s is not a valid service type' % serviceType
      raise InvalidRes, where( self, self.__validateServiceType ) + message

  def __validateResourceType( self, resourceType ):
    
    if not resourceType in ValidResourceType:
      message = '%s is not a valid resource type' % resourceType
      raise InvalidRes, where( self, self.__validateResourceType ) + message


  def __validateElementTableName( self, element ):
    
    element = element.replace('Status','').replace('History','')
    self.__validateRes( element )
                                   
  def __validateElementStatusTypes( self, element, statusTypes ):
    
    if not isinstance( statusTypes, list ):
      statusTypes = [ statusTypes ]
    
    for statusType in statusTypes: 
      if not statusType in self.ValidStatusTypes[ element ]:
        message = '%s is not a valid statusType for %s' % ( statusType, element )
        raise RSSDBException, where( self, self.__validateElementStatusTypes ) + message


  def __getWhereElements( self, element, dict ):
    
    if element in ValidRes:
      elements = [ '%sName' % element ]
    elif element.replace( 'Status', '' ) in ValidRes:
      elements = [ '%sName' % element.replace( 'Status', '' ), 'StatusType' ]
    elif element.replace( 'History', '') in ValidRes:
      elements = [ '%sName' % element.replace( 'History', '' ), 'StatusType', 'DateEnd' ]     
    else:
      message = '%s is a wrong element' % element
      raise RSSDBException, where( self, self.__getWhereElements ) + message

    whereElements = ' AND '.join("%s='%s'" % ( el, dict[el] ) for el in elements) 
    return whereElements 
   
  def __getMultipleWhereElements( self, dict ):
   
    items = []

    for k,v in dict.items():
      if v is None:
        pass
      elif isinstance( v, list ):
        items.append( '%s IN %s' % ( k, tuple(v) ) )
      else:
        items.append( "%s='%s'" % ( k, v ) )
                
    whereElements = ' AND '.join( item for item in items ) 
    return whereElements 
             
  def __getElementUniqueKeys( self, element ):      
      
    elements = [ '%sName' % element ]  
        
    if element in ValidRes:    
      pass      
    elif element.replace( 'Status', '' ) in ValidRes:    
      elements.append( 'StatusType' )
    elif element.replace( 'History', '') in ValidRes:
      elements.extend( [ 'StatusType', 'DateEnd' ] )
    else:
      message = '%s is a wrong element' % element
      raise RSSDBException, where( self, self.__getElementUniqueKeys ) + message

    return elements

  def __generateRowDict( self, dict ):

    rDict = {}
    for k,v in dict.items():
      if k not in ['self', 'dict', 'k', 'v', 'rDict' ]:
        if v is not None:
          rDict[ k[0].upper() + k[1:] ] = v   
    
    return rDict
        
  def __getColumns( self, columns ):
    
    cols = ""
    
    if columns is None:
      cols = "*"
    else:
      if not isinstance( columns, list):
        columns = [ columns ]
      cols = ','.join( col for col in columns )  
      
    return cols        
        
  def __addElementRow( self, element, dict ):
    
    self.__validateElementTableName( element )
    
    req = "INSERT INTO %s (" % element 
    req += ','.join( "%s" % key for key in dict.keys())
    req += ") VALUES ("
    req += ','.join( "'%s'" % value for value in dict.values())
    req += ")"   

    resUpdate = self.db._update( req )
    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self.__addElementRow ) + resUpdate[ 'Message' ]

  def __getElementRow( self, element, dict, columns ):

    self.__validateElementTableName( element )
        
    whereElements = self.__getMultipleWhereElements( dict )    
    cols          = self.__getColumns( columns )      
        
    req = "SELECT %s from %s" % ( cols, element )
    if whereElements:
      req += " WHERE %s" % whereElements

    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.__getElementRow ) + resQuery[ 'Message' ]

    return resQuery

  def __getElementStatusRowCount( self, element, dict ):

    self.__validateRes( element )

    whereElements = self.__getMultipleWhereElements( dict )    
        
    req = "SELECT Status, COUNT(*) from %sStatus" % element
    if whereElements:
      req += " WHERE %s" % whereElements
    req += " GROUP BY Status"

    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.__getElementStatusRowCount ) + resQuery[ 'Message' ]

    return resQuery  

  def __updateElementRow( self, element, dict ):

    self.__validateElementTableName( element )

    uniqueKeys = self.__getElementUniqueKeys( element )

    req = "UPDATE %s SET " % element
    req += ','.join( "%s='%s'" % (key,value) for (key,value) in dict.items() if (key not in uniqueKeys) )
    req += " WHERE %s" % self.__getWhereElements( element, dict )
    
    resUpdate = self.db._update( req )
    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self.__updateElementRow ) + resUpdate[ 'Message' ]

  def __deleteElementRow( self, element, dict ):
    
    self.__validateElementTableName( element )
            
    req = "DELETE from %s WHERE " % element
    req += self.__getMultipleWhereElements( element, dict )
    
    resDel = self.db._update( req )
    if not resDel[ 'OK' ]:
      raise RSSDBException, where( self, self.__deleteElementRow ) + resDel[ 'Message' ]  
  
  def __addOrModifyElement( self, element, dict ):
    
    self.__validateRes( element )
    
    elemnt = self.__getElementRow( element, 
                                   { 
                                    '%sName' % element : dict[ '%sName' % element ] 
                                    } 
                                  )
    if not elemnt[ 'OK' ]:
      raise RSSDBException, where( self, self.__addOrModifyElement ) + elemnt[ 'Message' ]

    if elemnt[ 'Value' ]:
      self.__updateElementRow( element, dict )
    else:
      # If we add a new site, we set the new Site with status 'Banned' 
      self.__addElementRow( element, dict )
      
      defaultStatus = 'Banned'
      defaultReason = 'Added to DB'
      tokenOwner    = 'RS_SVC'
      
      for statusType in self.ValidStatusTypes[ element ]:
        
        setStatus = getattr( self, 'set%sStatus' % element)
        setStatus( dict[ '%sName' % element ], statusType, defaultStatus, 
                   defaultReason, tokenOwner )
    
  def __setElementStatus( self, element, dict ):
    
    # START VALIDATION #
    self.__validateRes( element )
    self.__validateElementStatusTypes( element, dict['StatusType'])
    self.__validateStatus( dict['Status'] )
    # If elementName does not exist, the DB will complain with missing FK.
    # END VALIDATION #
    
    currentStatus = self.__getElementRow( '%sStatus' % element, 
                                          {
                                           '%sName' % element : dict[ '%sName' % element ],
                                           'StatusType'       : dict[ 'StatusType' ]
                                           }
                                         )
    if not currentStatus[ 'OK' ]:
      raise RSSDBException, where( self, self.__setElementStatus ) + currentStatus[ 'Message' ]
    
    now   = datetime.utcnow()
    never = datetime( 9999, 12, 31, 23, 59, 59 ).replace( microsecond = 0 )
    
    dict[ 'TokenExpiration' ] = ( 1 and ( dict.has_key['TokenExpiration'] and dict['TokenExpiration'] ) ) or never 
    dict[ 'DateCreated' ]     = ( 1 and ( dict.has_key['DateCreated']     and dict['DateCreated']     ) ) or now
    dict[ 'DateEffective' ]   = ( 1 and ( dict.has_key['DateEffective']   and dict['DateEffective']   ) ) or now
    dict[ 'DateEnd' ]         = ( 1 and ( dict.has_key['DateEnd']         and dict['DateEnd']         ) ) or never
    dict[ 'LastCheckTime' ]   = now  
        
    if currentStatus[ 'Value' ]:
    
      self.__updateElementRow( '%sStatus' % element , dict )
      
      cS            = currentStatus[ 0 ]
      dict[ 'Status' ]        = cS[ 2 ]
      dict[ 'Reason' ]        = cS[ 3 ]
      dict[ 'DateCreated' ]   = cS[ 4 ]
      dict[ 'DateEffective' ] = cS[ 5 ]
      dict[ 'DateEnd' ]       = cS[ 6 ]
      dict[ 'LastCheckTime' ] = cS[ 7 ]
      dict[ 'TokenOwner' ]    = cS[ 8 ]
 
      self.__addElementRow( '%sHistory' % element , dict)
      
    else:
      self.__addElementRow( '%sStatus' % element , dict )
    
  def __getElements( self, element, dict, columns = None ):    
    
    # START VALIDATION #
    self.__validateRes( element )
    # END VALIDATION #    
    
    elements = self.__getElementRow( element, dict, columns )
    
    if not elements[ 'OK' ]:
      raise RSSDBException, where( self, self.__getElements ) + elements[ 'Message' ]
    
    return elements
    
  def __getElementsStatus( self, element, dict ):    

    # START VALIDATION #
    self.__validateRes( element )
    self.__validateElementStatusTypes( element, dict['StatusType'] )
    # END VALIDATION #    
    
    elementsStatus = self.__getElementRow( '%sStatus' % element, dict )
    
    if not elementsStatus[ 'OK' ]:
      raise RSSDBException, where( self, self.getSitesStatus ) + elementsStatus[ 'Message' ]
    
    return elementsStatus
    
  def __deleteElements( self, element, dict ):

    # START VALIDATION #
    self.__validateRes( element )
    # END VALIDATION #    
    self.__deleteElementRow( '%sHistory' % element, dict)
    self.__deleteElementRow( '%sScheduled' % element, dict)
    self.__deleteElementRow( '%sStatus' % element, dict)
    self.__deleteElementRow( '%s' % element, dict)
    

  '''    
  ##############################################################################
  # SITE FUNCTIONS
  ##############################################################################
  '''

  def addOrModifySite( self, siteName, siteType, gridSiteName ):
  
    rDict = self.__generateRowDict( locals() )
    
    # START VALIDATION #
    self.__validateSiteType( siteType )
      
    gridSite = self.getGridSitesList( gridSiteName = gridSiteName )
    if not gridSite[ 'OK' ]:
      raise RSSDBException, where( self, self.addOrModifySite ) + gridSite[ 'Message' ]
    if not gridSite[ 'Value' ]:
      message = '%s is not a known gridSiteName' % gridSiteName
      raise RSSDBException, where( self, self.addOrModifySite ) + message   
    # END VALIDATION #    
    
    self.__addOrModifyElement( 'Site', rDict)
    
  def setSiteStatus( self, siteName, statusType, status, reason, tokenOwner, 
                     tokenExpiration = None, dateCreated = None, 
                     dateEffective = None, dateEnd = None, lastCheckTime = None ):
    
    rDict = self.__generateRowDict( locals() )
    self.__setElementStatus( 'Site', rDict )

  def getSites( self, siteName ):
    
    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Site', rDict )

  def getSitesStatus( self, siteName, statusType ):
        
    rDict = self.__generateRowDict( locals() )
    return self.__getElementsStatus( 'Site', rDict)   

  def deleteSites( self, siteName ):
    
    rDict = self.__generateRowDict( locals() )  
    self.__deleteElements( 'Site', rDict)

  '''
  ##############################################################################
  # SERVICE FUNCTIONS
  ##############################################################################
  '''

  def addOrModifyService( self, serviceName, serviceType, siteName ):
 
    rDict = self.__generateRowDict( locals() )
    
    # START VALIDATION #
    self.__validateServiceType( serviceType )
    
    site = self.__getElementRow( 'Site', {'SiteName' : siteName } )
    if not site[ 'OK' ]:
      raise RSSDBException, where( self, self.addOrModifyService ) + site[ 'Message' ]
    if not site[ 'Value' ]:
      message = '%s is not a known siteName' % siteName
      raise RSSDBException, where( self, self.addOrModifyService ) + message
    # END VALIDATION #    
    
    self.__addOrModifyElement( 'Service', rDict)
       
  def setServiceStatus( self, serviceName, statusType, status, reason, tokenOwner, 
                        tokenExpiration = None, dateCreated = None, 
                        dateEffective = None, dateEnd = None, lastCheckTime = None ):

    rDict = self.__generateRowDict( locals() )
    self.__setElementStatus( 'Service', rDict )

  def getServices( self, serviceName ):
    
    rDict    = self.__generateRowDict( locals() )
    return self.__getElements( 'Service', rDict )
  
  def getServicesStatus( self, serviceName, statusType ):
    
    rDict          = self.__generateRowDict( locals() )   
    return self.__getElementsStatus( 'Service', rDict)   

  def getServiceStats( self, siteName, statusType = None ):
    """
    Returns simple statistics of active, probing, bad and banned services of a site;

    :params:
      :attr:`siteName`: string - a site name

    :returns:
      { 'Active':xx, 'Probing':yy, 'Bad':vv, 'Banned':zz, 'Total':xyz }
    """

    res = { 'Total' : 0 }
    for validStatus in ValidStatus:
      res[ validStatus ] = 0

    rDict = {}
    
    if statusType is not None:
      self.__validateElementStatusTypes( 'Service', statusType )
      rDict[ 'StatusType'] = statusType
    
    count = self.__getElementStatusRowCount( 'Service', rDict )

    if not count[ 'OK' ]:
      raise RSSDBException, where( self, self.getServiceStats ) + count[ 'Message' ]
    else:
      for x in count[ 'Value' ]:
        res[x[0]] = int(x[1])

    res['Total'] = sum( res.values() )

    return S_OK( res )
    
  def deleteServices( self, siteName ):
    
    rDict = self.__generateRowDict( locals() )  
    self.__deleteElements( 'Service', rDict)

  '''
  ##############################################################################
  # RESOURCE FUNCTIONS
  ##############################################################################
  '''
  
  def addOrModifyResource( self, resourceName, resourceType, serviceType, siteName,
                           gridSiteName ):

    rDict = self.__generateRowDict( locals() )
    
    # START VALIDATION #
    self.__validateResourceType( resourceType )
    self.__validateServiceType( serviceType )
    
    site = self.__getElementRow( 'Site', {'SiteName' : siteName } )
    if not site[ 'OK' ]:
      raise RSSDBException, where( self, self.addOrModifyService ) + site[ 'Message' ]
    if not site[ 'Value' ]:
      message = '%s is not a known siteName' % siteName
      raise RSSDBException, where( self, self.addOrModifyService ) + message
    
    gridSite = self.getGridSitesList( gridSiteName = gridSiteName )
    if not gridSite[ 'OK' ]:
      raise RSSDBException, where( self, self.addOrModifySite ) + gridSite[ 'Message' ]
    if not gridSite[ 'Value' ]:
      message = '%s is not a known gridSiteName' % gridSiteName
      raise RSSDBException, where( self, self.addOrModifySite ) + message 
    # END VALIDATION #    
    
    self.__addOrModifyElement( 'Resource', rDict)
  
  def setResourceStatus( self, resourceName, statusType, status, reason, tokenOwner, 
                         tokenExpiration = None, dateCreated = None, 
                         dateEffective = None, dateEnd = None, lastCheckTime = None ):
    
    rDict = self.__generateRowDict( locals() )
    self.__setElementStatus( 'Resource', rDict )
  
  def getResources( self, resourceName ):

    rDict     = self.__generateRowDict( locals() )
    return self.__getElements( 'Resource', rDict )
  
  def getResourcesStatus( self, resourceName, statusType ):
    
    rDict           = self.__generateRowDict( locals() )   
    return self.__getElementsStatus( 'Resource', rDict)   

  def getResourceStats( self, element, name, statusType = None ):
    """
    Returns simple statistics of active, probing, bad and banned services of a site;

    :params:
      :attr:`siteName`: string - a site name

    :returns:
      { 'Active':xx, 'Probing':yy, 'Bad':vv, 'Banned':zz, 'Total':xyz }
    """
    
    res = { 'Total' : 0 }
    for validStatus in ValidStatus:
      res[ validStatus ] = 0

    rDict = {}
    
    if statusType is not None:
      self.__validateElementStatusTypes( 'Service', statusType )
      rDict[ 'StatusType'] = statusType

    if element == 'Site':
      name   = self.getGridSiteName( element, name )[ 'Value' ]
      rDict[ 'GridSiteName' ] = name

    elif element == 'Service':
      serviceType = name.split( '@' )[ 0 ]
      name        = name.split( '@' )[ 1 ]
      if serviceType == 'Computing':
        rDict[ 'SiteName' ] = name
      else:
        name = self.getGridSiteName( 'Site', name )[ 'Value' ]
        rDict[ 'GridSiteName' ] = name
        rDict[ 'ServiceType' ]  = serviceType
    else:
      message = '%s is non accepted element. Only Site or Service' % element
      raise RSSDBException, where( self, self.getResourceStats ) + message

    count = self.__getElementStatusRowCount( 'Resource', rDict )

    if not count[ 'OK' ]:
      raise RSSDBException, where( self, self.getResourceStats ) + count[ 'Message' ]
    else:
      for x in count[ 'Value' ]:
        res[ x[ 0 ] ] = int( x[ 1 ] )

    res[ 'Total' ] = sum( res.values() )

    return S_OK( res )
    
  def deleteResources( self, resourceName ):
    
    rDict = self.__generateRowDict( locals() )  
    self.__deleteElements( 'Resource', rDict)

  '''
  ##############################################################################
  # STORAGE ELEMENT FUNCTIONS
  ##############################################################################
  '''

  def addOrModifyStorageElement( self, storageElementName, resourceName, 
                                 gridSiteName ):
    
    rDict = self.__generateRowDict( locals() )
    
    # START VALIDATION #
    
    resource = self.__getElementRow( 'Resource', {'ResourceName' : resourceName } )
    if not resource[ 'OK' ]:
      raise RSSDBException, where( self, self.addOrModifyResource ) + resource[ 'Message' ]
    if not resource[ 'Value' ]:
      message = '%s is not a known resourceName' % resourceName
      raise RSSDBException, where( self, self.addOrModifyResource ) + message
    
    gridSite = self.getGridSitesList( gridSiteName = gridSiteName )
    if not gridSite[ 'OK' ]:
      raise RSSDBException, where( self, self.addOrModifySite ) + gridSite[ 'Message' ]
    if not gridSite[ 'Value' ]:
      message = '%s is not a known gridSiteName' % gridSiteName
      raise RSSDBException, where( self, self.addOrModifySite ) + message 
    # END VALIDATION #    
    
    self.__addOrModifyElement( 'StorageElement', rDict)
  
  def setStorageElementStatus( self, storageElementName, statusType, status, 
                               reason, tokenOwner, tokenExpiration = None, 
                               dateCreated = None, dateEffective = None, dateEnd = None, 
                               lastCheckTime = None ):
    
    rDict = self.__generateRowDict( locals() )
    self.__setElementStatus( 'StorageElement', rDict )
  
  def getStorageElements( self, storageElementName ):
    
    rDict     = self.__generateRowDict( locals() )
    return self.__getElements( 'StorageElementName', rDict )
  
  def getStorageElementsStatus( self, storageElementName, statusType  ):
    
    rDict           = self.__generateRowDict( locals() )   
    return self.__getElementsStatus( 'StorageElement', rDict) 

  def getStorageElementStats( self, element, name, statusType = None ):
    
    res = { 'Total' : 0 }
    for validStatus in ValidStatus:
      res[ validStatus ] = 0

    rDict = {}
    
    if statusType is not None:
      self.__validateElementStatusTypes( 'StorageElement', statusType )
      rDict[ 'StatusType'] = statusType
    
    if element == 'Site':
      rDict[ 'GridSiteName' ] = self.getGridSiteName( element, name )[ 'Value' ]
    elif element == 'Resource':
      rDict[ 'ResourceName' ] = name
    
    count = self.__getElementStatusRowCount( 'StorageElement', rDict )

    if not count[ 'OK' ]:
      raise RSSDBException, where( self, self.getStorageElementStats ) + count[ 'Message' ]
    else:
      for x in count[ 'Value' ]:
        res[x[0]] = int(x[1])

    res['Total'] = sum( res.values() )

    return S_OK( res )   
    
  def deleteStorageElements( self, storageElementName ):
    
    rDict = self.__generateRowDict( locals() )  
    self.__deleteElements( 'StorageElement', rDict)

  '''
  ##############################################################################
  # GRID SITE FUNCTIONS
  ##############################################################################
  Hardcoded SQL queries, bad bad.. to be fixed.
  '''

  def addOrModifyGridSite( self, name, tier  ):
    """
    Add or modify a Grid Site to the GridSites table.

    :params:
      :attr:`name`: string - name of the site in GOC DB

      :attr:`tier`: string - tier of the site
    """

    if tier not in ValidSiteType:
      raise RSSDBException, "Not the right SiteType"

    req = "SELECT GridSiteName, GridTier FROM GridSites "
    req = req + "WHERE GridSiteName = '%s'" %( name )
    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.addOrModifyGridSite ) + resQuery[ 'Message' ]

    if resQuery[ 'Value' ]:
      req = "UPDATE GridSites SET GridTier = '%s' WHERE GridSiteName = '%s'" %( tier, name )

      resUpdate = self.db._update( req )
      if not resUpdate[ 'OK' ]:
        raise RSSDBException, where( self, self.addOrModifyGridSite ) + resUpdate[ 'Message' ]
    else:
      req = "INSERT INTO GridSites (GridSiteName, GridTier) VALUES ('%s', '%s')" %( name, tier )

      resUpdate = self.db._update( req )
      if not resUpdate[ 'OK' ]:
        raise RSSDBException, where( self, self.addOrModifyGridSite ) + resUpdate[ 'Message' ]
  
  def getGridSitesList( self, paramsList = None, gridSiteName = None, gridTier = None ):
    """
    Get grid site lists.

    :params:
      :attr:`paramsList`: a list of parameters can be entered. If not given,
      a custom list is used.

      :attr:`gridSiteName` grid site name. If not given, fetch all.

      :attr:`gridTier`: a string or a list representing the site type.
      If not given, fetch all.

    :return:
      list of gridSites paramsList's values
    """

    #paramsList
    if (paramsList == None or paramsList == []):
      params = 'GridSiteName, GridTier'
    else:
      if type( paramsList ) is not type( [] ):
        paramsList = [ paramsList ]
      params = ','.join( [ x.strip()+' ' for x in paramsList ] )

    #gridSiteName
    if ( gridSiteName == None or gridSiteName == [] ):
      r = "SELECT GridSiteName FROM GridSites"
      resQuery = self.db._query( r )
      if not resQuery[ 'OK' ]:
        raise RSSDBException, where( self, self.getGridSitesList )+resQuery[ 'Message' ]
      if not resQuery[ 'Value' ]:
        gridSiteName = []
      gridSiteName = [ x[0] for x in resQuery['Value'] ]
      gridSiteName = ','.join( [ '"'+x.strip()+'"' for x in gridSiteName ] )
    else:
      if type( gridSiteName ) is not type( [] ):
        gridSiteName = [ gridSiteName ]
      gridSiteName = ','.join( [ '"'+x.strip()+'"' for x in gridSiteName ] )

    #gridTier
    if ( gridTier == None or gridTier == [] ):
      gridTier = ValidSiteType
    else:
      if type( gridTier ) is not type([]):
        gridTier = [ gridTier ]
    gridTier = ','.join( [ '"'+x.strip()+'"' for x in gridTier ] )

    #query construction
    req = "SELECT %s FROM GridSites WHERE" %( params )
    if gridSiteName != [] and gridSiteName != None and gridSiteName is not None and gridSiteName != '':
      req = req + " GridSiteName IN (%s) " %( gridSiteName )
    req = req + " AND GridTier IN (%s)" % ( gridTier )

    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getGridSitesList )+resQuery[ 'Message' ]

    if not resQuery[ 'Value' ]:
      return S_OK( [] )
    list_ = [ x for x in resQuery[ 'Value' ] ]
    return S_OK( list_ )

  def getGridSiteName( self, granularity, name ):

    self.__validateRes( granularity )

    req = "SELECT GridSiteName FROM %s WHERE %sName = '%s'" %( granularity, granularity, name )

    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getGridSiteName ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return S_OK( [] )

    return S_OK( resQuery[ 'Value' ][ 0 ][ 0 ] )

  '''
  ##############################################################################
  # MISC FUNCTIONS
  ##############################################################################
  '''

  def getGeneralName( self, name, from_element, to_element ):
    """
    Get name of res, of granularity `from_g`, to the name of res with granularity `to_g`

    For a StorageElement, get the Site name, or the Service name, or the Resource name.
    For a Resource, get the Site name, or the Service name.
    For a Service name, get the Site name

    :params:
      :attr:`name`: a string with a name

      :attr:`from_g`: a string with a valid granularity
      (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`)

      :attr:`to_g`: a string with a valid granularity
      (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`)

    :return:
      a string with the resulting name
    """

    self.__validateRes( from_element )
    self.__validateRes( to_element )

    if from_element == 'Service':
      resQuery = self.__getElements( from_element, { 'ServiceName' : name }, 'SiteName' )

    elif from_element == 'Resource':
      resQuery = self.__getElements( from_element, { 'ResourceName' : name }, 'ServiceType' )
      if not resQuery[ 'OK' ]:
        raise RSSDBException, where( self, self.getGeneralName ) + resQuery[ 'Message' ]
      serviceType = resQuery[ 'Value' ][ 0 ][ 0 ]

      if serviceType == 'Computing':
        resQuery = self.__getElements( from_element, { 'ResourceName' : name }, 'SiteName' )
      else:
        gridSiteNames = self.__getElements( from_element, { 'ResourceName' : name }, 'GridSiteName' )
        if not gridSiteNames[ 'OK' ]:
          raise RSSDBException, where( self, self.getGeneralName ) + gridSiteNames[ 'Message' ]
        
        resQuery = self.__getElements( 'Sites', {'GridSiteName' : list( gridSiteNames[ 'Value' ] )}, 'SiteName')
        
    elif from_element == 'StorageElement':

      if to_element == 'Resource':
        resQuery = self.__getElements( from_element, { 'StorageElementName' : name }, 'ResourceName' )
      else:
        gridSiteNames = self.__getElements( from_element, { 'StorageElementName' : name }, 'GridSiteName' )
        if not gridSiteNames[ 'OK' ]:
          raise RSSDBException, where( self, self.getGeneralName ) + gridSiteNames[ 'Message' ]
        
        resQuery = self.__getElements( 'Sites', {'GridSiteName' : list( gridSiteNames[ 'Value' ] )}, 'SiteName')

        if to_element == 'Service':
          serviceType = 'Storage'

    else:
      raise ValueError

    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getGeneralName ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return S_OK( [] )

    newNames = [ x[0] for x in resQuery[ 'Value' ] ]

    if to_element == 'Service':
      return S_OK( [ serviceType + '@' + x for x in newNames ] )
    else:
      return S_OK( newNames )

  def getCountries( self, granularity ):
    """
    Get countries of resources in granularity

    :params:
      :attr:`granularity`: string - a ValidRes
    """

    self.__validateRes( granularity )

    if granularity == 'StorageElement':
      granularity = "Site"

    resQuery = self.__getElementRow( granularity, {}, '%sName' % granularity )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getCountries ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return S_OK( None )

    countries = []

    for name in resQuery[ 'Value' ]:
      country = name[0].split('.').pop()
      if country not in countries:
        countries.append( country )

    return S_OK( countries )
  
  def getTokens( self, granularity, name = None, dateExpiration = None, 
                 statusType = None ):
    """
    Get tokens, either by name, those expiring or expired

    :params:
      :attr:`granularity`: a ValidRes

      :attr:`granularity`: optional name of the res

      :attr:`dateExpiration`: optional, datetime.datetime - date from which to consider
    """

    self.__validateRes( granularity )  

    dict = {}

    columns = [ '%sName' % granularity, 'TokenOwner', 'TokenExpiration']
    if name is not None:
      dict[ '%sName' % granularity ] = name
    if statusType is not None:
      self.__validateElementStatusTypes( granularity, statusType )
      dict[ '%sName' % granularity ] = statusType

    resQuery = self.__getElementRow( '%sStatus' % granularity , dict, columns)

#    req = "SELECT %s, TokenOwner, TokenExpiration FROM %s WHERE " % ( DBname, DBtable )
#    if name is not None:
#      req = req + "%s = '%s' " % ( DBname, name )
#      if dateExpiration is not None:
#        req = req + "AND "
#    if dateExpiration is not None:
#      req = req + "TokenExpiration < '%s'" % ( dateExpiration )

#    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.getTokens ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return S_OK( [] )

    if dateExpiration is None:
      tokenList = [ x for x in resQuery[ 'Value' ] ]
    else:
      tokenList = [ x for x in resQuery[ 'Value' ] if x[2] < str( dateExpiration ) ]
    
    return S_OK( tokenList )