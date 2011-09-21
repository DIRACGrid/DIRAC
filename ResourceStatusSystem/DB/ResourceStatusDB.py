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
    ValidResourceType, ValidServiceType, ValidStatusTypes

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
    
    element = element.replace('Status','').replace('History','').replace('Scheduled','').replace('Present','')
    self.__validateRes( element )
                                   
  def __validateElementStatusTypes( self, element, statusTypes ):
    
    if not isinstance( statusTypes, list ):
      statusTypes = [ statusTypes ]
    
    for statusType in statusTypes: 
      if not statusType in ValidStatusTypes[ element ][ 'StatusType' ]:
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
  
        
  def __addRow( self, element, dict ):      

    req = "INSERT INTO %s (" % element 
    req += ','.join( "%s" % key for key in dict.keys())
    req += ") VALUES ("
    req += ','.join( "'%s'" % value for value in dict.values())
    req += ")"   

    resUpdate = self.db._update( req )
    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self.__addElementRow ) + resUpdate[ 'Message' ]
        
  def __addElementRow( self, element, dict ):
    
    self.__validateElementTableName( element )
    self.__addRow( element, dict ) 

  def __addGridRow( self, dict ):
         
    self.__addRow( 'GridSite', dict )

  def __getElement( self, element, cols, whereElements, sort, order, limit ):

    req = "SELECT %s from %s" % ( cols, element )
    if whereElements:
      req += " WHERE %s" % whereElements
    if sort:
      req += " ORDER BY %s" % sort
      if order:
        req += " %s" % order 
    if limit:
      req += " LIMIT %d" % limit     

    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.__getElement ) + resQuery[ 'Message' ]

    return S_OK( [ list(rQ) for rQ in resQuery[ 'Value' ]] )

  def __getElementRow( self, element, dict, columns, 
                       sort = None, order = None, limit = None ):

    self.__validateElementTableName( element )
        
    whereElements = self.__getMultipleWhereElements( dict )    
    cols          = self.__getColumns( columns )
    if sort is not None: 
      sort        = self.__getColumns( sort )  
    if order is not None:
      order       = self.__getColumns( order )   
        
    return self.__getElement( element, cols, whereElements, sort, order, limit )    

  def __getGridElement( self, dict, columns ):
    
    whereElements = self.__getMultipleWhereElements( dict )
    cols          = self.__getColumns( None )
    
    return self.__getElement( 'GridSite', cols, whereElements, None, None, None )

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

  def __updateRow( self, element, dict, uniqueKeys, whereElements ):

    req = "UPDATE %s SET " % element
    req += ','.join( "%s='%s'" % (key,value) for (key,value) in dict.items() if (key not in uniqueKeys) )
    req += " WHERE %s" % whereElements
    
    resUpdate = self.db._update( req )
    if not resUpdate[ 'OK' ]:
      raise RSSDBException, where( self, self.__updateRow ) + resUpdate[ 'Message' ]

  def __updateElementRow( self, element, dict ):

    self.__validateElementTableName( element )
    
    uniqueKeys    = self.__getElementUniqueKeys( element )
    whereElements = self.__getWhereElements( element, dict )
    
    self.__updateRow( element, dict,  uniqueKeys, whereElements )
    
  def __updateGridRow( self, dict ):
    
    uniqueKeys    = [ 'GridSiteName' ]
    whereElements = 'GridSiteName = "%s"' % dict[ 'GridSiteName' ]
    
    self.__updateRow( 'GridSite', dict, uniqueKeys, whereElements )
  
  def __deleteRow( self, element, whereElements ):

    req = "DELETE from %s" % element
    if whereElements is not None:
      req += " WHERE %s" % whereElements
        
    resDel = self.db._update( req )
    if not resDel[ 'OK' ]:
      raise RSSDBException, where( self, self.__deleteRow ) + resDel[ 'Message' ]  
    
  def __deleteElementRow( self, element, dict ):
    
    self.__validateElementTableName( element )
    
    whereElements = self.__getMultipleWhereElements( dict )
    self.__deleteRow(element, whereElements)        
  
  def __addOrModifyElement( self, element, dict ):
    
    self.__validateRes( element )
    
    elemnt = self.__getElementRow( element, 
                                   { 
                                    '%sName' % element : dict[ '%sName' % element ] 
                                    },
                                    '%sName' % element 
                                  )

    if elemnt[ 'Value' ]:
      self.__updateElementRow( element, dict )
    else:
      # If we add a new site, we set the new Site with status 'Banned' 
      self.__addElementRow( element, dict )
      
      defaultStatus  = 'Banned'
      defaultReasons = [ 'Added to DB', 'Init' ]
      tokenOwner     = 'RS_SVC'
      
      setStatus = getattr( self, 'set%sStatus' % element)
       
      # This three lines make not much sense, but sometimes statusToSet is '',
      # and we need it as a list to work properly 
      statusToSet = ValidStatusTypes[ element ][ 'StatusType' ]  
      if not isinstance( statusToSet, list ):
        statusToSet = [ statusToSet ]   
           
      for statusType in statusToSet:
        
        # Trick to populate ElementHistory table with one entry. This allows
        # us to use PresentElement views ( otherwise they do not work ).
        for defaultReason in defaultReasons: 
          setStatus( dict[ '%sName' % element ], statusType, defaultStatus, 
                     defaultReason, tokenOwner )
    
  def __setElementStatus( self, element, dict ):
    
    # START VALIDATION #
    self.__validateRes( element )
    self.__validateElementStatusTypes( element, dict['StatusType'])
    self.__validateStatus( dict['Status'] )
    
    el = self.__getElementRow( element, { '%sName' % element : dict[ '%sName' % element ] }, None )
    if not el[ 'Value' ]:
      message = '%s "%s" does not exist' % ( element, dict[ '%sName' % element ] )
      raise RSSDBException, where( self, self.__setElementStatus ) + message
    
    # END VALIDATION #
    
    currentStatus = self.__getElementRow( '%sStatus' % element, 
                                          {
                                           '%sName' % element : dict[ '%sName' % element ],
                                           'StatusType'       : dict[ 'StatusType' ]
                                           },
                                           None
                                         )
    
    znever = datetime.min
    now    = datetime.utcnow()
    never  = datetime( 9999, 12, 31, 23, 59, 59 ).replace( microsecond = 0 )
    
    dict[ 'TokenExpiration' ] = ( 1 and ( dict.has_key('TokenExpiration') and dict['TokenExpiration'] ) ) or never 
    dict[ 'DateCreated' ]     = ( 1 and ( dict.has_key('DateCreated')     and dict['DateCreated']     ) ) or now
    dict[ 'DateEffective' ]   = ( 1 and ( dict.has_key('DateEffective')   and dict['DateEffective']   ) ) or now
    dict[ 'DateEnd' ]         = ( 1 and ( dict.has_key('DateEnd')         and dict['DateEnd']         ) ) or never
    dict[ 'LastCheckTime' ]   = znever  
            
    if currentStatus[ 'Value' ]:
  
      self.__updateElementRow( '%sStatus' % element , dict )
      
      cS            = currentStatus[ 'Value' ][ 0 ]
      
      dict[ 'Status' ]          = cS[ 3 ]
      dict[ 'Reason' ]          = cS[ 4 ]
      dict[ 'DateCreated' ]     = cS[ 5 ]
      dict[ 'DateEffective' ]   = cS[ 6 ]
      dict[ 'DateEnd' ]         = now # cS[ 7 ]
      dict[ 'LastCheckTime' ]   = cS[ 8 ]
      dict[ 'TokenOwner' ]      = cS[ 9 ]
      dict[ 'TokenExpiration' ] = now # cS[ 10 ]
 
      self.__addElementRow( '%sHistory' % element , dict)
      
    else:
      
      self.__addElementRow( '%sStatus' % element , dict )
    
  def __setElementScheduledStatus( self, element, dict ):

    # START VALIDATION #
    self.__validateRes( element )
    self.__validateElementStatusTypes( element, dict['StatusType'])
    self.__validateStatus( dict['Status'] )
    
    el = self.__getElementRow( element, { '%sName' % element : dict[ '%sName' % element ] }, None )
    if not el[ 'Value' ]:
      message = '%s "%s" does not exist' % ( element, dict[ '%sName' % element ] )
      raise RSSDBException, where( self, self.__setElementStatus ) + message
    # END VALIDATION #

    self.__addElementRow( '%sScheduledStatus' % element , dict )
  
  def __getElements( self, element, dict, columns = None, table = None ):    
    
    # START VALIDATION #
    self.__validateRes( element )
    # END VALIDATION #    
    
    if table is not None:
      element = '%s%s' % ( element, table )
    
    elements = self.__getElementRow( element, dict, columns )
    return elements
    
  def __deleteElements( self, element, dict ):

    # START VALIDATION #
    self.__validateRes( element )
    # END VALIDATION #    
    self.__deleteElementRow( '%sHistory' % element, dict)
    self.__deleteElementRow( '%sScheduledStatus' % element, dict)
    self.__deleteElementRow( '%sStatus' % element, dict)
    self.__deleteElementRow( '%s' % element, dict)

  def __deleteElementsScheduledStatus( self, element, dict ):
    # START VALIDATION #
    self.__validateRes( element )
    # END VALIDATION #    
    self.__deleteElementRow( '%sScheduledStatus' % element, dict)
    
  '''    
  ##############################################################################
  # SITE FUNCTIONS
  ##############################################################################
  '''

  def addOrModifySite( self, siteName, siteType, gridSiteName ):
  
    rDict = self.__generateRowDict( locals() )
    
    self.__validateSiteType( siteType ) 
    self.__addOrModifyElement( 'Site', rDict )
    
  def setSiteStatus( self, siteName, statusType, status, reason, tokenOwner, 
                     tokenExpiration = None, dateCreated = None, 
                     dateEffective = None, dateEnd = None, lastCheckTime = None ):
    
    rDict = self.__generateRowDict( locals() )
    self.__setElementStatus( 'Site', rDict )

  def setSiteScheduledStatus( self, siteName, statusType, status, reason, tokenOwner, 
                              tokenExpiration = None, dateCreated = None, 
                              dateEffective = None, dateEnd = None, lastCheckTime = None):

    rDict = self.__generateRowDict( locals() )
    self.__setElementScheduledStatus( 'Site', rDict )


  def getSites( self, siteName = None, siteType = None, gridSiteName = None ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Site', rDict )

  def getSitesStatus( self, siteName = None, statusType = None, status = None, 
                      reason = None, tokenOwner = None, tokenExpiration = None, 
                      dateCreated = None, dateEffective = None, dateEnd = None, 
                      lastCheckTime = None ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Site', rDict, table = 'Status' )

  def getSitesHistory( self, siteName = None, statusType = None, status = None, 
                       reason = None, tokenOwner = None, tokenExpiration = None, 
                       dateCreated = None, dateEffective = None, dateEnd = None, 
                       lastCheckTime = None ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Site', rDict, table = 'History' )

  def getSitesScheduledStatus( self, siteName = None, statusType = None, 
                               status = None, reason = None, tokenOwner = None, 
                               tokenExpiration = None, dateCreated = None, 
                               dateEffective = None, dateEnd = None, 
                               lastCheckTime = None):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Site', rDict, table = 'ScheduledStatus' )

  def deleteSites( self, siteName ):
    
    rDict = self.__generateRowDict( locals() )  
    self.__deleteElements( 'Site', rDict)

  def deleteSitesScheduledStatus( self, siteName = None, statusType = None, 
                                  status = None, reason = None, tokenOwner = None, 
                                  tokenExpiration = None, dateCreated = None, 
                                  dateEffective = None, dateEnd = None, 
                                  lastCheckTime = None):
    
    rDict = self.__generateRowDict( locals() )
    self.__deleteElementsScheduledStatus( 'Site', rDict )

  '''
  ##############################################################################
  # SERVICE FUNCTIONS
  ##############################################################################
  '''

  def addOrModifyService( self, serviceName, serviceType, siteName ):
 
    rDict = self.__generateRowDict( locals() )
    
    # START VALIDATION #
    self.__validateServiceType( serviceType )
    
#    site = self.__getElementRow( 'Site', { 'SiteName' : siteName }, 'SiteName' )
#    if not site[ 'Value' ]:
#      message = '"%s" is not a known siteName' % siteName
#      raise RSSDBException, where( self, self.addOrModifyService ) + message
    # END VALIDATION #    
    
    self.__addOrModifyElement( 'Service', rDict)
       
  def setServiceStatus( self, serviceName, statusType, status, reason, tokenOwner, 
                        tokenExpiration = None, dateCreated = None, 
                        dateEffective = None, dateEnd = None, lastCheckTime = None ):

    rDict = self.__generateRowDict( locals() )
    self.__setElementStatus( 'Service', rDict )

  def setServiceScheduledStatus( self, serviceName, statusType, status, reason, tokenOwner, 
                        tokenExpiration = None, dateCreated = None, 
                        dateEffective = None, dateEnd = None, lastCheckTime = None ):

    rDict = self.__generateRowDict( locals() )
    self.__setElementScheduledStatus( 'Service', rDict )

  def getServices( self, serviceName = None, serviceType = None, siteName = None ):
    
    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Service', rDict )

  def getServicesStatus( self, serviceName = None, statusType = None, status = None, 
                         reason = None, tokenOwner = None, tokenExpiration = None, 
                         dateCreated = None, dateEffective = None, dateEnd = None, 
                         lastCheckTime = None ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Service', rDict, table = 'Status' )

  def getServicesHistory( self, serviceName = None, statusType = None, status = None, 
                         reason = None, tokenOwner = None, tokenExpiration = None, 
                         dateCreated = None, dateEffective = None, dateEnd = None, 
                         lastCheckTime = None ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Service', rDict, table = 'History' )

  def getServicesScheduledStatus( self, serviceName = None, statusType = None, 
                               status = None, reason = None, tokenOwner = None, 
                               tokenExpiration = None, dateCreated = None, 
                               dateEffective = None, dateEnd = None, lastCheckTime = None):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Service', rDict, table = 'ScheduledStatus' )

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
    
  def deleteServices( self, serviceName ):
    
    rDict = self.__generateRowDict( locals() )  
    self.__deleteElements( 'Service', rDict)

  def deleteServicesScheduledStatus( self, serviceName = None, statusType = None, 
                                     status = None, reason = None, tokenOwner = None, 
                                     tokenExpiration = None, dateCreated = None, 
                                     dateEffective = None, dateEnd = None, lastCheckTime = None):

    rDict = self.__generateRowDict( locals() )
    self.__deleteElementsScheduledStatus( 'Service', rDict )

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
    
#   Check commented. Some Resources are not assigned to a site    
#    site = self.__getElementRow( 'Site', {'SiteName' : siteName }, 'SiteName' )
#    if not site[ 'Value' ]:
#      message = '"%s" is not a known siteName' % siteName
#      raise RSSDBException, where( self, self.addOrModifyService ) + message
    
#    gridSite = self.getGridSitesList( gridSiteName = gridSiteName )
#    if not gridSite[ 'OK' ]:
#      raise RSSDBException, where( self, self.addOrModifySite ) + gridSite[ 'Message' ]
#    if not gridSite[ 'Value' ]:
#      message = '%s is not a known gridSiteName' % gridSiteName
#      raise RSSDBException, where( self, self.addOrModifySite ) + message 
    # END VALIDATION #    
    
    self.__addOrModifyElement( 'Resource', rDict )
  
  def setResourceStatus( self, resourceName, statusType, status, reason, tokenOwner, 
                         tokenExpiration = None, dateCreated = None, 
                         dateEffective = None, dateEnd = None, lastCheckTime = None ):
    
    rDict = self.__generateRowDict( locals() )
    self.__setElementStatus( 'Resource', rDict )

  def setResourceScheduledStatus( self, resourceName, statusType, status, reason, 
                                  tokenOwner, tokenExpiration = None, dateCreated = None, 
                                  dateEffective = None, dateEnd = None, lastCheckTime = None ):
    
    rDict = self.__generateRowDict( locals() )
    self.__setElementScheduledStatus( 'Resource', rDict )
  
  def getResources( self, resourceName = None, resourceType = None, 
                    serviceType = None, siteName = None, gridSiteName = None ):
    
    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Resource', rDict )

  def getResourcesStatus( self, resourceName = None, statusType = None, status = None,
                          reason = None, tokenOwner = None, tokenExpiration = None, 
                          dateCreated = None, dateEffective = None, dateEnd = None, 
                          lastCheckTime = None ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Resource', rDict, table = 'Status' )

  def getResourcesHistory( self, resourceName = None, statusType = None, status = None,
                           reason = None, tokenOwner = None, tokenExpiration = None, 
                           dateCreated = None, dateEffective = None, dateEnd = None, 
                           lastCheckTime = None ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Resource', rDict, table = 'History' )

  def getResourcesScheduledStatus( self, resourceName = None, statusType = None, status = None,
                                  reason = None, tokenOwner = None, tokenExpiration = None, 
                                  dateCreated = None, dateEffective = None, dateEnd = None, 
                                  lastCheckTime = None):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Resource', rDict, table = 'ScheduledStatus' )  
  
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

  def deleteResourcesScheduledStatus( self, resourceName = None, statusType = None, 
                                      status = None, reason = None, tokenOwner = None, 
                                      tokenExpiration = None, dateCreated = None, 
                                      dateEffective = None, dateEnd = None, 
                                      lastCheckTime = None):

    rDict = self.__generateRowDict( locals() )
    self.__deleteElementsScheduledStatus( 'Resource', rDict )

  '''
  ##############################################################################
  # STORAGE ELEMENT FUNCTIONS
  ##############################################################################
  '''

  def addOrModifyStorageElement( self, storageElementName, resourceName, 
                                 gridSiteName ):
    
    rDict = self.__generateRowDict( locals() )
    
    # START VALIDATION #
    
#    resource = self.__getElementRow( 'Resource', {'ResourceName' : resourceName }, 'ResourceName' )
#    if not resource[ 'Value' ]:
#      message = '"%s" is not a known resourceName' % resourceName
#      raise RSSDBException, where( self, self.addOrModifyResource ) + message
    
#    gridSite = self.getGridSitesList( gridSiteName = gridSiteName )
#    if not gridSite[ 'Value' ]:
#      message = '%s is not a known gridSiteName' % gridSiteName
#      raise RSSDBException, where( self, self.addOrModifySite ) + message 
    # END VALIDATION #    
    
    self.__addOrModifyElement( 'StorageElement', rDict)
  
  def setStorageElementStatus( self, storageElementName, statusType, status, 
                               reason, tokenOwner, tokenExpiration = None, 
                               dateCreated = None, dateEffective = None, dateEnd = None, 
                               lastCheckTime = None ):
    
    rDict = self.__generateRowDict( locals() )
    self.__setElementStatus( 'StorageElement', rDict )

  def setStorageElementScheduledStatus( self, storageElementName, statusType, status, 
                                        reason, tokenOwner, tokenExpiration = None, 
                                        dateCreated = None, dateEffective = None, 
                                        dateEnd = None, lastCheckTime = None ):
    
    rDict = self.__generateRowDict( locals() )
    self.__setElementScheduledStatus( 'StorageElement', rDict )

  def getStorageElements( self, storageElementName = None, resourceName = None, 
                          gridSiteName = None ):
    
    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'StorageElement', rDict )

  def getStorageElementsStatus( self, storageElementName = None, statusType = None, 
                                status = None, reason = None, tokenOwner = None, 
                                tokenExpiration = None, dateCreated = None, 
                                dateEffective = None, dateEnd = None, lastCheckTime = None ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'StorageElement', rDict, table = 'Status' )

  def getStorageElementsHistory( self, storageElementName = None, statusType = None, 
                                 status = None, reason = None, tokenOwner = None, 
                                 tokenExpiration = None, dateCreated = None, 
                                 dateEffective = None, dateEnd = None, lastCheckTime = None ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'StorageElement', rDict, table = 'History' )

  def getStorageElementsScheduledStatus( self, storageElementName = None, statusType = None, 
                                         status = None, reason = None, tokenOwner = None, 
                                         tokenExpiration = None, dateCreated = None, 
                                         dateEffective = None, dateEnd = None, lastCheckTime = None):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'StorageElement', rDict, table = 'ScheduledStatus' )  
  
#  def getStorageElement( self, storageElementName ):
#    
#    rDict     = self.__generateRowDict( locals() )
#    return self.__getElements( 'StorageElementName', rDict )
  
#  def getStorageElementStatus( self, storageElementName, statusType  ):
#    
#    rDict           = self.__generateRowDict( locals() )   
#    return self.__getElements( 'StorageElement', rDict, table = 'Status') 

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

    for x in count[ 'Value' ]:
      res[x[0]] = int(x[1])

    res['Total'] = sum( res.values() )

    return S_OK( res )   
    
  def deleteStorageElements( self, storageElementName ):
    
    rDict = self.__generateRowDict( locals() )  
    self.__deleteElements( 'StorageElement', rDict)

  def deleteStorageElementsScheduledStatus( self, storageElementName = None, statusType = None, 
                                            status = None, reason = None, tokenOwner = None, 
                                            tokenExpiration = None, dateCreated = None, 
                                            dateEffective = None, dateEnd = None, 
                                            lastCheckTime = None ):

    rDict = self.__generateRowDict( locals() )
    self.__deleteElementsScheduledStatus( 'StorageElement', rDict )

  '''
  ##############################################################################
  # GRID SITE FUNCTIONS
  ##############################################################################
  '''

  def addOrModifyGridSite( self, gridSiteName, gridTier ):
    """
    Add or modify a Grid Site to the GridSites table.

    :params:
      :attr:`name`: string - name of the site in GOC DB

      :attr:`tier`: string - tier of the site
    """

    self.__validateSiteType( gridTier )

    rDict = self.__generateRowDict( locals() )

    gridSite = self.__getGridElement( { 'GridSiteName' : gridSiteName }, None )
    
    if gridSite[ 'Value' ]:
      self.__updateGridRow( rDict )
    else:
      self.__addGridRow( rDict )
  
  def getGridSitesList( self, gridSiteName = None, gridTier = None ):
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
   
    rDict = self.__generateRowDict( locals() )
   
#    dict = { 
#            'GridSiteName' : gridSiteName,
#            'GridTier'     : gridTier                                                      
#           } 
    
#    resQuery = self.__getGridElement( dict, paramsList )

    resQuery = self.__getGridElement( rDict )

    if not resQuery[ 'Value' ]:
      return S_OK( [] )

    resList = [ x for x in resQuery[ 'Value' ] ]
    return S_OK( resList )

  def getGridSiteName( self, granularity, name ):

    self.__validateRes( granularity )

    resQuery = self.__getElements( granularity, 
                        { '%sName' % granularity : name }, 
                        [ 'GridSiteName' ] )

#    req = "SELECT GridSiteName FROM %s WHERE %sName = '%s'" %( granularity, granularity, name )

#    resQuery = self.db._query( req )
#    if not resQuery[ 'OK' ]:
#      raise RSSDBException, where( self, self.getGridSiteName ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return S_OK( [] )

    return S_OK( resQuery[ 'Value' ][ 0 ][ 0 ] )

  def deleteGridSiteName( self, gridSiteName ):
    
    whereElements = self.__getMultipleWhereElements( { 'GridSiteName' : gridSiteName })
    self.__deleteRow( 'GridSite', whereElements )

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
      resQuery = self.__getElements( from_element, { '%sName' % from_element : name }, 'SiteName' )

    elif from_element == 'Resource':
      resQuery = self.__getElements( from_element, { '%sName' % from_element : name }, 'ServiceType' )
      serviceType = resQuery[ 'Value' ][ 0 ][ 0 ]

      if serviceType == 'Computing':
        resQuery = self.__getElements( from_element, { '%sName' % from_element : name }, 'SiteName' )
      else:
        gridSiteNames = self.__getElements( from_element, { '%sName' % from_element : name }, 'GridSiteName' )
        
        resQuery = self.__getElements( 'Sites', {'GridSiteName' : list( gridSiteNames[ 'Value' ] )}, 'SiteName')
        
    elif from_element == 'StorageElement':

      if to_element == 'Resource':
        resQuery = self.__getElements( from_element, { 'StorageElementName' : name }, 'ResourceName' )
      else:
        gridSiteNames = self.__getElements( from_element, { 'StorageElementName' : name }, 'GridSiteName' )
        
        resQuery = self.__getElements( 'Sites', {'GridSiteName' : list( gridSiteNames[ 'Value' ] )}, 'SiteName')

        if to_element == 'Service':
          serviceType = 'Storage'

    else:
      raise ValueError

    if not resQuery[ 'Value' ]:
      return S_OK( [] )

    newNames = [ x[0] for x in resQuery[ 'Value' ] ]

    if to_element == 'Service':
      return S_OK( [ serviceType + '@' + x for x in newNames ] )
      #return S_OK( [ serviceType + '@' + x[0] for x in resQuery[ 'Value' ] ] )
    else:
      return S_OK( newNames )
      #return resQuery
      

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

    if not resQuery[ 'Value' ]:
      return S_OK( [] )

    if dateExpiration is None:
      tokenList = [ x for x in resQuery[ 'Value' ] ]
    else:
      tokenList = [ x for x in resQuery[ 'Value' ] if x[2] < str( dateExpiration ) ]
    
    return S_OK( tokenList )
  
  def setToken( self, granularity, name, reason, newTokenOwner, dateExpiration, statusType ):
    """
    (re)Set token properties.
    """

    self.__validateRes(granularity)
    self.__validateElementStatusTypes(granularity, statusType )

    rDict = {
             '%sName'     : name,
             'StatusType' : statusType
             }
    
    elementStatus = self.__getElementRow( '%sStatus' % granularity, rDict, 'Status')

    if not elementStatus[ 'Value' ]:
      message = 'Not found entry with name %s and type %s of granularity %s' % ( name, statusType, granularity )
      raise RSSDBException, where( self, self.setToken ) + message
    
    status = elementStatus[ 'Value' ][ 0 ]
    
    tokenSetter = getattr( self, 'set%sStatus' % granularity )
    tokenSetter( name, statusType, status, reason, newTokenOwner, tokenExpiration = dateExpiration,
                 dateEnd = dateExpiration )
    
  def whatIs( self, name ):
    """
    Find which is the granularity of name.
    """

    for g in ValidRes:
      
      resQuery = self.__getElementRow( g, { '%sName' % g : name }, '%sName' % g )

      if not resQuery[ 'Value' ]:
        continue
      else:
        return S_OK( g )

    return S_OK( 'Unknown' )    
    
  def getStuffToCheck( self, granularity, checkFrequency = None, maxN = None, name = None ):
    """
    Get Sites, Services, Resources, StorageElements to be checked using Present-x views.

    :params:
      :attr:`granularity`: a ValidRes

      :attr:`checkFrequecy': dictonary. Frequency of active sites/resources checking in minutes.

      :attr:`maxN`: integer - maximum number of lines in output
    """

    self.__validateRes( granularity )

    if checkFrequency is not None:

      now = datetime.datetime.utcnow().replace(microsecond = 0)
      toCheck = {}

      for name, freq in checkFrequency.items():
        toCheck[ name ] = ( now - datetime.timedelta(minutes=freq)).isoformat(' ')

    if granularity == 'Site':
      cols = ['SiteName', 'StatusType', 'Status', 'FormerStatus', 'SiteType', 'TokenOwner']
      #req = "SELECT SiteName, Status, FormerStatus, SiteType, TokenOwner FROM PresentSites"
    elif granularity == 'Service':
      cols = [ 'ServiceName', 'StatusType', 'Status', 'FormerStatus', 'SiteType', 'ServiceType', 'TokenOwner' ]
      #req = "SELECT ServiceName, Status, FormerStatus, SiteType, ServiceType, TokenOwner FROM PresentServices"
    elif granularity == 'Resource':
      cols = [ 'ResourceName', 'StatusType', 'Status', 'FormerStatus', 'SiteType', 'ResourceType', 'TokenOwner' ]
      #req = "SELECT ResourceName, Status, FormerStatus, SiteType, ResourceType, TokenOwner FROM PresentResources"
    elif granularity == 'StorageElement':
      cols = [ 'StorageElementName', 'StatusType', 'Status', 'FormerStatus', 'SiteType', 'TokenOwner' ]
      #req = "SELECT StorageElementName, Status, FormerStatus, SiteType, TokenOwner FROM PresentStorageElements"

    whereElements = ""

    if name is None:
      if checkFrequency is not None:

#        req = req + " WHERE"
        
        for k,v in toCheck.items():
          
          siteType, status = k.replace( '_CHECK_FREQUENCY', '' ).split( '_' )
          status = status[0] + status[1:].lower()
          whereElements += " (Status = '%s' AND SiteType = '%s' AND LastCheckTime < '%s') OR" %( status, siteType, v )
        
        # Remove the last OR
        whereElements = whereElements[:-2] + " ORDER BY LastCheckTime"
                
    else:
#      req = req + " WHERE"

      whereElements = self.__getMultipleWhereElements( { '%sName' % granularity : name } )

#      if granularity == 'Site':
#        req = req + " SiteName = '%s'" %name
#      elif granularity == 'Service':
#        req = req + " ServiceName = '%s'" %name
#      elif granularity == 'Resource':
#        req = req + " ResourceName = '%s'" %name
#      elif granularity == 'StorageElement':
#        req = req + " StorageElementName = '%s'" %name

    if maxN != None:
      whereElements = whereElements + " LIMIT %d" %maxN

    resQuery = self.__getElement( granularity , cols, whereElements )

    #resQuery = self.db._query( req )
    #if not resQuery[ 'OK' ]:
    #  raise RSSDBException, where( self, self.getStuffToCheck ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return S_OK( [] )

    stuffList = [ x for x in resQuery[ 'Value' ]]

    return S_OK( stuffList )   
    
  def getMonitoredsHistory( self, granularity, paramsList = None, name = None,
                            presentAlso = True, order = 'ASC', limit = None ):
    """
    Get history of sites / services / resources / storageElements in a list
    (a site name can be specified)

    :params:
      :attr:`granularity`: a ValidRes

      :attr:`paramsList`: A list of parameters can be entered. If not, a custom list is used.

      :attr:`name`: list of strings. If not given, fetches the complete list
    """

    self.__validateRes( granularity )

    dict = {}
    if name is not None:
      dict[ '%sName' % granularity ] = name
    
    if order not in ['ASC','DESC']:
      message = '"%s" is not a valid order' % order
      raise RSSDBException, where( self, self.getMonitoredsHistory ) + message
    
    resQuery = self.__getElementRow( '%sHistory' % granularity, dict, paramsList, 
                                     ['%sName' % granularity, '%sHistoryID' % granularity], 
                                     order, limit )

    elements = resQuery[ 'Value' ]

    if not elements:
      return S_OK( elements )
    
    if presentAlso:
      resQuery = self.__getElementRow( '%sStatus' % granularity, dict, paramsList, 
                                       ['%sName' % granularity, '%sStatusID' % granularity], 
                                       order, limit )
      pElements = resQuery[ 'Value' ]
      if not pElements:
        return S_OK( pElements )

      elements += pElements

    return S_OK( elements )    
  
  def getMonitoredsList( self, granularity, paramsList = None, siteName = None,
                         serviceName = None, resourceName = None, storageElementName = None,
                         statusType = None,
                         status = None, siteType = None, resourceType = None,
                         serviceType = None, #countries = None, 
                         gridSiteName = None ):
    """
    Get Present Sites /Services / Resources / StorageElements lists.

    :params:
      :attr:`granularity`: a ValidRes

      :attr:`paramsList`: a list of parameters can be entered. If not given,
      a custom list is used.

      :attr:`siteName`, `serviceName`, `resourceName`, `storageElementName`:
      a string or a list representing the site/service/resource/storageElement name.
      If not given, fetch all.

      :attr:`status`: a string or a list representing the status. If not given, fetch all.

      :attr:`siteType`: a string or a list representing the site type.
      If not given, fetch all.

      :attr:`serviceType`: a string or a list representing the service type.
      If not given, fetch all.

      :attr:`resourceType`: a string or a list representing the resource type.
      If not given, fetch all.

      :attr:`countries`: a string or a list representing the countries extensions.
      If not given, fetch all.

      :attr:`gridSiteName`: a string or a list representing the grid site name.
      If not given, fetch all.

      See :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils` for these parameters.

    :return:
      list of monitored paramsList's values
    """

    self.__validateRes( granularity )

    if ( paramsList == None or paramsList == [] ):
      paramsList = [ '%sName' % granularity, 'StatusType', 'Status', 'FormerStatus', 'DateEffective', 'LastCheckTime' ]

    dict = {}
    if siteName is not None:
      dict[ 'SiteName']           = siteName
    if serviceName is not None:
      dict[ 'ServiceName']        = serviceName
    if resourceName is not None:
      dict[ 'ResourceName']       = resourceName
    if storageElementName is not None:
      dict[ 'storageElementName'] = storageElementName
    if statusType is not None:
      dict[ 'StatusType' ]        = statusType
    if status is not None:
      dict[ 'Status' ]            = status
    if siteType is not None:
      dict[ 'SiteType']           = siteType  
    if serviceType is not None:
      dict[ 'ServiceType']        = serviceType
    if resourceType is not None:
      dict[ 'ResourceType']       = resourceType   
    if gridSiteName is not None:
      dict[ 'GridSiteName']       = gridSiteName      
   
    resQuery = self.__getElementRow( 'Present%s' % granularity, dict, paramsList )   
    if not resQuery[ 'Value' ]:
      return S_OK( [] )
    
    return resQuery

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    