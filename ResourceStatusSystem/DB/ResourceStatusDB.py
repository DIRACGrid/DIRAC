"""
The ResourcesStatusDB module contains a couple of exception classes, and a
class to interact with the ResourceStatus DB.
"""

from datetime import datetime, timedelta

from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getDIRACSiteName

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException, InvalidRes, InvalidStatus

from DIRAC.ResourceStatusSystem.Utilities.Utils import where, convertTime
from DIRAC.ResourceStatusSystem import ValidRes, ValidStatus, ValidSiteType, \
    ValidResourceType, ValidServiceType, ValidStatusTypes

import types

################################################################################

class RSSDBException( RSSException ):
  """
  DB exception
  """

  def __init__( self, message = "" ):
    self.message = message
    RSSException.__init__( self, message )

  def __str__( self ):
    return "Exception in the RSS DB: " + repr( self.message )

################################################################################

class NotAllowedDate( RSSException ):
  """
  Exception that signals a not allowed date
  """

  def __init__( self, message = "" ):
    self.message = message
    RSSException.__init__( self, message )

  def __str__( self ):
    return "Not allowed date in the RSS DB: " + repr( self.message )

################################################################################

'''
  Decorator that try / catches all API functions ( the ones expossed on the
  client) and makes a reasonable Exception handling.
'''
class CheckExecution( object ):
  
  def __init__( self, f ):
    self.f = f
    
  def __get__( self, obj, objtype=None ):
    return types.MethodType( self, obj, objtype ) 
    
  def __call__( self, *args, **kwargs ):
    try:
      return self.f( *args, **kwargs )     
    except Exception, x:
      return S_ERROR( x )
        
################################################################################

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
      elements = [ '%sName' % element.replace( 'Status', '' )]
      if dict.has_key( 'StatusType' ):
        elements.append( 'StatusType' )
    elif element.replace( 'History', '') in ValidRes:
      elements = [ '%sName' % element.replace( 'History', '' ), 'DateEnd' ]#'StatusType', 'DateEnd' ]
      if dict.has_key( 'StatusType' ):
        elements.append( 'StatusType' )     
    else:
      message = '%s is a wrong element' % element
      raise RSSDBException, where( self, self.__getWhereElements ) + message

    newDict = {}
    for el in elements:
      newDict[ el ] = dict[ el ]
      
    whereElements = self.__getMultipleWhereElements( newDict )

#    whereElements = ' AND '.join("%s='%s'" % ( el, dict[el] ) for el in elements) 
    return whereElements 
   
  def __getMultipleWhereElements( self, dict, **kwargs ):
   
    items = []

    for k,v in dict.items():
      if v is None:
        pass
      elif isinstance( v, list ):
        if len(v) > 1:
          items.append( '%s IN %s' % ( k, tuple(v) ) )
        elif len(v):
          items.append( "%s='%s'" % ( k, v[0] ) )
        else:
          raise NameError( dict )      
      else:
        items.append( "%s='%s'" % ( k, v ) )
                
    if kwargs.has_key( 'minor' ):
      for k,v in kwargs[ 'minor' ].items():
        if v is not None:  
          items.append( "%s < '%s'" % ( k, v ) )                  
                
    whereElements = ' AND '.join( item for item in items ) 
    return whereElements 
             
  def __getElementUniqueKeys( self, element ):        
        
    if element in ValidRes:    
      elements = [ '%sName' % element ]      
    elif element.replace( 'Status', '' ) in ValidRes:    
      elements = [ '%sName' % element.replace( 'Status', '' ), 'StatusType']
    elif element.replace( 'History', '') in ValidRes:
      elements = [ '%sName' % element.replace( 'History', '' ), 'StatusType', 'DateEnd' ] 
    else:
      message = '%s is a wrong element' % element
      raise RSSDBException, where( self, self.__getElementUniqueKeys ) + message

    return elements

  def __generateRowDict( self, dict ):

    rDict = {}
    for k,v in dict.items():
      if k not in ['self', 'dict', 'k', 'v', 'rDict', 'kwargs' ]:
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

  def __getElementRow( self, element, dict, **kwargs ):
                       #sort = None, order = None, limit = None ):

    self.__validateElementTableName( element )
        
    # PARAMS PROCESSED FROM KWARGS !!!    
    sort    = kwargs.pop( 'sort',    None )    
    order   = kwargs.pop( 'order',   None )
    limit   = kwargs.pop( 'limit',   None )
    columns = kwargs.pop( 'columns', None)
        
    whereElements = self.__getMultipleWhereElements( dict, **kwargs )    
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
        
    req = "SELECT Status, COUNT(*) from Present%s" % element
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
    
  def __deleteElementRow( self, element, dict, **kwargs ):
    
    self.__validateElementTableName( element )
    
    whereElements = self.__getMultipleWhereElements( dict, **kwargs )
    self.__deleteRow(element, whereElements)        
  
  def __addOrModifyElement( self, element, dict ):
    
    self.__validateRes( element )
    
    kwargs = { 'columns' : [ '%sName' % element ] }
    elemnt = self.__getElementRow( element, 
                                   { 
                                    '%sName' % element : dict[ '%sName' % element ] 
                                    },
                                    **kwargs
                                    #: '%sName' % element 
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
    
    el = self.__getElementRow( element, { '%sName' % element : dict[ '%sName' % element ] } )#, None )
    if not el[ 'Value' ]:
      message = '%s "%s" does not exist' % ( element, dict[ '%sName' % element ] )
      raise RSSDBException, where( self, self.__setElementStatus ) + message
    
    # END VALIDATION #
    
    currentStatus = self.__getElementRow( '%sStatus' % element, 
                                          {
                                           '%sName' % element : dict[ '%sName' % element ],
                                           'StatusType'       : dict[ 'StatusType' ]
                                           } )#,
                                           #None
                                         #)
    
    znever = datetime.min
    now    = datetime.utcnow()
    never  = datetime( 9999, 12, 31, 23, 59, 59 ).replace( microsecond = 0 )
    
    dict[ 'TokenExpiration' ] = ( 1 and ( dict.has_key('TokenExpiration') and dict['TokenExpiration'] ) ) or never 
    dict[ 'DateCreated' ]     = ( 1 and ( dict.has_key('DateCreated')     and dict['DateCreated']     ) ) or now
    dict[ 'DateEffective' ]   = ( 1 and ( dict.has_key('DateEffective')   and dict['DateEffective']   ) ) or now
    dict[ 'DateEnd' ]         = ( 1 and ( dict.has_key('DateEnd')         and dict['DateEnd']         ) ) or never
    dict[ 'LastCheckTime' ]   = now #znever  
            
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
      dict[ 'TokenExpiration' ] = cS[ 10 ] # now
 
      self.__addElementRow( '%sHistory' % element , dict)
      
    else:
      
      self.__addElementRow( '%sStatus' % element , dict )

  def __updateElementStatus( self, element, dict ):

    now = datetime.utcnow()

    # START VALIDATION #
    self.__validateRes( element )
#    self.__validateElementStatusTypes( element, dict['StatusType'])

    el = self.__getElementRow( element, { '%sName' % element : dict[ '%sName' % element ] } )#, None )
    if not el[ 'Value' ]:
      message = '%s "%s" does not exist' % ( element, dict[ '%sName' % element ] )
      raise RSSDBException, where( self, self.__updateElementStatus ) + message
    # END VALIDATION #

    rDict = { '%sName' % element : dict[ '%sName' % element ] }
    if dict.has_key( 'StatusType' ):
      rDict[ 'StatusType' ] = dict[ 'StatusType' ]
           
#    dict[ 'DateEffective' ] = now       
    dict[ 'LastCheckTime'] = now
           
#    if not currentStatus[ 'Value' ]:
#      message = '%s %s has no status of type %s to be updated' % ( element, dict[ '%sName' % element ], dict[ 'StatusType' ] )
#      raise RSSDBException, where( self, self.__updateElementStatus ) + message

    self.__updateElementRow( '%sStatus' % element, dict )

    currentStatus = self.__getElementRow( '%sStatus' % element, dict )#, None )
    for cSs in currentStatus[ 'Value' ]:
    
      rDict[ '%sName' % element ] = cSs[ 1 ]
      rDict[ 'Status' ]           = cSs[ 3 ]
      rDict[ 'Reason' ]           = cSs[ 4 ]
      rDict[ 'DateCreated' ]      = cSs[ 5 ]
      rDict[ 'DateEffective' ]    = cSs[ 6 ]
      rDict[ 'DateEnd' ]          = now # cSs[ 7 ]
      rDict[ 'LastCheckTime' ]    = cSs[ 8 ]
      rDict[ 'TokenOwner' ]       = cSs[ 9 ]
      rDict[ 'TokenExpiration' ]  = cSs[ 10 ]    
      
      #We store any modification on the Status
      self.__addElementRow( '%sHistory' % element , rDict )
    
  def __setElementScheduledStatus( self, element, dict ):

    # START VALIDATION #
    self.__validateRes( element )
    self.__validateElementStatusTypes( element, dict['StatusType'])
    self.__validateStatus( dict['Status'] )
    
    el = self.__getElementRow( element, { '%sName' % element : dict[ '%sName' % element ] } )#, None )
    if not el[ 'Value' ]:
      message = '%s "%s" does not exist' % ( element, dict[ '%sName' % element ] )
      raise RSSDBException, where( self, self.__setElementStatus ) + message
    # END VALIDATION #

    self.__addElementRow( '%sScheduledStatus' % element , dict )
  
  def __getElements( self, element, dict, table = None, **kwargs ):    
    
    # START VALIDATION #
    self.__validateRes( element )
    # END VALIDATION #    
    
    if table is not None:  
      element = '%s%s' % ( element, table )
    
    elements = self.__getElementRow( element, dict, **kwargs )
    return elements
    
  def __deleteElements( self, element, dict ):

    # START VALIDATION #
    self.__validateRes( element )
    # END VALIDATION #    
    self.__deleteElementRow( '%sHistory' % element,         dict)
    self.__deleteElementRow( '%sScheduledStatus' % element, dict)
    self.__deleteElementRow( '%sStatus' % element,          dict)
    self.__deleteElementRow( element,                       dict)

  def __deleteElementHistory( self, element, dict, **kwargs ):
    
    # START VALIDATION #
    self.__validateRes( element )
    # END VALIDATION #    
    self.__deleteElementRow( '%sHistory' % element, dict, **kwargs )
    
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

  @CheckExecution
  def addOrModifySite( self, siteName, siteType, gridSiteName ):
  
    rDict = self.__generateRowDict( locals() )
    
    self.__validateSiteType( siteType ) 
    self.__addOrModifyElement( 'Site', rDict )
    return S_OK()   
  
  @CheckExecution  
  def setSiteStatus( self, siteName, statusType, status, reason, tokenOwner, 
                     tokenExpiration, dateCreated, dateEffective, dateEnd, 
                     lastCheckTime ):
#  def setSiteStatus( self, siteName, statusType, status, reason, tokenOwner, 
#                     tokenExpiration = None, dateCreated = None, 
#                     dateEffective = None, dateEnd = None, lastCheckTime = None ):

    
    rDict = self.__generateRowDict( locals() )
    self.__setElementStatus( 'Site', rDict )
    return S_OK()

  @CheckExecution
  def setSiteScheduledStatus( self, siteName, statusType, status, reason, tokenOwner, 
                              tokenExpiration, dateCreated, dateEffective, 
                              dateEnd, lastCheckTime ):
#  def setSiteScheduledStatus( self, siteName, statusType, status, reason, tokenOwner, 
#                              tokenExpiration = None, dateCreated = None, 
#                              dateEffective = None, dateEnd = None, lastCheckTime = None):

    rDict = self.__generateRowDict( locals() )
    self.__setElementScheduledStatus( 'Site', rDict )
    return S_OK()

  @CheckExecution
  def updateSiteStatus( self, siteName, statusType, status, reason, tokenOwner, 
                        tokenExpiration, dateCreated, dateEffective, dateEnd, 
                        lastCheckTime ):
#  def updateSiteStatus( self, siteName, statusType = None, status = None, reason = None, 
#                        tokenOwner = None, tokenExpiration = None, dateCreated = None, 
#                        dateEffective = None, dateEnd = None, lastCheckTime = None ):

    rDict = self.__generateRowDict( locals() )
    self.__updateElementStatus( 'Site' , rDict )
    return S_OK()

  #@CheckExecution
  def getSites( self, siteName, siteType, gridSiteName, **kwargs ):
#  def getSites( self, siteName = None, siteType = None, gridSiteName = None, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Site', rDict, **kwargs )

  @CheckExecution
  def getSitesStatus( self, siteName, statusType, status, reason, tokenOwner, 
                      tokenExpiration, dateCreated, dateEffective, dateEnd, 
                      lastCheckTime, **kwargs ):
#  def getSitesStatus( self, siteName = None, statusType = None, status = None, 
#                      reason = None, tokenOwner = None, tokenExpiration = None, 
#                      dateCreated = None, dateEffective = None, dateEnd = None, 
#                      lastCheckTime = None, **kwargs ):


    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Site', rDict, table = 'Status', **kwargs )

  @CheckExecution
  def getSitesHistory( self, siteName, statusType, status, reason, tokenOwner, 
                       tokenExpiration, dateCreated, dateEffective, dateEnd, 
                       lastCheckTime, **kwargs ):
#  def getSitesHistory( self, siteName = None, statusType = None, status = None, 
#                       reason = None, tokenOwner = None, tokenExpiration = None, 
#                       dateCreated = None, dateEffective = None, dateEnd = None, 
#                       lastCheckTime = None, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'SiteName', 'SiteHistoryID' ]
    return self.__getElements( 'Site', rDict, table = 'History', **kwargs )

  @CheckExecution
  def getSitesScheduledStatus( self, siteName, statusType, status, reason, 
                               tokenOwner, tokenExpiration, dateCreated, 
                               dateEffective, dateEnd, lastCheckTime, **kwargs ):

#  def getSitesScheduledStatus( self, siteName = None, statusType = None, 
#                               status = None, reason = None, tokenOwner = None, 
#                               tokenExpiration = None, dateCreated = None, 
#                               dateEffective = None, dateEnd = None, 
#                               lastCheckTime = None, **kwargs):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Site', rDict, table = 'ScheduledStatus', **kwargs )

  @CheckExecution
  def getSitesPresent( self, siteName, siteType, gridSiteName, gridTier, 
                       statusType, status, dateEffective, reason, lastCheckTime, 
                       tokenOwner, tokenExpiration, formerStatus, **kwargs ):
#  def getSitesPresent( self, siteName = None, siteType = None, gridSiteName = None,
#                       gridTier = None, statusType = None, status = None, dateEffective = None,
#                       reason = None, lastCheckTime = None, tokenOwner = None,
#                       tokenExpiration = None, formerStatus = None, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    if not kwargs.has_key( 'columns' ):
      kwargs[ 'columns' ] = [ 'SiteName', 'StatusType', 'Status', 'FormerStatus', 'DateEffective', 'LastCheckTime' ]
    return self.__getElements( 'Site', rDict, table = 'Present', **kwargs )

  @CheckExecution
  def deleteSites( self, siteName ):
    
    rDict = self.__generateRowDict( locals() )  
    self.__deleteElements( 'Site', rDict)
    return S_OK()

  @CheckExecution
  def deleteSitesScheduledStatus( self, siteName, statusType, status, reason, 
                                  tokenOwner, tokenExpiration, dateCreated, 
                                  dateEffective, dateEnd, lastCheckTime ):
#  def deleteSitesScheduledStatus( self, siteName = None, statusType = None, 
#                                  status = None, reason = None, tokenOwner = None, 
#                                  tokenExpiration = None, dateCreated = None, 
#                                  dateEffective = None, dateEnd = None, 
#                                  lastCheckTime = None):

    
    rDict = self.__generateRowDict( locals() )
    self.__deleteElementsScheduledStatus( 'Site', rDict )
    return S_OK()
    
  @CheckExecution    
  def deleteSitesHistory( self, siteName, statusType, status, reason, tokenOwner, 
                          tokenExpiration, dateCreated, dateEffective, dateEnd, 
                          lastCheckTime, **kwargs ):
#  def deleteSitesHistory( self, siteName = None, statusType = None, status = None, 
#                          reason = None, tokenOwner = None, tokenExpiration = None, 
#                          dateCreated = None, dateEffective = None, dateEnd = None, 
#                          lastCheckTime = None, **kwargs ):
      
    rDict = self.__generateRowDict( locals() )  
    self.__deleteElementHistory( 'Site', rDict, **kwargs )
    return S_OK()
  
  '''
  ##############################################################################
  # SERVICE FUNCTIONS
  ##############################################################################
  '''

  @CheckExecution
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
    return S_OK()
       
  @CheckExecution     
  def setServiceStatus( self, serviceName, statusType, status, reason, tokenOwner, 
                        tokenExpiration, dateCreated, dateEffective, dateEnd, 
                        lastCheckTime ):
  #def setServiceStatus( self, serviceName, statusType, status, reason, tokenOwner, 
  #                      tokenExpiration = None, dateCreated = None, 
  #                      dateEffective = None, dateEnd = None, lastCheckTime = None ):
    rDict = self.__generateRowDict( locals() )
    self.__setElementStatus( 'Service', rDict )
    return S_OK()

  @CheckExecution
  def setServiceScheduledStatus( self, serviceName, statusType, status, reason, 
                                 tokenOwner, tokenExpiration, dateCreated, 
                                 dateEffective, dateEnd, lastCheckTime ):
  #def setServiceScheduledStatus( self, serviceName, statusType, status, reason, tokenOwner, 
  #                      tokenExpiration = None, dateCreated = None, 
  #                      dateEffective = None, dateEnd = None, lastCheckTime = None ):
    rDict = self.__generateRowDict( locals() )
    self.__setElementScheduledStatus( 'Service', rDict )
    return S_OK()

  @CheckExecution
  def updateServiceStatus( self, serviceName, statusType, status, reason, 
                           tokenOwner, tokenExpiration, dateCreated, 
                           dateEffective, dateEnd, lastCheckTime ):
#  def updateServiceStatus( self, serviceName, statusType = None, status = None, reason = None, 
#                        tokenOwner = None, tokenExpiration = None, dateCreated = None, 
#                        dateEffective = None, dateEnd = None, lastCheckTime = None ):

    rDict = self.__generateRowDict( locals() )
    self.__updateElementStatus( 'Service', rDict )
    return S_OK()

  @CheckExecution
  def getServices( self, serviceName, serviceType, siteName, **kwargs ):
#  def getServices( self, serviceName = None, serviceType = None, siteName = None, **kwargs ):    
    
    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Service', rDict, **kwargs )

  @CheckExecution
  def getServicesStatus( self, serviceName, statusType, status, reason, 
                         tokenOwner, tokenExpiration, dateCreated, dateEffective, 
                         dateEnd, lastCheckTime, **kwargs ):
  #def getServicesStatus( self, serviceName = None, statusType = None, status = None, 
  #                       reason = None, tokenOwner = None, tokenExpiration = None, 
  #                       dateCreated = None, dateEffective = None, dateEnd = None, 
  #                       lastCheckTime = None, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Service', rDict, table = 'Status', **kwargs )

  @CheckExecution
  def getServicesHistory( self, serviceName, statusType, status, reason, 
                          tokenOwner, tokenExpiration, dateCreated, dateEffective, 
                          dateEnd, lastCheckTime, **kwargs ):
#  def getServicesHistory( self, serviceName = None, statusType = None, status = None, 
#                          reason = None, tokenOwner = None, tokenExpiration = None, 
#                          dateCreated = None, dateEffective = None, dateEnd = None, 
#                          lastCheckTime = None, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'ServiceName', 'ServiceHistoryID' ]
    return self.__getElements( 'Service', rDict, table = 'History', **kwargs )

  @CheckExecution
  def getServicesScheduledStatus( self, serviceName, statusType, status, reason, 
                                  tokenOwner, tokenExpiration, dateCreated, 
                                  dateEffective, dateEnd, lastCheckTime, **kwargs ):
#  def getServicesScheduledStatus( self, serviceName = None, statusType = None, 
#                                 status = None, reason = None, tokenOwner = None, 
#                                 tokenExpiration = None, dateCreated = None, 
#                                 dateEffective = None, dateEnd = None, 
#                                 slastCheckTime = None, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Service', rDict, table = 'ScheduledStatus', **kwargs )

  @CheckExecution
  def getServicesPresent( self, serviceName, siteName, siteType, serviceType, 
                          statusType, status, dateEffective, reason, lastCheckTime, 
                          tokenOwner, tokenExpiration, formerStatus, **kwargs ):
#  def getServicesPresent( self, serviceName = None, siteName = None, siteType = None, 
#                          serviceType = None, statusType = None, status = None, 
#                          dateEffective = None, reason = None, lastCheckTime = None, 
#                          tokenOwner = None, tokenExpiration = None, 
#                          formerStatus = None, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    if not kwargs.has_key( 'columns' ):
      kwargs[ 'columns' ] = [ 'ServiceName', 'StatusType', 'Status', 'FormerStatus', 'DateEffective', 'LastCheckTime' ]
    return self.__getElements( 'Service', rDict, table = 'Present', **kwargs )

  @CheckExecution
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

    rDict = { 'SiteName' : siteName }
    
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
    
  @CheckExecution  
  def deleteServices( self, serviceName ):
    
    rDict = self.__generateRowDict( locals() )  
    self.__deleteElements( 'Service', rDict)
    return S_OK()

  @CheckExecution
  def deleteServicesScheduledStatus( self, serviceName, statusType, status, 
                                     reason, tokenOwner, tokenExpiration, dateCreated, 
                                     dateEffective, dateEnd, lastCheckTime ):
#  def deleteServicesScheduledStatus( self, serviceName = None, statusType = None, 
#                                     status = None, reason = None, tokenOwner = None, 
#                                     tokenExpiration = None, dateCreated = None, 
#                                     dateEffective = None, dateEnd = None, lastCheckTime = None):
    
    rDict = self.__generateRowDict( locals() )
    self.__deleteElementsScheduledStatus( 'Service', rDict )
    return S_OK()

  @CheckExecution
  def deleteServicesHistory( self, serviceName, statusType, status, reason, 
                             tokenOwner, tokenExpiration, dateCreated, dateEffective, 
                             dateEnd, lastCheckTime, **kwargs ):
#  def deleteServicesHistory( self, serviceName = None, statusType = None, status = None, 
#                          reason = None, tokenOwner = None, tokenExpiration = None, 
#                          dateCreated = None, dateEffective = None, dateEnd = None, 
#                          lastCheckTime = None, **kwargs ):      

    rDict = self.__generateRowDict( locals() )  
    self.__deleteElementHistory( 'Service', rDict, **kwargs )
    return S_OK()

  '''
  ##############################################################################
  # RESOURCE FUNCTIONS
  ##############################################################################
  '''
  
  @CheckExecution
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
  
  @CheckExecution
  def setResourceStatus( self, resourceName, statusType, status, reason, tokenOwner, 
                         tokenExpiration = None, dateCreated = None, 
                         dateEffective = None, dateEnd = None, lastCheckTime = None ):
    
    rDict = self.__generateRowDict( locals() )
    self.__setElementStatus( 'Resource', rDict )

  @CheckExecution
  def updateResourceStatus( self, resourceName, statusType = None, status = None, reason = None, 
                         tokenOwner = None, tokenExpiration = None, dateCreated = None, 
                         dateEffective = None, dateEnd = None, lastCheckTime = None ):
    
    rDict = self.__generateRowDict( locals() )
    self.__updateElementStatus( 'Resource', rDict )

  @CheckExecution
  def setResourceScheduledStatus( self, resourceName, statusType, status, reason, 
                                  tokenOwner, tokenExpiration = None, dateCreated = None, 
                                  dateEffective = None, dateEnd = None, lastCheckTime = None ):
    
    rDict = self.__generateRowDict( locals() )
    self.__setElementScheduledStatus( 'Resource', rDict )
  
  @CheckExecution
  def getResources( self, resourceName = None, resourceType = None, 
                    serviceType = None, siteName = None, gridSiteName = None, 
                    **kwargs ):
    
    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Resource', rDict, **kwargs )

  @CheckExecution
  def getResourcesStatus( self, resourceName = None, statusType = None, status = None,
                          reason = None, tokenOwner = None, tokenExpiration = None, 
                          dateCreated = None, dateEffective = None, dateEnd = None, 
                          lastCheckTime = None, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Resource', rDict, table = 'Status', **kwargs )

  @CheckExecution
  def getResourcesHistory( self, resourceName = None, statusType = None, status = None,
                           reason = None, tokenOwner = None, tokenExpiration = None, 
                           dateCreated = None, dateEffective = None, dateEnd = None, 
                           lastCheckTime = None, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'ResourceName', 'ResourceHistoryID' ]
    return self.__getElements( 'Resource', rDict, table = 'History', **kwargs )

  @CheckExecution
  def getResourcesScheduledStatus( self, resourceName = None, statusType = None, status = None,
                                  reason = None, tokenOwner = None, tokenExpiration = None, 
                                  dateCreated = None, dateEffective = None, dateEnd = None, 
                                  lastCheckTime = None, **kwargs):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Resource', rDict, table = 'ScheduledStatus', **kwargs )  

  @CheckExecution
  def getResourcesPresent( self, resourceName = None, siteName = None, serviceType = None,
                           gridSiteName = None, siteType = None, resourceType = None,
                           statusType = None, status = None, dateEffective = None, 
                           reason = None, lastCheckTime = None, tokenOwner = None, 
                           tokenExpiration = None, formerStatus = None, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    if not kwargs.has_key( 'columns' ):
      kwargs[ 'columns' ] = [ 'ResourceName', 'StatusType', 'Status', 'FormerStatus', 'DateEffective', 'LastCheckTime' ]
    return self.__getElements( 'Resource', rDict, table = 'Present', **kwargs )
  
  @CheckExecution
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
      #name   = self.getGridSiteName( element, name )[ 'Value' ]
      #rDict[ 'GridSiteName' ] = name
      resourceNames = [ sn[0] for sn in self.getResources( siteName = name )[ 'Value' ] ]
      rDict[ 'ResourceName' ] = resourceNames

    elif element == 'Service':
      serviceType = name.split( '@' )[ 0 ]
      name        = name.split( '@' )[ 1 ]
      if serviceType == 'Computing':
        resourceNames = [ sn[0] for sn in self.getResources( siteName = name )[ 'Value' ] ]
        rDict[ 'ResourceName' ] = resourceNames
        #rDict[ 'SiteName' ] = name
      else:
        gridSiteName = self.getGridSiteName( 'Site', name )[ 'Value' ]
        #rDict[ 'GridSiteName' ] = gridSiteName
        resourceNames = [ sn[0] for sn in self.getResources( gridSiteName = gridSiteName, serviceType = serviceType )[ 'Value' ] ]
        #rDict[ 'SiteName' ] = siteNames
        rDict[ 'ResourceName' ] = resourceNames
        #rDict[ 'ServiceType' ]  = serviceType
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
    
  @CheckExecution  
  def deleteResources( self, resourceName ):
    
    rDict = self.__generateRowDict( locals() )  
    self.__deleteElements( 'Resource', rDict)

  @CheckExecution
  def deleteResourcesScheduledStatus( self, resourceName = None, statusType = None, 
                                      status = None, reason = None, tokenOwner = None, 
                                      tokenExpiration = None, dateCreated = None, 
                                      dateEffective = None, dateEnd = None, 
                                      lastCheckTime = None):

    rDict = self.__generateRowDict( locals() )
    self.__deleteElementsScheduledStatus( 'Resource', rDict )

  @CheckExecution
  def deleteResourcesHistory( self, resourceName = None, statusType = None, status = None, 
                              reason = None, tokenOwner = None, tokenExpiration = None, 
                              dateCreated = None, dateEffective = None, dateEnd = None, 
                              lastCheckTime = None, **kwargs ):
      
    rDict = self.__generateRowDict( locals() )  
    self.__deleteElementHistory( 'Resource', rDict, **kwargs )

  '''
  ##############################################################################
  # STORAGE ELEMENT FUNCTIONS
  ##############################################################################
  '''

  @CheckExecution
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
  
  @CheckExecution
  def setStorageElementStatus( self, storageElementName, statusType, status, 
                               reason, tokenOwner, tokenExpiration = None, 
                               dateCreated = None, dateEffective = None, dateEnd = None, 
                               lastCheckTime = None ):
    
    rDict = self.__generateRowDict( locals() )
    self.__setElementStatus( 'StorageElement', rDict )

  @CheckExecution
  def updateStorageElementStatus( self, storageElementName, statusType = None, status = None, 
                                 reason = None , tokenOwner = None, tokenExpiration = None, 
                                 dateCreated = None, dateEffective = None, dateEnd = None, 
                                 lastCheckTime = None ):
    
    rDict = self.__generateRowDict( locals() )
    self.__updateElementStatus( 'StorageElement', rDict )

  @CheckExecution
  def setStorageElementScheduledStatus( self, storageElementName, statusType, status, 
                                        reason, tokenOwner, tokenExpiration = None, 
                                        dateCreated = None, dateEffective = None, 
                                        dateEnd = None, lastCheckTime = None ):
    
    rDict = self.__generateRowDict( locals() )
    self.__setElementScheduledStatus( 'StorageElement', rDict )

  @CheckExecution
  def getStorageElements( self, storageElementName = None, resourceName = None, 
                          gridSiteName = None, **kwargs ):
    
    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'StorageElement', rDict, **kwargs )

  @CheckExecution
  def getStorageElementsStatus( self, storageElementName = None, statusType = None, 
                                status = None, reason = None, tokenOwner = None, 
                                tokenExpiration = None, dateCreated = None, 
                                dateEffective = None, dateEnd = None, 
                                lastCheckTime = None, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'StorageElement', rDict, table = 'Status', **kwargs )

  @CheckExecution
  def getStorageElementsHistory( self, storageElementName = None, statusType = None, 
                                 status = None, reason = None, tokenOwner = None, 
                                 tokenExpiration = None, dateCreated = None, 
                                 dateEffective = None, dateEnd = None, 
                                 lastCheckTime = None, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'StorageElement', rDict, table = 'History', **kwargs )

  @CheckExecution
  def getStorageElementsScheduledStatus( self, storageElementName = None, statusType = None, 
                                         status = None, reason = None, tokenOwner = None, 
                                         tokenExpiration = None, dateCreated = None, 
                                         dateEffective = None, dateEnd = None, 
                                         lastCheckTime = None, **kwargs ):
    '''
      **kwargs can be:
          columns <list> column names
          sort    <list> column names
          order   'ASC' || 'DESC' 
          limit   <integer>
    '''
    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'StorageElement', rDict, table = 'ScheduledStatus', **kwargs )  

  @CheckExecution
  def getStorageElementsPresent( self, storageElementName = None, resourceName = None, 
                                 gridSiteName = None, siteType = None, statusType = None, 
                                 status = None, dateEffective = None, reason = None, 
                                 lastCheckTime = None, tokenOwner = None,
                                 tokenExpiration = None, formerStatus = None, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    if not kwargs.has_key( 'columns' ):
      kwargs[ 'columns' ] = [ 'StorageElementName', 'StatusType', 'Status', 'FormerStatus', 'DateEffective', 'LastCheckTime' ]
    return self.__getElements( 'StorageElement', rDict, table = 'Present', **kwargs )

  @CheckExecution
  def getStorageElementStats( self, element, name, statusType = None ):
    
    res = { 'Total' : 0 }
    for validStatus in ValidStatus:
      res[ validStatus ] = 0

    rDict = {}
    
    if statusType is not None:
      self.__validateElementStatusTypes( 'StorageElement', statusType )
      rDict[ 'StatusType'] = statusType
    
    if element == 'Site':
      #rDict[ 'GridSiteName' ] = self.getGridSiteName( element, name )[ 'Value' ]
      gridSiteName = self.getGridSiteName( element, name )[ 'Value' ]
      ##siteNames = [ sn[0] for sn in self.getSites( gridSiteName = gridSiteName )[ 'Value' ] ]
      ##rDict[ 'SiteName' ] = siteNames
      seNames = [ sn[0] for sn in self.getStorageElements( gridSiteName = gridSiteName )[ 'Value' ] ]
      rDict[ 'StorageElementName' ] = seNames            
      
    elif element == 'Resource':
      #rDict[ 'ResourceName' ] = name
      seNames = [ sn[0] for sn in self.getStorageElements( resourceName = name )[ 'Value' ] ]
      rDict[ 'StorageElementName' ] = seNames            
      
    
    count = self.__getElementStatusRowCount( 'StorageElement', rDict )

    for x in count[ 'Value' ]:
      res[x[0]] = int(x[1])

    res['Total'] = sum( res.values() )

    return S_OK( res )   
    
  @CheckExecution  
  def deleteStorageElements( self, storageElementName ):
    
    rDict = self.__generateRowDict( locals() )  
    self.__deleteElements( 'StorageElement', rDict)

  @CheckExecution
  def deleteStorageElementsScheduledStatus( self, storageElementName = None, statusType = None, 
                                            status = None, reason = None, tokenOwner = None, 
                                            tokenExpiration = None, dateCreated = None, 
                                            dateEffective = None, dateEnd = None, 
                                            lastCheckTime = None ):

    rDict = self.__generateRowDict( locals() )
    self.__deleteElementsScheduledStatus( 'StorageElement', rDict )

  @CheckExecution
  def deleteStorageElementsHistory( self, storageElementName = None, statusType = None, 
                                    status = None, reason = None, tokenOwner = None, 
                                    tokenExpiration = None, dateCreated = None, 
                                    dateEffective = None, dateEnd = None, 
                                    lastCheckTime = None, **kwargs ):
      
    rDict = self.__generateRowDict( locals() )  
    self.__deleteElementHistory( 'StorageElement', rDict, **kwargs )

  '''
  ##############################################################################
  # GRID SITE FUNCTIONS
  ##############################################################################
  '''
 
  @CheckExecution 
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
  
  @CheckExecution
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

    resQuery = self.__getGridElement( rDict, None )

    if not resQuery[ 'Value' ]:
      return S_OK( [] )

    resList = [ x for x in resQuery[ 'Value' ] ]
    return S_OK( resList )

  @CheckExecution
  def getGridSiteName( self, granularity, name ):

    self.__validateRes( granularity )

    kwargs = { 'columns' : [ 'GridSiteName' ] }

    resQuery = self.__getElements( granularity, 
                        { '%sName' % granularity : name }, 
                         **kwargs )

    if not resQuery[ 'Value' ]:
      return S_OK( [] )

    return S_OK( resQuery[ 'Value' ][ 0 ][ 0 ] )

  @CheckExecution
  def deleteGridSiteName( self, gridSiteName ):
    
    whereElements = self.__getMultipleWhereElements( { 'GridSiteName' : gridSiteName })
    self.__deleteRow( 'GridSite', whereElements )

  '''
  ##############################################################################
  # MISC FUNCTIONS
  ##############################################################################
  '''
  
  @CheckExecution
  def getGeneralName( self, from_element, name, to_element ):
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
      kwargs = { 'columns' : [ 'SiteName' ] }  
      resQuery = self.__getElements( from_element, { '%sName' % from_element : name }, **kwargs )#'SiteName' )

    elif from_element == 'Resource':
      kwargs = { 'columns' : [ 'ServiceType' ] }    
      resQuery = self.__getElements( from_element, { '%sName' % from_element : name }, **kwargs )#'ServiceType' )
      serviceType = resQuery[ 'Value' ][ 0 ][ 0 ]

      if serviceType == 'Computing':
        kwargs = { 'columns' : [ 'SiteName' ] }  
        resQuery = self.__getElements( from_element, { '%sName' % from_element : name }, **kwargs )#'SiteName' )
      else:
        kwargs = { 'columns' : [ 'GridSiteName' ] }    
        gridSiteNames = self.__getElements( from_element, { '%sName' % from_element : name }, **kwargs )#'GridSiteName' )
        kwargs = { 'columns' : [ 'SiteName' ] }  
        resQuery = self.__getElements( 'Sites', {'GridSiteName' : list( gridSiteNames[ 'Value' ] )}, **kwargs )#'SiteName')
        
    elif from_element == 'StorageElement':

      if to_element == 'Resource':
        kwargs = { 'columns' : [ 'ResourceName' ] }    
        resQuery = self.__getElements( from_element, { 'StorageElementName' : name }, **kwargs )#'ResourceName' )
      else:
        kwargs = { 'columns' : [ 'GridSiteName' ] }  
        gridSiteNames = self.__getElements( from_element, { 'StorageElementName' : name }, **kwargs)#'GridSiteName' )
        kwargs = { 'columns' : [ 'SiteName' ] }
        resQuery = self.__getElements( 'Sites', {'GridSiteName' : list( gridSiteNames[ 'Value' ] )}, **kwargs)#'SiteName')

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
      
  @CheckExecution
  def getCountries( self, granularity ):
    """
    Get countries of resources in granularity

    :params:
      :attr:`granularity`: string - a ValidRes
    """

    self.__validateRes( granularity )

    if granularity == 'StorageElement':
      granularity = "Site"

    kwargs = { 'columns' : [ '%sName' % granularity ] }
    resQuery = self.__getElementRow( granularity, {}, **kwargs )#'%sName' % granularity )
    if not resQuery[ 'Value' ]:
      return S_OK( None )

    countries = []

    for name in resQuery[ 'Value' ]:
      country = name[0].split('.').pop()
      if country not in countries:
        countries.append( country )

    return S_OK( countries )
  
  @CheckExecution
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

    kwargs = { 'columns' : [ '%sName' % granularity, 'StatusType', 'TokenOwner', 'TokenExpiration'] }
    if name is not None:
      dict[ '%sName' % granularity ] = name
    if statusType is not None:
      self.__validateElementStatusTypes( granularity, statusType )
      dict[ 'StatusType' ] = statusType

    resQuery = self.__getElementRow( '%sStatus' % granularity , dict, **kwargs )#columns)

    if not resQuery[ 'Value' ]:
      return S_OK( [] )

    if dateExpiration is None:
      tokenList = [ x for x in resQuery[ 'Value' ] ]
    else:
      tokenList = [ x for x in resQuery[ 'Value' ] if x[3] < dateExpiration ]
    
    return S_OK( tokenList )
  
  @CheckExecution
  def setToken( self, granularity, name, reason, newTokenOwner, dateExpiration, statusType ):
    """
    (re)Set token properties.
    """

    self.__validateRes(granularity)
    self.__validateElementStatusTypes(granularity, statusType )

    rDict = {
             '%sName' % granularity : name,
             'StatusType'           : statusType
             }
    
    kwargs = { 'columns': [ 'Status' ] }
    elementStatus = self.__getElementRow( '%sStatus' % granularity, rDict, **kwargs )#'Status')

    if not elementStatus[ 'Value' ]:
      message = 'Not found entry with name %s and type %s of granularity %s' % ( name, statusType, granularity )
      raise RSSDBException, where( self, self.setToken ) + message
    
    status = elementStatus[ 'Value' ][ 0 ][ 0 ]
    
    tokenSetter = getattr( self, 'set%sStatus' % granularity )
    tokenSetter( name, statusType, status, reason, newTokenOwner, tokenExpiration = dateExpiration,
                 dateEnd = dateExpiration )
    
  @CheckExecution  
  def whatIs( self, name ):
    """
    Find which is the granularity of name.
    """

    for g in ValidRes:
      
      kwargs = { 'columns' : [ '%sName' % g ] }
      resQuery = self.__getElementRow( g, { '%sName' % g : name }, **kwargs )#'%sName' % g )

      if not resQuery[ 'Value' ]:
        continue
      else:
        return S_OK( g )

    return S_OK( 'Unknown' )    
    
  @CheckExecution  
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

      now = datetime.utcnow().replace(microsecond = 0)
      toCheck = {}

      for freqName, freq in checkFrequency.items():
        toCheck[ freqName ] = ( now - timedelta( minutes=freq) ).isoformat(' ')

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

    cols = self.__getColumns( cols )

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

    resQuery = self.__getElement( 'Present%s' % granularity , cols, whereElements, None, None, None )

    #resQuery = self.db._query( req )
    #if not resQuery[ 'OK' ]:
    #  raise RSSDBException, where( self, self.getStuffToCheck ) + resQuery[ 'Message' ]
    if not resQuery[ 'Value' ]:
      return S_OK( [] )

    stuffList = [ x for x in resQuery[ 'Value' ]]

    return S_OK( stuffList )   
  
#  def getMonitoredsList( self, granularity, paramsList = None, siteName = None,
#                         serviceName = None, resourceName = None, storageElementName = None,
#                         statusType = None,
#                         status = None, siteType = None, resourceType = None,
#                         serviceType = None, 
#                         gridSiteName = None ):
#    """
#    Get Present Sites /Services / Resources / StorageElements lists.
#
#    :params:
#      :attr:`granularity`: a ValidRes
#
#      :attr:`paramsList`: a list of parameters can be entered. If not given,
#      a custom list is used.
#
#      :attr:`siteName`, `serviceName`, `resourceName`, `storageElementName`:
#      a string or a list representing the site/service/resource/storageElement name.
#      If not given, fetch all.
#
#      :attr:`status`: a string or a list representing the status. If not given, fetch all.
#
#      :attr:`siteType`: a string or a list representing the site type.
#      If not given, fetch all.
#
#      :attr:`serviceType`: a string or a list representing the service type.
#      If not given, fetch all.
#
#      :attr:`resourceType`: a string or a list representing the resource type.
#      If not given, fetch all.
#
#      :attr:`countries`: a string or a list representing the countries extensions.
#      If not given, fetch all.
#
#      :attr:`gridSiteName`: a string or a list representing the grid site name.
#      If not given, fetch all.
#
#      See :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils` for these parameters.
#
#    :return:
#      list of monitored paramsList's values
#    """
#
#    self.__validateRes( granularity )
#
#    if ( paramsList == None or paramsList == [] ):
#      paramsList = [ '%sName' % granularity, 'StatusType', 'Status', 'FormerStatus', 'DateEffective', 'LastCheckTime' ]
#
#    dict = {}
#    if siteName is not None:
#      dict[ 'SiteName']           = siteName
#    if serviceName is not None:
#      dict[ 'ServiceName']        = serviceName
#    if resourceName is not None:
#      dict[ 'ResourceName']       = resourceName
#    if storageElementName is not None:
#      dict[ 'storageElementName'] = storageElementName
#    if statusType is not None:
#      dict[ 'StatusType' ]        = statusType
#    if status is not None:
#      dict[ 'Status' ]            = status
#    if siteType is not None:
#      dict[ 'SiteType']           = siteType  
#    if serviceType is not None:
#      dict[ 'ServiceType']        = serviceType
#    if resourceType is not None:
#      dict[ 'ResourceType']       = resourceType   
#    if gridSiteName is not None:
#      dict[ 'GridSiteName']       = gridSiteName      
#   
#    kwargs = { 'columns' : paramsList }
#    resQuery = self.__getElementRow( 'Present%s' % granularity, dict, **kwargs )#paramsList )   
#    if not resQuery[ 'Value' ]:
#      return S_OK( [] )
#    
#    return resQuery

#  def setMonitoredToBeChecked( self, monitoreds, granularity, name ):
#    """
#    Set LastCheckTime to 0 to monitored(s)
#
#    :params:
#      :attr:`monitoreds`: string, or a list of strings where each is a ValidRes:
#      which granularity has to be set to be checked
#
#      :attr:`granularity`: string, a ValidRes: from who this set comes
#
#      :attr:`name`: string, name of Site or Resource
#    """
#
#    znever = datetime.min
#
#    if type( monitoreds ) is not list:
#      monitoreds = [ monitoreds ]
#
#    for monitored in monitoreds:
#
#      if monitored == 'Site':
#
#        siteName = self.getGeneralName( granularity, name, monitored )[ 'Value' ]
#        self.updateSiteStatus(siteName = siteName, lastCheckTime = znever )
#
#      elif monitored == 'Service' :
#
#        if granularity =='Site':
#          serviceName = self.getMonitoredsList( 'Service', paramsList = [ 'ServiceName' ],
#                                                siteName = name )[ 'Value' ]
#          if type( serviceName ) is not list:
#            serviceName = [ serviceName ]
#          if serviceName != []:
##            raise RSSDBException, where( self, self.setMonitoredToBeChecked ) + " No services for site %s" %name
##          else:
#            serviceName = [ x[0] for x in serviceName ]
#            self.updateServiceStatus( serviceName = serviceName, lastCheckTime = znever )
#        else:
#          serviceName = self.getGeneralName( granularity, name, monitored )[ 'Value' ]
#          self.updateServiceStatus( serviceName = serviceName, lastCheckTime = znever )
#
#      elif monitored == 'Resource':
#
#        if granularity == 'Site' :
#          resourceName = self.getMonitoredsList( 'Resource', paramsList = [ 'ResourceName' ],
#                                                 siteName = name )[ 'Value' ]
#          if type( resourceName ) is not list:
#            resourceName = [ resourceName ]
#          if resourceName != []:
#            #raise RSSDBException, where( self, self.setMonitoredToBeChecked ) + " No resources for site %s" %name
#          #else:
#            resourceName = [ x[0] for x in resourceName ]
#            self.updateResourceStatus( resourceName = resourceName, lastCheckTime = znever )
#
#        elif granularity == 'Service' :
#
#          #siteName = self.getGeneralName( granularity, name, 'Resource' )
#          serviceType, siteName = name.split('@')
#          gridSiteName          = self.getGridSiteName('Site', siteName)[ 'Value' ] 
#
#          resourceName = self.getMonitoredsList( monitored, paramsList = [ 'ResourceName' ],
#                                                 gridSiteName = gridSiteName, 
#                                                 serviceType = serviceType )[ 'Value' ]
#          if type( resourceName ) is not list:
#            resourceName = [ resourceName ]
#          if resourceName != []:
#         #   raise RSSDBException, where( self, self.setMonitoredToBeChecked ) + " No resources for service %s" %name
#         # else:
#            resourceName = [ x[0] for x in resourceName ]
#            self.updateResourceStatus( resourceName = resourceName, lastCheckTime = znever )
#
#        elif granularity == 'StorageElement':
#          resourceName = self.getGeneralName( granularity,  name, monitored )[ 'Value' ]
#          self.updateResourceStatus( resourceName = resourceName, lastCheckTime = znever )
#
#      # Put read and write together here... too much fomr copy/paste
#      elif monitored == 'StorageElement':
#
#        if granularity == 'Site':
#          
#          gridSiteName          = self.getGridSiteName('Site', siteName)[ 'Value' ]
#          SEName = self.getMonitoredsList( monitored, paramsList = [ 'StorageElementName' ],
#                                           gridSiteName = gridSiteName )[ 'Value' ]
#          if type( SEName ) is not list:
#            SEName = [ SEName ]
#          if SEName != []:
#            #pass
#          #else:
#            SEName = [ x[0] for x in SEName ]
#            self.updateStorageElementStatus( storageElementName = SEName, lastCheckTime = znever )
#
#        elif granularity == 'Resource':
#          SEName = self.getMonitoredsList( monitored, paramsList = [ 'StorageElementName' ],
#                                           resourceName = name )[ 'Value' ]
#          if type( SEName ) is not list:
#            SEName = [ SEName ]
#          if SEName == []:
#            pass
##            raise RSSDBException, where(self, self.setMonitoredToBeChecked) + "No storage elements for resource %s" %name
#          else:
#            SEName = [ x[0] for x in SEName ]
#            self.updateStorageElementStatus( storageElementName = SEName, lastCheckTime = znever )
#
#        elif granularity == 'Service':
#          
#          serviceType, siteName = name.split('@')
#          gridSiteName          = self.getGridSiteName('Site', siteName)[ 'Value' ] 
#          
#          SEName = self.getMonitoredsList( monitored, paramsList = [ 'StorageElementName' ],
#                                           gridSiteName = gridSiteName )[ 'Value' ]#name.split('@').pop() )[ 'Value' ]
#          if type( SEName ) is not list:
#            SEName = [ SEName ]
#          if SEName != []:
#            #pass
##            raise RSSDBException, where(self, self.setMonitoredToBeChecked) + "No storage elements for service %s" %name
#          #else:
#            SEName = [ x[0] for x in SEName ]
#            self.updateStorageElementStatus( storageElementName = SEName, lastCheckTime = znever ) 

  @CheckExecution
  def setMonitoredReason( self, granularity, name, statusType, reason, tokenOwner ):
    
    self.__validateRes( granularity )
    
    updatter = getattr( self, 'update%sStatus' % granularity )
    updatter( name, statusType = statusType, reason = reason, tokenOwner = tokenOwner )
    
    
  ## Hasta la vista baby
  ## Nr of hours spent trying to rewrite this method:
  ## 2 ( increase this counter when needed )
  @CheckExecution
  def getMonitoredsStatusWeb( self, granularity, selectDict, startItem, maxItems ):
    """
    Get present sites status list, for the web.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
    and :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsHistory`

    Example of parameters:

    :params:
      :attr:`selectDict`: { 'SiteName':['XX', ...] , 'ExpandSiteHistory': ['XX', ...],
      'Status': ['XX', ...]}
      and equivalents for the other monitoreds

      :attr:`sortList`

      :attr:`startItem`

      :attr:`maxItems`

    :return: {
      :attr:`ParameterNames`: ['SiteName', 'Tier', 'GridType', 'Country',
      'Status', 'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'],

      :attr:'Records': [[], [], ...],

      :attr:'TotalRecords': X,

      :attr:'Extras': {}

      }
    """

    if granularity == 'Site':
      paramNames = [ 'SiteName', 'Tier', 'GridType', 'Country',
                     'StatusType','Status', 'DateEffective', 'FormerStatus', 'Reason' ]
      paramsList = [ 'SiteName', 'SiteType', 'StatusType','Status', 'DateEffective',
                     'FormerStatus', 'Reason' ]
    elif granularity == 'Service':
      paramNames = [ 'ServiceName', 'ServiceType', 'Site', 'Country', 'StatusType','Status',
                     'DateEffective', 'FormerStatus', 'Reason' ]
      paramsList = [ 'ServiceName', 'ServiceType', 'SiteName', 'StatusType','Status',
                     'DateEffective', 'FormerStatus', 'Reason' ]
    elif granularity == 'Resource':
      paramNames = [ 'ResourceName', 'ServiceType', 'SiteName', 'ResourceType',
                     'Country', 'StatusType','Status', 'DateEffective', 'FormerStatus', 'Reason' ]
      paramsList = [ 'ResourceName', 'ServiceType', 'SiteName', 'GridSiteName', 'ResourceType',
                     'StatusType','Status', 'DateEffective', 'FormerStatus', 'Reason' ]
    elif granularity == 'StorageElement':
      paramNames = [ 'StorageElementName', 'ResourceName', 'SiteName',
                     'Country', 'StatusType','Status', 'DateEffective', 'FormerStatus', 'Reason' ]
      paramsList = [ 'StorageElementName', 'ResourceName', 'GridSiteName', 'StatusType','Status',
                     'DateEffective', 'FormerStatus', 'Reason' ]
    else:
      raise InvalidRes, where( self, self.getMonitoredsStatusWeb )

    records                = []

    rDict = { 'SiteName'                    : None,
              'ServiceName'                 : None,
              'ResourceName'                : None,
              'StorageElementName'          : None,
              'StatusType'                  : None,
              'Status'                      : None,
              'SiteType'                    : None,
              'ServiceType'                 : None,
              'ResourceType'                : None,
#              'Countries'                   : None,
              'ExpandSiteHistory'           : None,
              'ExpandServiceHistory'        : None,
              'ExpandResourceHistory'       : None,
              'ExpandStorageElementHistory' : None }


    for k in rDict.keys():
      if selectDict.has_key( k ):
        rDict[ k ] = selectDict[ k ]
        if not isinstance( rDict, list ):
          rDict[ k ] = [ rDict[ k ] ]

    if selectDict.has_key( 'Expanded%sHistory' % granularity ):
      paramsList = [ '%sName', 'StatusType', 'Status', 'Reason', 'DateEffective' ]
      elements   = rDict[ 'Expanded%sHistory' % granularity ]
      hgetter    = getattr( self, 'get%ssHhistory' )
      kwargs     = { '%sName' % granularity : elements, 'columns' : paramsList }  
      elementsH  = hgetter( **kwargs )
      #elementsH  = self.getMonitoredsHistory( granularity, paramsList = paramsList,
      #                                        name = elements )

      for elementH in elementsH[ 'Value' ]:
        record = []
        record.append( elementH[ 0 ] )  #%sName % granularity
        record.append( None )           #Tier
        record.append( None )           #GridType
        record.append( None )           #Country
        record.append( elementH[ 1 ] )  #StatusType 
        record.append( elementH[ 2 ] )  #Status
        record.append( elementH[ 4 ].isoformat(' ') ) #DateEffective
        record.append( None )           #FormerStatus
        record.append( elementH[ 3 ] )  #Reason
        records.append( record )        

    else:
      kwargs = { 'columns' : paramsList }  
      if granularity == 'Site':
          
        sitesList = self.getSitesPresent( siteName = rDict['SiteName'], 
                                          status   = rDict['Status'],
                                          siteType   = rDict['SiteType'],
                                          **kwargs )  
        #sitesList = self.getMonitoredsList(granularity,
        #                                   paramsList = paramsList,
        #                                   siteName   = rDict['SiteName'], #sites_select,
        #                                   status     = rDict['Status'],   #status_select,
        #                                   siteType   = rDict['SiteType'])#, #siteType_select,
        #                                   #countries  = rDict['Countries'])#countries_select)
        for site in sitesList[ 'Value' ]:
          record   = []
          gridType = ( site[ 0 ] ).split( '.' ).pop(0)
          country  = ( site[ 0 ] ).split( '.' ).pop()

          record.append( site[ 0 ] ) #SiteName
          record.append( site[ 1 ] ) #Tier
          record.append( gridType ) #GridType
          record.append( country ) #Country
          record.append( site[ 2 ] ) #StatusType
          record.append( site[ 3 ] ) #Status
          record.append( site[ 4 ].isoformat(' ') ) #DateEffective
          record.append( site[ 5 ] ) #FormerStatus
          record.append( site[ 6 ] ) #Reason
          records.append( record )

      elif granularity == 'Service':
        
        servicesList = self.getServicesPresent( serviceName = rDict['ServiceName'],
                                                siteName    = rDict['SiteName'],
                                                status      = rDict['Status'],
                                                siteType    = rDict['SiteType'],
                                                serviceType = rDict['ServiceType'],
                                                **kwargs )         
        
        #servicesList = self.getMonitoredsList( granularity,
        #                                       paramsList  = paramsList,
        #                                       serviceName = rDict['ServiceName'], #services_select,
        #                                       siteName    = rDict['SiteName'], #sites_select,
        #                                       status      = rDict['Status'], #status_select,
        #                                       siteType    = rDict['SiteType'], #siteType_select,
        #                                       serviceType = rDict['ServiceType'])#, #serviceType_select,
        #                                     #  countries   = rDict['Countries']) #countries_select )
        for service in servicesList[ 'Value' ]:
          record  = []
          country = ( service[ 0 ] ).split( '.' ).pop()

          record.append( service[ 0 ] ) #ServiceName
          record.append( service[ 1 ] ) #ServiceType
          record.append( service[ 2 ] ) #Site
          record.append( country ) #Country
          record.append( service[ 3 ] ) #StatusType
          record.append( service[ 4 ] ) #Status
          record.append( service[ 5 ].isoformat(' ') ) #DateEffective
          record.append( service[ 6 ] ) #FormerStatus
          record.append( service[ 7 ] ) #Reason
          records.append( record )

      elif granularity == 'Resource':
        if rDict[ 'SiteName' ] == None:
          kw = { 'columns' : [ 'SiteName' ] }
          sites_select = self.getSitesPresent( **kw )
          #sites_select = self.getMonitoredsList( 'Site',
          #                                       paramsList = [ 'SiteName' ] )
          rDict[ 'SiteName' ] = [ x[ 0 ] for x in sites_select[ 'Value' ] ] 
          
        kw = { 'columns' : [ 'GridSiteName' ] }
        gridSites_select = self.getSitesPresent( siteName = rDict[ 'SiteName'], **kw )
        #gridSites_select = self.getMonitoredsList( 'Site',
        #                                           paramsList = [ 'GridSiteName' ],
        #                                           siteName = rDict[ 'SiteName' ] )
        
        gridSites_select = [ x[ 0 ] for x in gridSites_select[ 'Value' ] ]

        resourcesList = self.getResourcesPresent( resourceName = rDict['ResourceName'],
                                                  status       = rDict['Status'],
                                                  siteType     = rDict['SiteType'],
                                                  resourceType = rDict['ResourceType'],
                                                  gridSiteName = gridSites_select,
                                                  **kwargs )

        #resourcesList = self.getMonitoredsList( granularity,
        #                                        paramsList   = paramsList,
        #                                        resourceName = rDict['ResourceName'],#resources_select,
        #                                        status       = rDict['Status'],#status_select,
        #                                        siteType     = rDict['SiteType'],#siteType_select,
        #                                        resourceType = rDict['ResourceType'],#resourceType_select,
        #                                        #countries    = rDict['Countries'],#countries_select,
        #                                        gridSiteName = gridSites_select )

        for resource in resourcesList[ 'Value' ]:
          DIRACsite = resource[ 2 ]

          if DIRACsite == 'NULL':
            GridSiteName = resource[ 3 ]  #self.getGridSiteName(granularity, resource[0])
            DIRACsites = getDIRACSiteName( GridSiteName )
            if not DIRACsites[ 'OK' ]:
              raise RSSDBException, 'Error executing getDIRACSiteName'
            DIRACsites = DIRACsites[ 'Value' ]
            DIRACsite_comp = ''
            for DIRACsite in DIRACsites:
              if DIRACsite not in rDict[ 'SiteName' ]:#sites_select:
                continue
              DIRACsite_comp = DIRACsite + ' ' + DIRACsite_comp

            record  = []
            country = ( resource[ 0 ] ).split( '.' ).pop()

            record.append( resource[ 0 ] ) #ResourceName
            record.append( resource[ 1 ] ) #ServiceType
            record.append( DIRACsite_comp ) #SiteName
            record.append( resource[ 4 ] ) #ResourceType
            record.append( country ) #Country
            record.append( resource[ 5 ] ) #StatusType
            record.append( resource[ 6 ] ) #Status
            record.append( resource[ 7 ].isoformat(' ') ) #DateEffective
            record.append( resource[ 8 ] ) #FormerStatus
            record.append( resource[ 9 ] ) #Reason
            records.append( record )

          else:
            if DIRACsite not in rDict[ 'SiteName' ]: #sites_select:
              continue
            record  = []
            country = ( resource[ 0 ] ).split( '.' ).pop()

            record.append( resource[ 0 ] ) #ResourceName
            record.append( resource[ 1 ] ) #ServiceType
            record.append( DIRACsite ) #SiteName
            record.append( resource[ 4 ] ) #ResourceType
            record.append( country ) #Country
            record.append( resource[ 5 ] ) #StatusType
            record.append( resource[ 6 ] ) #Status
            record.append( resource[ 7 ].isoformat(' ') ) #DateEffective
            record.append( resource[ 8 ] ) #FormerStatus
            record.append( resource[ 9 ] ) #Reason
            records.append( record )


      elif granularity == 'StorageElement':
        if rDict[ 'SiteName' ] == []:#sites_select == []:
          kw = { 'columns' : [ 'SiteName' ] }
          sites_select = self.getSitesPresent( **kw )
          #sites_select = self.getMonitoredsList( 'Site',
          #                                      paramsList = [ 'SiteName' ] )
          rDict[ 'SiteName' ] = [ x[ 0 ] for x in sites_select[ 'Value' ] ]

        kw = { 'columns' : [ 'GridSiteName' ] }
        gridSites_select = self.getSitesPresent( siteName = rDict[ 'SiteName' ], **kw )
        #gridSites_select = self.getMonitoredsList( 'Site',
        #                                           paramsList = [ 'GridSiteName' ],
        #                                           siteName = rDict[ 'SiteName' ] )
        gridSites_select = [ x[ 0 ] for x in gridSites_select[ 'Value' ] ]

        storageElementsList = self.getStorageElementsPresent( storageElementName = rDict[ 'StorageElementName' ],
                                                              status             = rDict[ 'Status' ],
                                                              gridSiteName       = gridSites_select,
                                                              **kwargs
                                                              )
        #storageElementsList = self.getMonitoredsList( granularity,
        #                                              paramsList         = paramsList,
        #                                              storageElementName = rDict[ 'StorageElementName' ],#storageElements_select,
        #                                              status             = rDict[ 'Status' ],#status_select,
        #                                         #     countries          = rDict[ 'Countries' ],#countries_select,
        #                                              gridSiteName       = gridSites_select )

        for storageElement in storageElementsList[ 'Value' ]:
          DIRACsites = getDIRACSiteName( storageElement[ 2 ] )
          if not DIRACsites[ 'OK' ]:
            raise RSSDBException, 'Error executing getDIRACSiteName'
          DIRACsites = DIRACsites[ 'Value' ]
          DIRACsite_comp = ''
          for DIRACsite in DIRACsites:
            if DIRACsite not in rDict[ 'SiteName' ]:
              continue
            DIRACsite_comp = DIRACsite + ' ' + DIRACsite_comp
          record  = []
          country = ( storageElement[ 1 ] ).split( '.' ).pop()

          record.append( storageElement[ 0 ] ) #StorageElementName
          record.append( storageElement[ 1 ] ) #ResourceName
          record.append( DIRACsite_comp ) #SiteName
          record.append( country ) #Country
          record.append( storageElement[ 3 ] ) #StatusType
          record.append( storageElement[ 4 ] ) #Status
          record.append( storageElement[ 5 ].isoformat(' ') ) #DateEffective
          record.append( storageElement[ 6 ] ) #FormerStatus
          record.append( storageElement[ 7 ] ) #Reason
          records.append( record )

    finalDict = {}
    finalDict[ 'TotalRecords' ]   = len( records )
    finalDict[ 'ParameterNames' ] = paramNames

    # Return all the records if maxItems == 0 or the specified number otherwise
    if maxItems:
      finalDict[ 'Records' ] = records[ startItem:startItem+maxItems ]
    else:
      finalDict[ 'Records' ] = records

    finalDict[ 'Extras' ] = None

    return S_OK( finalDict )    
    
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    