"""
The ResourcesStatusDB module contains a couple of exception classes, and a
class to interact with the ResourceStatus DB.
"""

from datetime import datetime

from DIRAC import S_OK, S_ERROR

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSDBException
from DIRAC.ResourceStatusSystem.Utilities.Utils import where
from DIRAC.ResourceStatusSystem import ValidRes, ValidStatus, ValidStatusTypes

from DIRAC.ResourceStatusSystem.Utilities.MySQLMonkey import MySQLMonkey
from DIRAC.ResourceStatusSystem.Utilities.Validator import ResourceStatusValidator

from DIRAC.ResourceStatusSystem.Utilities.Decorators import CheckExecution

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

    self.mm    = MySQLMonkey()
    self.rsVal = ResourceStatusValidator( self )

  '''
  ##############################################################################
  # ELEMENT FUNCTIONS
  ##############################################################################
  '''

  def __validateElementTableName( self, element ):

    element = element.replace('Status','').replace('History','').replace('Scheduled','').replace('Present','')
    self.rsVal.validateRes( element )

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

    return whereElements

  def __getMultipleWhereElements( self, dict, **kwargs ):

    return self.mm.getWhereElements( dict, **kwargs )

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
    return self.mm.getColumns( columns )

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

  def __addGridRow( self, rDict ):

    self.__addRow( 'GridSite', rDict )

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
    columns = kwargs.pop( 'columns', None )

    whereElements = self.__getMultipleWhereElements( dict, **kwargs )
    cols          = self.__getColumns( columns )
    if sort is not None:
      sort        = self.__getColumns( sort )
    if order is not None:
      order       = self.__getColumns( order )

    return self.__getElement( element, cols, whereElements, sort, order, limit )

  def __getGridElementRow( self, dict, **kwargs ):

    # PARAMS PROCESSED FROM KWARGS !!!
    sort    = kwargs.pop( 'sort',    None )
    order   = kwargs.pop( 'order',   None )
    limit   = kwargs.pop( 'limit',   None )
    columns = kwargs.pop( 'columns', None )

    whereElements = self.__getMultipleWhereElements( dict, **kwargs )
    cols          = self.__getColumns( columns )
    if sort is not None:
      sort        = self.__getColumns( sort )
    if order is not None:
      order       = self.__getColumns( order )

    return self.__getElement( 'GridSite', cols, whereElements, sort, order, limit )

  def __getElementStatusCountRow( self, element, whereElements ):

    #self.__validateRes( element )

    req = "SELECT Status, COUNT(*) from %sPresent" % element
    if whereElements:
      req += " WHERE %s" % whereElements
    req += " GROUP BY Status"

    resQuery = self.db._query( req )
    if not resQuery[ 'OK' ]:
      raise RSSDBException, where( self, self.__getElementStatusCountRow ) + resQuery[ 'Message' ]

    return resQuery

  def __getElementStatusCount( self, element, dict ):

    self.rsVal.validateRes( element )

    whereElements = self.__getMultipleWhereElements( dict )

    resQuery = self.__getElementStatusCountRow( element, whereElements )

    count = { 'Total' : 0 }
    for validStatus in ValidStatus:
      count[ validStatus ] = 0

    for x in resQuery[ 'Value' ]:
      count[ x[0] ] = int( x[1] )

    count['Total'] = sum( count.values() )
    return S_OK( count )

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

    self.rsVal.validateRes( element )

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

      # This three lines make not much sense, but sometimes statusToSet is '',
      # and we need it as a list to work properly
      statusToSet = ValidStatusTypes[ element ][ 'StatusType' ]
      if not isinstance( statusToSet, list ):
        statusToSet = [ statusToSet ]

      rDict = { '%sName' % element : dict[ '%sName' % element ],
                'Status'           : defaultStatus,
                'TokenOwner'       : tokenOwner }

      for statusType in statusToSet:

        rDict[ 'StatusType' ] = statusType

        # Trick to populate ElementHistory table with one entry. This allows
        # us to use PresentElement views ( otherwise they do not work ).
        for defaultReason in defaultReasons:

          rDict[ 'Reason' ] = defaultReason
          self.__setElementStatus( element, rDict )

  def __addOrModifyGridElement( self, rDict ):

    gridSite = self.__getGridElementRow( { 'GridSiteName' : rDict[ 'GridSiteName' ] } )

    if gridSite[ 'Value' ]:
      self.__updateGridRow( rDict )
    else:
      self.__addGridRow( rDict )

  def __setElementStatus( self, element, rDict ):

    # START VALIDATION #
    self.rsVal.validateRes( element )
    self.rsVal.validateSingleElementStatusType( element, rDict['StatusType'] )
    self.rsVal.validateStatus( rDict['Status'] )
    self.rsVal.validateName( rDict['Reason'] )
    self.rsVal.validateSingleDates( rDict )
    # END VALIDATION #

    el = self.__getElementRow( element, { '%sName' % element : rDict[ '%sName' % element ] } )#, None )
    if not el[ 'Value' ]:
      message = '%s "%s" does not exist' % ( element, rDict[ '%sName' % element ] )
      raise RSSDBException, where( self, self.__setElementStatus ) + message

    # END VALIDATION #

    currentStatus = self.__getElementRow( '%sStatus' % element,
                                          {
                                           '%sName' % element : rDict[ '%sName' % element ],
                                           'StatusType'       : rDict[ 'StatusType' ]
                                           } )

    now    = datetime.utcnow()
    never  = datetime( 9999, 12, 31, 23, 59, 59 ).replace( microsecond = 0 )

    rDict[ 'TokenExpiration' ] = ( 1 and ( rDict.has_key('TokenExpiration') and rDict['TokenExpiration'] ) ) or never
    rDict[ 'DateCreated' ]     = ( 1 and ( rDict.has_key('DateCreated')     and rDict['DateCreated']     ) ) or now
    rDict[ 'DateEffective' ]   = ( 1 and ( rDict.has_key('DateEffective')   and rDict['DateEffective']   ) ) or now
    rDict[ 'DateEnd' ]         = ( 1 and ( rDict.has_key('DateEnd')         and rDict['DateEnd']         ) ) or never
    rDict[ 'LastCheckTime' ]   = ( 1 and ( rDict.has_key('LastCheckTime')   and rDict['LastCheckTime']   ) ) or now

    rDict[ 'TokenOwner' ]      = ( 1 and ( rDict.has_key('TokenOwner')      and rDict['TokenOwner']      ) ) or 'RS_SVC'

    if currentStatus[ 'Value' ]:

      self.__updateElementRow( '%sStatus' % element , rDict )

      cS            = currentStatus[ 'Value' ][ 0 ]

      rDict[ 'Status' ]          = cS[ 3 ]
      rDict[ 'Reason' ]          = cS[ 4 ]
      rDict[ 'DateCreated' ]     = cS[ 5 ]
      rDict[ 'DateEffective' ]   = cS[ 6 ]
      rDict[ 'DateEnd' ]         = now
      rDict[ 'LastCheckTime' ]   = cS[ 8 ]
      rDict[ 'TokenOwner' ]      = cS[ 9 ]
      rDict[ 'TokenExpiration' ] = cS[ 10 ]

      self.__addElementRow( '%sHistory' % element , rDict)

    else:

      self.__addElementRow( '%sStatus' % element , rDict )

  def __updateElementStatus( self, element, dict ):

    now = datetime.utcnow()

    # START VALIDATION #
    self.rsVal.validateRes( element )
    self.rsVal.validateSingleDates( dict )
    if dict.has_key( 'Status' ):
      self.rsVal.validateStatus( dict['Status'] )
    self.rsVal.validateSingleElementStatusType( element, dict[ 'StatusType'] )
    self.rsVal.validateSingleDates( dict )

    el = self.__getElementRow( element, { '%sName' % element : dict[ '%sName' % element ] } )#, None )
    if not el[ 'Value' ]:
      message = '%s "%s" does not exist' % ( element, dict[ '%sName' % element ] )
      raise RSSDBException, where( self, self.__updateElementStatus ) + message
    # END VALIDATION #

    rDict = { '%sName' % element : dict[ '%sName' % element ] }
    if dict.has_key( 'StatusType' ):
      rDict[ 'StatusType' ] = dict[ 'StatusType' ]

    dict[ 'LastCheckTime'] = now

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

  def __setElementScheduledStatus( self, element, rDict ):

    # START VALIDATION #
    self.rsVal.validateRes( element )
    self.rsVal.validateSingleElementStatusType( element, rDict['StatusType'])
    self.rsVal.validateStatus( rDict['Status'] )
    self.rsVal.validateName( rDict['Reason'] )
    self.rsVal.validateSingleDates( rDict )

    el = self.__getElementRow( element, { '%sName' % element : rDict[ '%sName' % element ] } )
    if not el[ 'Value' ]:
      message = '%s "%s" does not exist' % ( element, rDict[ '%sName' % element ] )
      raise RSSDBException, where( self, self.__setElementStatus ) + message
    # END VALIDATION #

    # We prevent from users not giving all values.
    znever = datetime.min
    now    = datetime.utcnow()
    never  = datetime( 9999, 12, 31, 23, 59, 59 ).replace( microsecond = 0 )

    rDict[ 'TokenExpiration' ] = ( 1 and ( rDict.has_key('TokenExpiration') and rDict['TokenExpiration'] ) ) or never
    rDict[ 'DateCreated' ]     = ( 1 and ( rDict.has_key('DateCreated')     and rDict['DateCreated']     ) ) or now
    rDict[ 'DateEffective' ]   = ( 1 and ( rDict.has_key('DateEffective')   and rDict['DateEffective']   ) ) or now
    rDict[ 'DateEnd' ]         = ( 1 and ( rDict.has_key('DateEnd')         and rDict['DateEnd']         ) ) or never
    rDict[ 'LastCheckTime' ]   = ( 1 and ( rDict.has_key('LastCheckTime')   and rDict['LastCheckTime']   ) ) or now

    rDict[ 'TokenOwner' ]      = ( 1 and ( rDict.has_key('TokenOwner')      and rDict['TokenOwner']      ) ) or 'RS_SVC'

    self.__addElementRow( '%sScheduledStatus' % element , rDict )

  def __getElements( self, element, rDict, table = None, **kwargs ):

    # START VALIDATION #
    self.rsVal.validateRes( element )
    self.rsVal.validateDates( rDict )
    # END VALIDATION #

    if table is not None:
      element = '%s%s' % ( element, table )

    return self.__getElementRow( element, rDict, **kwargs )

  def __deleteElements( self, element, rDict ):

    # START VALIDATION #
    self.rsVal.validateRes( element )
    # END VALIDATION #
    self.__deleteElementRow( '%sHistory' % element,         rDict )
    self.__deleteElementRow( '%sScheduledStatus' % element, rDict )
    self.__deleteElementRow( '%sStatus' % element,          rDict )
    self.__deleteElementRow( element,                       rDict )

  def __deleteElementHistory( self, element, rDict, **kwargs ):

    # START VALIDATION #
    self.rsVal.validateRes( element )
    self.rsVal.validateDates(rDict)
    # END VALIDATION #
    self.__deleteElementRow( '%sHistory' % element, rDict, **kwargs )

  def __deleteElementsScheduledStatus( self, element, rDict, **kwargs ):
    # START VALIDATION #
    self.rsVal.validateRes( element )
    self.rsVal.validateDates(rDict)
    # END VALIDATION #
    self.__deleteElementRow( '%sScheduledStatus' % element, rDict, **kwargs)

  '''
  ##############################################################################
  # SITE FUNCTIONS
  ##############################################################################
  '''

  @CheckExecution
  def addOrModifySite( self, siteName, siteType, gridSiteName ):

    rDict = self.__generateRowDict( locals() )
    # VALIDATION #
    self.rsVal.validateName(siteName)
    self.rsVal.validateSiteType( siteType )
    self.rsVal.validateGridSite( gridSiteName )
    # END VALIDATION #
    self.__addOrModifyElement( 'Site', rDict )
    return S_OK()

  @CheckExecution
  def setSiteStatus( self, siteName, statusType, status, reason, dateCreated,
                     dateEffective, dateEnd, lastCheckTime, tokenOwner,
                     tokenExpiration ):

    rDict = self.__generateRowDict( locals() )
    self.__setElementStatus( 'Site', rDict )
    return S_OK()

  @CheckExecution
  def setSiteScheduledStatus( self, siteName, statusType, status, reason, dateCreated,
                              dateEffective, dateEnd, lastCheckTime, tokenOwner,
                              tokenExpiration ):

    rDict = self.__generateRowDict( locals() )
    self.__setElementScheduledStatus( 'Site', rDict )
    return S_OK()

  @CheckExecution
  def updateSiteStatus( self, siteName, statusType, status, reason, dateCreated,
                        dateEffective, dateEnd, lastCheckTime, tokenOwner,
                        tokenExpiration ):

    rDict = self.__generateRowDict( locals() )
    self.__updateElementStatus( 'Site' , rDict )
    return S_OK()

  @CheckExecution
  def getSites( self, siteName, siteType, gridSiteName, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Site', rDict, **kwargs )

  @CheckExecution
  def getSitesStatus( self, siteName, statusType, status, reason, dateCreated,
                     dateEffective, dateEnd, lastCheckTime, tokenOwner,
                     tokenExpiration, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Site', rDict, table = 'Status', **kwargs )

  @CheckExecution
  def getSitesHistory( self, siteName, statusType, status, reason, dateCreated,
                       dateEffective, dateEnd, lastCheckTime, tokenOwner,
                       tokenExpiration, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'SiteName', 'SiteHistoryID' ]
    return self.__getElements( 'Site', rDict, table = 'History', **kwargs )

  @CheckExecution
  def getSitesScheduledStatus( self, siteName, statusType, status, reason, dateCreated,
                               dateEffective, dateEnd, lastCheckTime, tokenOwner,
                               tokenExpiration, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Site', rDict, table = 'ScheduledStatus', **kwargs )

  @CheckExecution
  def getSitesPresent( self, siteName, siteType, gridSiteName, gridTier,
                       statusType, status, dateEffective, reason, lastCheckTime,
                       tokenOwner, tokenExpiration, formerStatus, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Site', rDict, table = 'Present', **kwargs )

  @CheckExecution
  def deleteSites( self, siteName ):

    #VALIDATION#
    self.rsVal.validateMultipleNames( siteName )
    # END VALIDATION #

    rDict = self.__generateRowDict( locals() )
    self.__deleteElements( 'Site', rDict)
    return S_OK()

  @CheckExecution
  def deleteSitesScheduledStatus( self, siteName, statusType, status, reason,
                                  dateCreated, dateEffective, dateEnd,
                                  lastCheckTime, tokenOwner, tokenExpiration,
                                  **kwargs ):

    rDict = self.__generateRowDict( locals() )
    self.__deleteElementsScheduledStatus( 'Site', rDict, **kwargs )
    return S_OK()

  @CheckExecution
  def deleteSitesHistory( self, siteName, statusType, status, reason, dateCreated,
                          dateEffective, dateEnd, lastCheckTime, tokenOwner,
                          tokenExpiration, **kwargs ):

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
    self.rsVal.validateName( serviceName )
    self.rsVal.validateServiceType( serviceType )
    self.rsVal.validateSite( siteName )
    # END VALIDATION #

    self.__addOrModifyElement( 'Service', rDict)
    return S_OK()

  @CheckExecution
  def setServiceStatus( self, serviceName, statusType, status, reason, dateCreated,
                        dateEffective, dateEnd, lastCheckTime,tokenOwner,
                        tokenExpiration ):

    rDict = self.__generateRowDict( locals() )
    self.__setElementStatus( 'Service', rDict )
    return S_OK()

  @CheckExecution
  def setServiceScheduledStatus( self, serviceName, statusType, status,
                                 reason, dateCreated, dateEffective, dateEnd,
                                 lastCheckTime, tokenOwner, tokenExpiration ):

    rDict = self.__generateRowDict( locals() )
    self.__setElementScheduledStatus( 'Service', rDict )
    return S_OK()

  @CheckExecution
  def updateServiceStatus( self, serviceName, statusType, status, reason,
                           dateCreated, dateEffective, dateEnd, lastCheckTime,
                           tokenOwner, tokenExpiration ):

    rDict = self.__generateRowDict( locals() )
    self.__updateElementStatus( 'Service', rDict )
    return S_OK()

  @CheckExecution
  def getServices( self, serviceName, serviceType, siteName, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Service', rDict, **kwargs )

  @CheckExecution
  def getServicesStatus( self, serviceName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Service', rDict, table = 'Status', **kwargs )

  @CheckExecution
  def getServicesHistory( self, serviceName, statusType, status, reason, dateCreated,
                          dateEffective, dateEnd, lastCheckTime, tokenOwner,
                          tokenExpiration, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'ServiceName', 'ServiceHistoryID' ]
    return self.__getElements( 'Service', rDict, table = 'History', **kwargs )

  @CheckExecution
  def getServicesScheduledStatus( self, serviceName, statusType, status, reason,
                                  dateCreated, dateEffective, dateEnd, lastCheckTime,
                                  tokenOwner, tokenExpiration, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Service', rDict, table = 'ScheduledStatus', **kwargs )

  @CheckExecution
  def getServicesPresent( self, serviceName, siteName, siteType, serviceType,
                          statusType, status, dateEffective, reason, lastCheckTime,
                          tokenOwner, tokenExpiration, formerStatus, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Service', rDict, table = 'Present', **kwargs )

  @CheckExecution
  def deleteServices( self, serviceName ):

    #VALIDATION#
    self.rsVal.validateMultipleNames( serviceName )
    # END VALIDATION #

    rDict = self.__generateRowDict( locals() )
    self.__deleteElements( 'Service', rDict)
    return S_OK()

  @CheckExecution
  def deleteServicesScheduledStatus( self, serviceName, statusType, status,
                                     reason, dateCreated, dateEffective, dateEnd,
                                     lastCheckTime, tokenOwner, tokenExpiration,
                                     **kwargs ):

    rDict = self.__generateRowDict( locals() )
    self.__deleteElementsScheduledStatus( 'Service', rDict, **kwargs )
    return S_OK()

  @CheckExecution
  def deleteServicesHistory( self, serviceName, statusType, status, reason, dateCreated,
                             dateEffective, dateEnd, lastCheckTime, tokenOwner,
                             tokenExpiration, **kwargs ):

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
    self.rsVal.validateResourceType( resourceType )
    self.rsVal.validateServiceType( serviceType )
#    self.rsVal.validateSite( siteName )
    self.rsVal.validateGridSite( gridSiteName )
    # END VALIDATION #

    self.__addOrModifyElement( 'Resource', rDict )
    return S_OK()

  @CheckExecution
  def setResourceStatus( self, resourceName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,tokenExpiration ):

    rDict = self.__generateRowDict( locals() )
    self.__setElementStatus( 'Resource', rDict )
    return S_OK()

  @CheckExecution
  def setResourceScheduledStatus( self, resourceName, statusType, status, reason,
                                  dateCreated, dateEffective, dateEnd, lastCheckTime,
                                  tokenOwner, tokenExpiration ):

    rDict = self.__generateRowDict( locals() )
    self.__setElementScheduledStatus( 'Resource', rDict )
    return S_OK()

  @CheckExecution
  def updateResourceStatus( self, resourceName, statusType, status, reason, dateCreated,
                            dateEffective, dateEnd, lastCheckTime, tokenOwner,
                            tokenExpiration ):

    rDict = self.__generateRowDict( locals() )
    self.__updateElementStatus( 'Resource', rDict )
    return S_OK()

  @CheckExecution
  def getResources( self, resourceName, resourceType, serviceType, siteName,
                    gridSiteName, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Resource', rDict, **kwargs )

  @CheckExecution
  def getResourcesStatus( self, resourceName, statusType, status, reason, dateCreated,
                          dateEffective, dateEnd, lastCheckTime, tokenOwner,
                          tokenExpiration, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Resource', rDict, table = 'Status', **kwargs )

  @CheckExecution
  def getResourcesHistory( self, resourceName, statusType, status, reason, dateCreated,
                           dateEffective, dateEnd, lastCheckTime, tokenOwner,
                           tokenExpiration, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'ResourceName', 'ResourceHistoryID' ]
    return self.__getElements( 'Resource', rDict, table = 'History', **kwargs )

  @CheckExecution
  def getResourcesScheduledStatus( self, resourceName, statusType, status, reason,
                                   dateCreated, dateEffective, dateEnd, lastCheckTime,
                                   tokenOwner, tokenExpiration, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Resource', rDict, table = 'ScheduledStatus', **kwargs )

  @CheckExecution
  def getResourcesPresent( self, resourceName, siteName, serviceType, gridSiteName,
                           siteType, resourceType, statusType, status, dateEffective,
                           reason, lastCheckTime, tokenOwner, tokenExpiration,
                           formerStatus, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'Resource', rDict, table = 'Present', **kwargs )

  @CheckExecution
  def deleteResources( self, resourceName ):

    #VALIDATION#
    self.rsVal.validateMultipleNames( resourceName )
    # END VALIDATION #

    rDict = self.__generateRowDict( locals() )
    self.__deleteElements( 'Resource', rDict)
    return S_OK()

  @CheckExecution
  def deleteResourcesScheduledStatus( self, resourceName, statusType, status,
                                      reason, dateCreated, dateEffective, dateEnd,
                                      lastCheckTime, tokenOwner, tokenExpiration,
                                      **kwargs ):

    rDict = self.__generateRowDict( locals() )
    self.__deleteElementsScheduledStatus( 'Resource', rDict, **kwargs )
    return S_OK()

  @CheckExecution
  def deleteResourcesHistory( self, resourceName, statusType, status, reason, dateCreated,
                              dateEffective, dateEnd, lastCheckTime, tokenOwner,
                              tokenExpiration, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    self.__deleteElementHistory( 'Resource', rDict, **kwargs )
    return S_OK()

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
    self.rsVal.validateResource( resourceName )
    self.rsVal.validateGridSite( gridSiteName )
    # END VALIDATION #

    self.__addOrModifyElement( 'StorageElement', rDict )
    return S_OK()

  @CheckExecution
  def setStorageElementStatus( self, storageElementName, statusType, status,
                               reason, dateCreated, dateEffective, dateEnd,
                               lastCheckTime, tokenOwner, tokenExpiration ):

    rDict = self.__generateRowDict( locals() )
    self.__setElementStatus( 'StorageElement', rDict )
    return S_OK()

  @CheckExecution
  def setStorageElementScheduledStatus( self, storageElementName, statusType, status,
                                        reason, dateCreated, dateEffective, dateEnd,
                                        lastCheckTime, tokenOwner, tokenExpiration ):

    rDict = self.__generateRowDict( locals() )
    self.__setElementScheduledStatus( 'StorageElement', rDict )
    return S_OK()

  @CheckExecution
  def updateStorageElementStatus( self, storageElementName, statusType, status,
                                  reason, dateCreated, dateEffective, dateEnd,
                                  lastCheckTime, tokenOwner, tokenExpiration ):

    rDict = self.__generateRowDict( locals() )
    self.__updateElementStatus( 'StorageElement', rDict )
    return S_OK()

  @CheckExecution
  def getStorageElements( self, storageElementName, resourceName, gridSiteName, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'StorageElement', rDict, **kwargs )

  @CheckExecution
  def getStorageElementsStatus( self, storageElementName, statusType, status,
                                reason, dateCreated, dateEffective, dateEnd,
                                lastCheckTime, tokenOwner, tokenExpiration, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'StorageElement', rDict, table = 'Status', **kwargs )

  @CheckExecution
  def getStorageElementsHistory( self, storageElementName, statusType, status,
                                 reason, dateCreated, dateEffective, dateEnd,
                                 lastCheckTime, tokenOwner, tokenExpiration, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'StorageElementName', 'StorageElementHistoryID' ]
    return self.__getElements( 'StorageElement', rDict, table = 'History', **kwargs )

  @CheckExecution
  def getStorageElementsScheduledStatus( self, storageElementName, statusType,
                                         status, reason, dateCreated, dateEffective,
                                         dateEnd, lastCheckTime, tokenOwner,
                                         tokenExpiration, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'StorageElement', rDict, table = 'ScheduledStatus', **kwargs )

  @CheckExecution
  def getStorageElementsPresent( self, storageElementName, resourceName,
                                 gridSiteName, siteType, statusType, status,
                                 dateEffective, reason, lastCheckTime, tokenOwner,
                                 tokenExpiration, formerStatus, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getElements( 'StorageElement', rDict, table = 'Present', **kwargs )

  @CheckExecution
  def deleteStorageElements( self, storageElementName ):

    #VALIDATION#
    self.rsVal.validateMultipleNames( storageElementName )
    # END VALIDATION #

    rDict = self.__generateRowDict( locals() )
    self.__deleteElements( 'StorageElement', rDict)
    return S_OK()

  @CheckExecution
  def deleteStorageElementsScheduledStatus( self, storageElementName, statusType,
                                            status, reason, dateCreated,
                                            dateEffective, dateEnd, lastCheckTime,
                                            tokenOwner, tokenExpiration, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    self.__deleteElementsScheduledStatus( 'StorageElement', rDict, **kwargs )
    return S_OK()

  @CheckExecution
  def deleteStorageElementsHistory( self, storageElementName, statusType,
                                    status, reason, dateCreated, dateEffective,
                                    dateEnd, lastCheckTime, tokenOwner, tokenExpiration,
                                    **kwargs ):

    rDict = self.__generateRowDict( locals() )
    self.__deleteElementHistory( 'StorageElement', rDict, **kwargs )
    return S_OK()

  '''
  ##############################################################################
  # STATS FUNCTIONS
  ##############################################################################
  '''

  @CheckExecution
  def getServiceStats( self, siteName, statusType ):
    """
    Returns simple statistics of active, probing, bad and banned services of a site;

    :params:
      :attr:`siteName`: string - a site name

    :returns:
      { 'Active':xx, 'Probing':yy, 'Bad':vv, 'Banned':zz, 'Total':xyz }
    """

#    rDict = { 'SiteName' : siteName }
#
#    if statusType is not None:
#      self.__validateElementStatusTypes( 'Service', statusType )
#      rDict[ 'StatusType'] = statusType
    rDict = self.__generateRowDict( locals() )
    return self.__getElementStatusCount( 'Service', rDict )

  @CheckExecution
  def getResourceStats( self, element, name, statusType ):

    rDict = {}

    if statusType is not None:
      self.rsVal.validateElementStatusTypes( 'Service', statusType )
      rDict[ 'StatusType'] = statusType

#    resourceDict = {}
    resourceName, resourceType, serviceType, siteName, gridSiteName = None, None, None, None, None

    if element == 'Site':
      #name   = self.getGridSiteName( element, name )[ 'Value' ]
      #rDict[ 'GridSiteName' ] = name
#      resourceDict = { 'siteName' : name }
      siteName = name
      #resourceNames = [ sn[0] for sn in self.getResources( siteName = name )[ 'Value' ] ]
##      rDict[ 'ResourceName' ] = resourceNames

    elif element == 'Service':

      serviceType = name.split( '@' )[ 0 ]
      siteName    = name.split( '@' )[ 1 ]

      if serviceType == 'Computing':
#        resourceDict = { 'siteName' : siteName }
        siteName = siteName
        #resourceName = [ sn[0] for sn in self.getResources( siteName = siteName )[ 'Value' ] ]
##        rDict[ 'ResourceName' ] = resourceNames
        #rDict[ 'SiteName' ] = name
      else:
        #gridSiteName =
        #rDict[ 'GridSiteName' ] = gridSiteName
#        resourceDict = { 'gridSiteName' : gridSiteName, 'serviceType' : serviceType }
        kwargs = { 'columns' : [ 'GridSiteName' ] }
        gridSiteName = [ gs[0] for gs in self.getSites( siteName, None, None, **kwargs )[ 'Value' ] ]
        #gridSiteName = [ gs[0] for gs in self.getGridSiteName( 'Site', siteName )[ 'Value' ] ]
        #serviceType  = serviceType
        siteName = None
        #resourceName = [ sn[0] for sn in self.getResources( None, None, serviceType, None,gridSiteName )[ 'Value' ] ]
        #rDict[ 'SiteName' ] = siteNames
##        rDict[ 'ResourceName' ] = resourceNames
        #rDict[ 'ServiceType' ]  = serviceType

    else:
      message = '%s is non accepted element. Only Site or Service' % element
      return S_ERROR( message )
#      raise RSSDBException, where( self, self.getResourceStats ) + message

    resourceArgs = ( resourceName, resourceType, serviceType, siteName, gridSiteName )
    rDict[ 'ResourceName' ] = [ re[0] for re in self.getResources( *resourceArgs )[ 'Value' ] ]

    return self.__getElementStatusCount( 'Resource', rDict )

  @CheckExecution
  def getStorageElementStats( self, element, name, statusType ):

    rDict = {}

    if statusType is not None:
      self.rsVal.validateElementStatusTypes( 'StorageElement', statusType )
      rDict[ 'StatusType'] = statusType

    storageElementName, resourceName, gridSiteName = None, None, None

    if element == 'Site':
      #rDict[ 'GridSiteName' ] = self.getGridSiteName( element, name )[ 'Value' ]
      kwargs = { 'columns' : [ 'GridSiteName' ] }
      gridSiteName = [ gs[0] for gs in self.getSites( name, None, None, **kwargs )[ 'Value' ] ]
      #gridSiteName = [ gs[0] for gs in self.getGridSiteName( element, name )[ 'Value' ] ]
#      seDict = { 'gridSiteName' : gridSiteName }
      ##siteNames = [ sn[0] for sn in self.getSites( gridSiteName = gridSiteName )[ 'Value' ] ]
      ##rDict[ 'SiteName' ] = siteNames
      #seNames = [ sn[0] for sn in self.getStorageElements( gridSiteName = gridSiteName )[ 'Value' ] ]
#      rDict[ 'StorageElementName' ] = seNames

    elif element == 'Resource':
      #rDict[ 'ResourceName' ] = name
#      seDict = { 'resourceName' : name }
      resourceName = name
      #seNames = [ sn[0] for sn in self.getStorageElements( resourceName = name )[ 'Value' ] ]
#      rDict[ 'StorageElementName' ] = seNames

    else:
      message = '%s is non accepted element. Only Site or Resource' % element
      return S_ERROR( message )

    seArgs = ( storageElementName, resourceName, gridSiteName )
    rDict[ 'StorageElementName' ] = [ se[0] for se in self.getStorageElements( *seArgs )[ 'Value' ] ]

    return self.__getElementStatusCount( 'StorageElement', rDict )

  '''
  ##############################################################################
  # GRID SITE FUNCTIONS
  ##############################################################################
  '''

  @CheckExecution
  def addOrModifyGridSite( self, gridSiteName, gridTier ):

    # VALIDATION #
    self.rsVal.validateName( gridSiteName )
    self.rsVal.validateGridSiteType( gridTier )
    # END VALIDATION #

    rDict = self.__generateRowDict( locals() )

    self.__addOrModifyGridElement( rDict )
    return S_OK()

  @CheckExecution
  def getGridSites( self, gridSiteName, gridTier, **kwargs ):

    rDict = self.__generateRowDict( locals() )
    return self.__getGridElementRow( rDict, **kwargs )

  @CheckExecution
  def deleteGridSites( self, gridSiteName ):

    #VALIDATION#
    self.rsVal.validateMultipleNames( gridSiteName )
    # END VALIDATION #

    rDict = self.__generateRowDict( locals() )

    whereElements = self.__getMultipleWhereElements( rDict )
    self.__deleteRow( 'GridSite', whereElements )
    return S_OK()

  '''
  ##############################################################################
  # MISC FUNCTIONS
  ##############################################################################
  '''

  # Check the booster ResourceStatusSystem.Utilities.ResourceStatusBooster
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
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
