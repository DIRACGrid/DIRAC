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
 
  __TABLES__ = {
                'GridSite'                      : {'uniqueKeys' : [ 'GridSiteName' ] },
                'Site'                          : {'uniqueKeys' : [ 'SiteName' ] },
                'SiteStatus'                    : {'uniqueKeys' : [ 'SiteName', 'StatusType' ] },
                'SiteScheduledStatus'           : {'uniqueKeys' : [ 'SiteName', 'StatusType', 'DateEffective' ] },
                'SiteHistory'                   : {'uniqueKeys' : [ 'SiteName', 'StatusType', 'DateEnd' ] },
                'SitePresent'                   : {'uniqueKeys' : [ 'SiteName', 'StatusType' ] },                                   
                'Service'                       : {'uniqueKeys' : [ 'ServiceName' ] },
                'ServiceStatus'                 : {'uniqueKeys' : [ 'ServiceName', 'StatusType' ] },
                'ServiceScheduledStatus'        : {'uniqueKeys' : [ 'ServiceName', 'StatusType', 'DateEffective' ] },
                'ServiceHistory'                : {'uniqueKeys' : [ 'ServiceName', 'StatusType', 'DateEnd' ] },
                'ServicePresent'                : {'uniqueKeys' : [ 'ServiceName', 'StatusType' ]},
                'Resource'                      : {'uniqueKeys' : [ 'ResourceName' ] },
                'ResourceStatus'                : {'uniqueKeys' : [ 'ResourceName', 'StatusType' ] },
                'ResourceScheduledStatus'       : {'uniqueKeys' : [ 'ResourceName', 'StatusType', 'DateEffective' ] },
                'ResourceHistory'               : {'uniqueKeys' : [ 'ResourceName', 'StatusType', 'DateEnd' ] },
                'ResourcePresent'               : {'uniqueKeys' : [ 'ResourceName', 'StatusType' ] },   
                'StorageElement'                : {'uniqueKeys' : [ 'StorageElement' ] },
                'StorageElementStatus'          : {'uniqueKeys' : [ 'StorageElementName', 'StatusType' ] },
                'StorageElementScheduledStatus' : {'uniqueKeys' : [ 'StorageElementName', 'StatusType', 'DateEffective' ] },
                'StorageElementHistory'         : {'uniqueKeys' : [ 'StorageElementName', 'StatusType', 'DateEnd' ] },
                'StorageElementPresent'         : {'uniqueKeys' : [ 'StorageElementName', 'StatusType' ] }                               
                }

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
    self.mm    = MySQLMonkey( self )  
    self.rsVal = ResourceStatusValidator( self )

  '''
  ##############################################################################
  # ELEMENT FUNCTIONS
  ##############################################################################
  '''

#  def __validateElementTableName( self, element ):
#
#    element = element.replace('Status','').replace('History','').replace('Scheduled','').replace('Present','')
#    self.rsVal.validateRes( element )

#  def __getWhereElements( self, element, dict ):
#
#    if element in ValidRes:
#      elements = [ '%sName' % element ]
#    elif element.replace( 'Status', '' ) in ValidRes:
#      elements = [ '%sName' % element.replace( 'Status', '' )]
#      if dict.has_key( 'StatusType' ):
#        elements.append( 'StatusType' )
#    elif element.replace( 'History', '') in ValidRes:
#      elements = [ '%sName' % element.replace( 'History', '' ), 'DateEnd' ]#'StatusType', 'DateEnd' ]
#      if dict.has_key( 'StatusType' ):
#        elements.append( 'StatusType' )
#    else:
#      message = '%s is a wrong element' % element
#      raise RSSDBException, where( self, self.__getWhereElements ) + message
#
#    newDict = {}
#    for el in elements:
#      newDict[ el ] = dict[ el ]
#
#    whereElements = self.__getMultipleWhereElements( newDict )
#
#    return whereElements

  def __getMultipleWhereElements( self, dict, **kwargs ):

    return self.mm.getWhereElements( dict, **kwargs )

#  def __getElementUniqueKeys( self, element ):
#
#    if element in ValidRes:
#      elements = [ '%sName' % element ]
#    elif element.replace( 'Status', '' ) in ValidRes:
#      elements = [ '%sName' % element.replace( 'Status', '' ), 'StatusType']
#    elif element.replace( 'History', '') in ValidRes:
#      elements = [ '%sName' % element.replace( 'History', '' ), 'StatusType', 'DateEnd' ]
#    else:
#      message = '%s is a wrong element' % element
#      raise RSSDBException, where( self, self.__getElementUniqueKeys ) + message
#
#    return elements

#  def __getColumns( self, columns ):
#    return self.mm.getColumns( columns )  

#  def __addRow( self, element, dict ):
#    
#    req = "INSERT INTO %s (" % element
#    req += ','.join( "%s" % key for key in dict.keys())
#    req += ") VALUES ("
#    req += ','.join( "'%s'" % value for value in dict.values())
#
#    req += ")"   
#    sqlQuery = self.db._update( req )
#
#    #sqlQuery = self.mm.insert( element, rDict )
#
#    if not sqlQuery[ 'OK' ]:
#      raise RSSDBException, where( self, self.__addRow ) + sqlQuery[ 'Message' ]
        
    #req += ")"

    #resUpdate = self.db._update( req )
    #if not resUpdate[ 'OK' ]:
    #  raise RSSDBException, where( self, self.__addElementRow ) + resUpdate[ 'Message' ]


#  def __addElementRow( self, element, dict ):
#
#    self.__validateElementTableName( element )
#    self.__addRow( element, dict )

#  def __addGridRow( self, rDict ):
#
#    self.__addRow( 'GridSite', rDict )

#  def __getElement2( self, element, rDict, **kwargs ):
#    
#    sqlQuery = self.mm.select( element, rDict, **kwargs )
#    if not sqlQuery[ 'OK' ]:
#      raise RSSDBException, where( self, self.__getElement ) + sqlQuery[ 'Message' ]
#
#    return S_OK( [ list(rQ) for rQ in sqlQuery[ 'Value' ]] )

#  def __getElement( self, element, cols, whereElements, sort, order, limit ):
#
#    req = "SELECT %s from %s" % ( cols, element )
#    if whereElements:
#      req += " WHERE %s" % whereElements
#    if sort:
#      req += " ORDER BY %s" % sort
#      if order:
#        req += " %s" % order
#    if limit:
#      req += " LIMIT %d" % limit
#
#    resQuery = self.db._query( req )
#    if not resQuery[ 'OK' ]:
#      raise RSSDBException, where( self, self.__getElement ) + resQuery[ 'Message' ]
#
#    return S_OK( [ list(rQ) for rQ in resQuery[ 'Value' ]] )

#  def __getElementRow( self, element, dict, **kwargs ):
#                        #sort = None, order = None, limit = None ):
#
#    self.__validateElementTableName( element )
#
#    # PARAMS PROCESSED FROM KWARGS !!!
#    sort    = kwargs.pop( 'sort',    None )
#    order   = kwargs.pop( 'order',   None )
#    limit   = kwargs.pop( 'limit',   None )
#    columns = kwargs.pop( 'columns', None )
#
#    whereElements = self.__getMultipleWhereElements( dict, **kwargs )
#    cols          = self.__getColumns( columns )
#    if sort is not None:
#      sort        = self.__getColumns( sort )
#    if order is not None:
#      order       = self.__getColumns( order )
#
#    return self.__getElement( element, cols, whereElements, sort, order, limit )

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

#  def __updateRow( self, element, dict, uniqueKeys, whereElements ):
#
#    req = "UPDATE %s SET " % element
#    req += ','.join( "%s='%s'" % (key,value) for (key,value) in dict.items() if (key not in uniqueKeys) )
#    req += " WHERE %s" % whereElements
#
#    resUpdate = self.db._update( req )
#    if not resUpdate[ 'OK' ]:
#      raise RSSDBException, where( self, self.__updateRow ) + resUpdate[ 'Message' ]

#  def __updateElementRow( self, element, dict ):
#
#    self.__validateElementTableName( element )
#
#    uniqueKeys    = self.__getElementUniqueKeys( element )
#    whereElements = self.__getWhereElements( element, dict )
#
#    self.__updateRow( element, dict,  uniqueKeys, whereElements )
#
#  def __updateGridRow( self, dict ):
#
#    uniqueKeys    = [ 'GridSiteName' ]
#    whereElements = 'GridSiteName = "%s"' % dict[ 'GridSiteName' ]
#
#    self.__updateRow( 'GridSite', dict, uniqueKeys, whereElements )
#
#  def __deleteRow( self, element, whereElements ):
#
#    req = "DELETE from %s" % element
#    if whereElements is not None:
#      req += " WHERE %s" % whereElements
#
#    resDel = self.db._update( req )
#    if not resDel[ 'OK' ]:
#      raise RSSDBException, where( self, self.__deleteRow ) + resDel[ 'Message' ]
#
#  def __deleteElementRow( self, element, dict, **kwargs ):
#
#    self.__validateElementTableName( element )
#
#    whereElements = self.__getMultipleWhereElements( dict, **kwargs )
#    self.__deleteRow(element, whereElements)

#  def __addOrModifyElement( self, element, dict ):
#
#    self.rsVal.validateRes( element )
#
#    kwargs = { 'columns' : [ '%sName' % element ] }
#    elemnt = self.__getElementRow( element,
#                                   {
#                                    '%sName' % element : dict[ '%sName' % element ]
#                                    },
#                                    **kwargs
#                                    #: '%sName' % element
#                                  )
#
#    if elemnt[ 'Value' ]:
#      self.__updateElementRow( element, dict )
#    else:
#      # If we add a new site, we set the new Site with status 'Banned'
#      self.__addElementRow( element, dict )
#
#      defaultStatus  = 'Banned'
#      defaultReasons = [ 'Added to DB', 'Init' ]
#      tokenOwner     = 'RS_SVC'
#
#      # This three lines make not much sense, but sometimes statusToSet is '',
#      # and we need it as a list to work properly
#      statusToSet = ValidStatusTypes[ element ][ 'StatusType' ]
#      if not isinstance( statusToSet, list ):
#        statusToSet = [ statusToSet ]
#
#      rDict = { '%sName' % element : dict[ '%sName' % element ],
#                'Status'           : defaultStatus,
#                'TokenOwner'       : tokenOwner }
#
#      for statusType in statusToSet:
#
#        rDict[ 'StatusType' ] = statusType
#
#        # Trick to populate ElementHistory table with one entry. This allows
#        # us to use PresentElement views ( otherwise they do not work ).
#        for defaultReason in defaultReasons:
#
#          rDict[ 'Reason' ] = defaultReason
#          self.__setElementStatus( element, rDict )

################################################################################
################################################################################
# SUB PUBLIC FUNCTIONS
################################################################################
################################################################################

  def _addOrModifyElement( self, rDict, **kwargs ):
    
    sqlQuery = self.mm.select( rDict, **kwargs )
    
    if sqlQuery[ 'Value' ]:      
      return self.mm.update( rDict, **kwargs )
    else: 
      sqlQuery = self.mm.insert( rDict, **kwargs )
      if sqlQuery[ 'OK' ]:       
        self.__setInitStatus( rDict, **kwargs )
      else:
        return sqlQuery       

################################################################################

  def _addOrModifyElementStatus( self, rDict, **kwargs ):

    # START VALIDATION #
    if rDict.has_key( 'Status' ):
      self.rsVal.validateStatus( rDict['Status'] )
    if rDict.has_key( 'Reason' ):
      self.rsVal.validateName( rDict['Reason'] )
    self.rsVal.validateSingleDates( rDict )
    # END VALIDATION #

    rDict, now = self.__setStatusDefaults( rDict )

    sqlQuery = self.mm.select( rDict, **kwargs )
    
    if not sqlQuery[ 'Value' ]:
      return self.mm.insert( rDict, **kwargs )
    
#    if sqlQuery[ 'Value' ]:
    updateSQLQuery = self.mm.update( rDict, **kwargs )
    if not sqlQuery[ 'OK' ]:
      return updateSQLQuery 

    sqlQ                       = sqlQuery[ 'Value' ][ 0 ]
    
    rDict[ 'Status' ]          = sqlQ[ 3 ]
    rDict[ 'Reason' ]          = sqlQ[ 4 ]
    rDict[ 'DateCreated' ]     = sqlQ[ 5 ]
    rDict[ 'DateEffective' ]   = sqlQ[ 6 ]
    rDict[ 'DateEnd' ]         = now
    rDict[ 'LastCheckTime' ]   = sqlQ[ 8 ]
    rDict[ 'TokenOwner' ]      = sqlQ[ 9 ]
    rDict[ 'TokenExpiration' ] = sqlQ[ 10 ]

    kwargs.update( { 'table' : kwargs[ 'table' ].replace( 'Status', 'History' ) } )
    return self.mm.insert( rDict, **kwargs )
  
################################################################################

#  def _updateElementStatus( self, rDict, **kwargs ):
#
#    # START VALIDATION #
#    self.rsVal.validateSingleDates( rDict )
#    if rDict.has_key( 'Status' ):
#      self.rsVal.validateStatus( dict['Status'] )
#    self.rsVal.validateSingleDates( rDict )
#    # END VALIDATION #
#
#    now = datetime.utcnow()
#    #We force LastCheckTime to now
#    rDict[ 'LastCheckTime'] = now
#
#    # We keep a copy before the update, to add a history row
#    sqlQuery = self.mm.select( rDict, **kwargs )
#    if not sqlQuery[ 'OK' ]:
#      return sqlQuery
#
#    # THIS MUST FAIL IF WE TRY TO UPDATE SOMETHING THAT IS NOT ON THE DB    
#    #    if sqlQuery[ 'Value' ]:
#    updateSQLQuery = self.mm.update( rDict, **kwargs )
#    if not updateSQLQuery[ 'OK']:
#      return updateSQLQuery
#    
#    sqlQ                        = sqlQuery[ 'Value' ][ 0 ]    
#    rDict[ 'Status' ]           = sqlQ[ 3 ]
#    rDict[ 'Reason' ]           = sqlQ[ 4 ]
#    rDict[ 'DateCreated' ]      = sqlQ[ 5 ]
#    rDict[ 'DateEffective' ]    = sqlQ[ 6 ]
#    rDict[ 'DateEnd' ]          = now # cSs[ 7 ]
#    rDict[ 'LastCheckTime' ]    = sqlQ[ 8 ]
#    rDict[ 'TokenOwner' ]       = sqlQ[ 9 ]
#    rDict[ 'TokenExpiration' ]  = sqlQ[ 10 ]
#
#    #We store any modification on the Status
#    kwargs.update( { 'table' : kwargs[ 'table' ].replace( 'Status', 'History' ) } )
#    return self.mm.insert( rDict, **kwargs )

################################################################################
    
  def _addOrModifyElementScheduledStatus( self, rDict, **kwargs ):

    # START VALIDATION #
    self.rsVal.validateStatus( rDict['Status'] )
    self.rsVal.validateName( rDict['Reason'] )
    self.rsVal.validateSingleDates( rDict )
    # END VALIDATION #
 
    # We prevent from users not giving all values.
    rDict, _now = self.__setStatusDefaults( rDict )

    sqlQuery = self.mm.select( rDict, **kwargs )
    if sqlQuery[ 'Value' ]:
      return self.mm.update( rDict, **kwargs )
    else:  
      return self.mm.insert( rDict, **kwargs )    

################################################################################

  def _deleteElement( self, rDict, **kwargs ):
    
    element = kwargs[ 'table' ]
    
    for table in [ 'History', 'ScheduledStatus', 'Status', '' ]:
      
      kwargs[ 'table' ] = '%s%s' % ( element, table )
      sqlQuery = self.mm.delete( rDict, **kwargs )
      
      if not sqlQuery[ 'OK' ]:
        return sqlQuery
    
    return S_OK()
    
################################################################################
################################################################################    

################################################################################
################################################################################
# AUXILIAR FUNCTIONS
################################################################################
################################################################################

  def __setInitStatus( self, rDict, **kwargs ):

    defaultStatus  = 'Banned'
    defaultReasons = [ 'Added to DB', 'Init' ]
    tokenOwner     = 'RS_SVC'

    # This three lines make not much sense, but sometimes statusToSet is '',
    # and we need it as a list to work properly
    statusToSet = ValidStatusTypes[ kwargs[ 'table' ] ][ 'StatusType' ]
    
    if not isinstance( statusToSet, list ):
      statusToSet = [ statusToSet ]

    rDict = { '%sName' % kwargs[ 'table' ] : rDict[ '%sName' % kwargs[ 'table' ] ],
              'Status'                     : defaultStatus,
              'TokenOwner'                 : tokenOwner }

    element           = kwargs[ 'table' ]
    kwargs[ 'table' ] = '%sStatus' % element
    
    for statusType in statusToSet:

      rDict[ 'StatusType' ] = statusType
      # Trick to populate ElementHistory table with one entry. This allows
      # us to use PresentElement views ( otherwise they do not work ).
      for defaultReason in defaultReasons:

        rDict[ 'Reason' ] = defaultReason
        sqlQuery = self._addOrModifyElementStatus( element, rDict, **kwargs )
        
        if not sqlQuery[ 'OK' ]:
          return sqlQuery
        
    return S_OK()      

################################################################################
    
  def __setStatusDefaults( self, rDict ):
     
    now    = datetime.utcnow()
    never  = datetime( 9999, 12, 31, 23, 59, 59 ).replace( microsecond = 0 )

    rDict[ 'TokenExpiration' ] = ( 1 and ( rDict.has_key('TokenExpiration') and rDict['TokenExpiration'] ) ) or never
    rDict[ 'DateCreated' ]     = ( 1 and ( rDict.has_key('DateCreated')     and rDict['DateCreated']     ) ) or now
    rDict[ 'DateEffective' ]   = ( 1 and ( rDict.has_key('DateEffective')   and rDict['DateEffective']   ) ) or now
    rDict[ 'DateEnd' ]         = ( 1 and ( rDict.has_key('DateEnd')         and rDict['DateEnd']         ) ) or never
    rDict[ 'LastCheckTime' ]   = ( 1 and ( rDict.has_key('LastCheckTime')   and rDict['LastCheckTime']   ) ) or now
    rDict[ 'TokenOwner' ]      = ( 1 and ( rDict.has_key('TokenOwner')      and rDict['TokenOwner']      ) ) or 'RS_SVC'
    
    return rDict, now     

################################################################################
################################################################################

#  def __getElements( self, element, rDict, table = None, **kwargs ):
#
#    # START VALIDATION #
#    self.rsVal.validateRes( element )
#    self.rsVal.validateDates( rDict )
#    # END VALIDATION #
#
#    if table is not None:
#      element = '%s%s' % ( element, table )
#
#    return self.__getElementRow( element, rDict, **kwargs )
#
#  def __deleteElements( self, element, rDict ):
#
#    # START VALIDATION #
#    self.rsVal.validateRes( element )
#    # END VALIDATION #
#    self.__deleteElementRow( '%sHistory' % element,         rDict )
#    self.__deleteElementRow( '%sScheduledStatus' % element, rDict )
#    self.__deleteElementRow( '%sStatus' % element,          rDict )
#    self.__deleteElementRow( element,                       rDict )
#
#  def __deleteElementHistory( self, element, rDict, **kwargs ):
#
#    # START VALIDATION #
#    self.rsVal.validateRes( element )
#    self.rsVal.validateDates(rDict)
#    # END VALIDATION #
#    self.__deleteElementRow( '%sHistory' % element, rDict, **kwargs )
#
#  def __deleteElementsScheduledStatus( self, element, rDict, **kwargs ):
#    # START VALIDATION #
#    self.rsVal.validateRes( element )
#    self.rsVal.validateDates(rDict)
#    # END VALIDATION #
#    self.__deleteElementRow( '%sScheduledStatus' % element, rDict, **kwargs)

  '''
  ##############################################################################
  # SITE FUNCTIONS
  ##############################################################################
  '''

  @CheckExecution
  def addOrModifySite( self, siteName, siteType, gridSiteName, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateName( siteName )
    self.rsVal.validateSiteType( siteType )
    self.rsVal.validateGridSite( gridSiteName )
    # END VALIDATION #
   
    return self._addOrModifyElement( rDict, **kwargs )

  @CheckExecution
  def addOrModifySiteStatus( self, siteName, statusType, status, reason, dateCreated,
                             dateEffective, dateEnd, lastCheckTime, tokenOwner,
                             tokenExpiration, **kwargs ):
  
    rDict = self.mm.localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateName( siteName )
    self.rsVal.validateSingleElementStatusType( 'Site', rDict[ 'StatusType' ] )
    # END VALIDATION #
       
    print rDict   
       
    return self._addOrModifyElementStatus( rDict, **kwargs )
    
  @CheckExecution
  def addOrModifySiteScheduledStatus( self, siteName, statusType, status, reason, dateCreated,
                                      dateEffective, dateEnd, lastCheckTime, tokenOwner,
                                      tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateName( siteName )
    self.rsVal.validateSingleElementStatusType( 'Site', rDict[ 'StatusType' ] )
    # END VALIDATION #
    
    return self._addOrModifyElementScheduledStatus( rDict, **kwargs )

  @CheckExecution
  def getSite( self, siteName, siteType, gridSiteName, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def getSiteStatus( self, siteName, statusType, status, reason, dateCreated,
                     dateEffective, dateEnd, lastCheckTime, tokenOwner,
                     tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def getSiteHistory( self, siteName, statusType, status, reason, dateCreated,
                      dateEffective, dateEnd, lastCheckTime, tokenOwner,
                      tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'SiteName', 'SiteHistoryID' ]
    return self.mm.get( rDict, **kwargs )  

  @CheckExecution
  def getSiteScheduledStatus( self, siteName, statusType, status, reason, dateCreated,
                              dateEffective, dateEnd, lastCheckTime, tokenOwner,
                              tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )  

  @CheckExecution
  def getSitePresent( self, siteName, siteType, gridSiteName, gridTier,
                      statusType, status, dateEffective, reason, lastCheckTime,
                      tokenOwner, tokenExpiration, formerStatus, **kwargs ):

    rDict = self.mm.localsToDict( locals() )   
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def deleteSite( self, siteName, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateMultipleNames( siteName )
    # END VALIDATION #

    return self._deleteElement( rDict, **kwargs )

  @CheckExecution
  def deleteSiteScheduledStatus( self, siteName, statusType, status, reason,
                                 dateCreated, dateEffective, dateEnd,
                                 lastCheckTime, tokenOwner, tokenExpiration,
                                 **kwargs ):
    
    rDict = self.mm.localsToDict( locals() )  
    return self.mm.delete( rDict, **kwargs )

  @CheckExecution
  def deleteSiteHistory( self, siteName, statusType, status, reason, dateCreated,
                          dateEffective, dateEnd, lastCheckTime, tokenOwner,
                          tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # SERVICE FUNCTIONS
  ##############################################################################
  '''

  @CheckExecution
  def addOrModifyService( self, serviceName, serviceType, siteName, **kwargs ):

    rDict = self.mm.localsToDict( locals() )

    # START VALIDATION #
    self.rsVal.validateName( serviceName )
    self.rsVal.validateServiceType( serviceType )
    self.rsVal.validateSite( siteName )
    # END VALIDATION #

    return self._addOrModifyElement( rDict, **kwargs )

  @CheckExecution
  def addOrModifyServiceStatus( self, serviceName, statusType, status, reason, dateCreated,
                                dateEffective, dateEnd, lastCheckTime,tokenOwner,
                                tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateName( serviceName )
    self.rsVal.validateSingleElementStatusType( 'Service', rDict[ 'StatusType' ] )
    # END VALIDATION #
       
    return self._addOrModifyElementStatus( rDict, **kwargs )    

  @CheckExecution
  def addOrModifyServiceScheduledStatus( self, serviceName, statusType, status,
                                         reason, dateCreated, dateEffective, dateEnd,
                                         lastCheckTime, tokenOwner, tokenExpiration,
                                         **kwargs ):
    
    rDict = self.mm.localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateName( serviceName )
    self.rsVal.validateSingleElementStatusType( 'Service', rDict[ 'StatusType' ] )
    # END VALIDATION #
    
    return self._addOrModifyElementScheduledStatus( rDict, **kwargs )

  @CheckExecution
  def getService( self, serviceName, serviceType, siteName, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def getServiceStatus( self, serviceName, statusType, status, reason, dateCreated,
                        dateEffective, dateEnd, lastCheckTime, tokenOwner,
                        tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def getServiceHistory( self, serviceName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'ServiceName', 'ServiceHistoryID' ]
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def getServiceScheduledStatus( self, serviceName, statusType, status, reason,
                                 dateCreated, dateEffective, dateEnd, lastCheckTime,
                                 tokenOwner, tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def getServicePresent( self, serviceName, siteName, siteType, serviceType,
                         statusType, status, dateEffective, reason, lastCheckTime,
                         tokenOwner, tokenExpiration, formerStatus, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def deleteService( self, serviceName, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    #VALIDATION#
    self.rsVal.validateMultipleNames( serviceName )
    # END VALIDATION #

    return self._deleteElement( rDict, **kwargs )

  @CheckExecution
  def deleteServiceScheduledStatus( self, serviceName, statusType, status,
                                     reason, dateCreated, dateEffective, dateEnd,
                                     lastCheckTime, tokenOwner, tokenExpiration,
                                     **kwargs ):

    rDict = self.mm.localsToDict( locals() )  
    return self.mm.delete( rDict, **kwargs )

  @CheckExecution
  def deleteServiceHistory( self, serviceName, statusType, status, reason, dateCreated,
                             dateEffective, dateEnd, lastCheckTime, tokenOwner,
                             tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )  
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # RESOURCE FUNCTIONS
  ##############################################################################
  '''

  @CheckExecution
  def addOrModifyResource( self, resourceName, resourceType, serviceType, siteName,
                           gridSiteName, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    
    # START VALIDATION #
    self.rsVal.validateName( resourceName )  
    self.rsVal.validateResourceType( resourceType )
    self.rsVal.validateServiceType( serviceType )
    # Not used, some resources have NULL site !!
#    self.rsVal.validateSite( siteName )
    self.rsVal.validateGridSite( gridSiteName )
    # END VALIDATION #

    return self._addOrModifyElement( rDict, **kwargs )

  @CheckExecution
  def addOrModifyResourceStatus( self, resourceName, statusType, status, reason, 
                                 dateCreated, dateEffective, dateEnd, lastCheckTime, 
                                 tokenOwner,tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateName( resourceName )
    self.rsVal.validateSingleElementStatusType( 'Resource', rDict[ 'StatusType' ] )
    # END VALIDATION #
       
    return self._addOrModifyElementStatus( rDict, **kwargs )    

  @CheckExecution
  def addOrModifyResourceScheduledStatus( self, resourceName, statusType, status, 
                                          reason, dateCreated, dateEffective, dateEnd, 
                                          lastCheckTime, tokenOwner, tokenExpiration,
                                          **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateName( resourceName )
    self.rsVal.validateSingleElementStatusType( 'Resource', rDict[ 'StatusType' ] )
    # END VALIDATION #
    
    return self._addOrModifyElementScheduledStatus( rDict, **kwargs )

  @CheckExecution
  def getResource( self, resourceName, resourceType, serviceType, siteName,
                   gridSiteName, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def getResourceStatus( self, resourceName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def getResourceHistory( self, resourceName, statusType, status, reason, dateCreated,
                          dateEffective, dateEnd, lastCheckTime, tokenOwner,
                          tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'ResourceName', 'ResourceHistoryID' ]
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def getResourceScheduledStatus( self, resourceName, statusType, status, reason,
                                  dateCreated, dateEffective, dateEnd, lastCheckTime,
                                  tokenOwner, tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def getResourcePresent( self, resourceName, siteName, serviceType, gridSiteName,
                          siteType, resourceType, statusType, status, dateEffective,
                          reason, lastCheckTime, tokenOwner, tokenExpiration,
                          formerStatus, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def deleteResource( self, resourceName, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    #VALIDATION#
    self.rsVal.validateMultipleNames( resourceName )
    # END VALIDATION #

    return self._deleteElement( rDict, **kwargs )
  
  @CheckExecution
  def deleteResourceScheduledStatus( self, resourceName, statusType, status,
                                     reason, dateCreated, dateEffective, dateEnd,
                                     lastCheckTime, tokenOwner, tokenExpiration,
                                     **kwargs ):

    rDict = self.mm.localsToDict( locals() )  
    return self.mm.delete( rDict, **kwargs )

  @CheckExecution
  def deleteResourceHistory( self, resourceName, statusType, status, reason, dateCreated,
                             dateEffective, dateEnd, lastCheckTime, tokenOwner,
                             tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )  
    return self.mm.delete( rDict, **kwargs )
  
  '''
  ##############################################################################
  # STORAGE ELEMENT FUNCTIONS
  ##############################################################################
  '''

  @CheckExecution
  def addOrModifyStorageElement( self, storageElementName, resourceName,
                                 gridSiteName, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    
    # START VALIDATION #
    self.rsVal.validateName( storageElementName )  
    self.rsVal.validateResource( resourceName )
    self.rsVal.validateGridSite( gridSiteName )
    # END VALIDATION #

    return self._addOrModifyElement( rDict, **kwargs )

  @CheckExecution
  def addOrModifyStorageElementStatus( self, storageElementName, statusType, status,
                                       reason, dateCreated, dateEffective, dateEnd,
                                       lastCheckTime, tokenOwner, tokenExpiration,
                                       **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateName( storageElementName )
    self.rsVal.validateSingleElementStatusType( 'StorageElement', rDict[ 'StatusType' ] )
    # END VALIDATION #
       
    return self._addOrModifyElementStatus( rDict, **kwargs )   

  @CheckExecution
  def addOrModifyStorageElementScheduledStatus( self, storageElementName, statusType, status,
                                                reason, dateCreated, dateEffective, dateEnd,
                                                lastCheckTime, tokenOwner, tokenExpiration,
                                                **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateName( storageElementName )
    self.rsVal.validateSingleElementStatusType( 'StorageElement', rDict[ 'StatusType' ] )
    # END VALIDATION #
    
    return self._addOrModifyElementScheduledStatus( rDict, **kwargs )

  @CheckExecution
  def getStorageElement( self, storageElementName, resourceName, gridSiteName, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def getStorageElementStatus( self, storageElementName, statusType, status,
                                reason, dateCreated, dateEffective, dateEnd,
                                lastCheckTime, tokenOwner, tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def getStorageElementHistory( self, storageElementName, statusType, status,
                                 reason, dateCreated, dateEffective, dateEnd,
                                 lastCheckTime, tokenOwner, tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'StorageElementName', 'StorageElementHistoryID' ]
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def getStorageElementScheduledStatus( self, storageElementName, statusType,
                                         status, reason, dateCreated, dateEffective,
                                         dateEnd, lastCheckTime, tokenOwner,
                                         tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def getStorageElementPresent( self, storageElementName, resourceName,
                                 gridSiteName, siteType, statusType, status,
                                 dateEffective, reason, lastCheckTime, tokenOwner,
                                 tokenExpiration, formerStatus, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def deleteStorageElement( self, storageElementName, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    #VALIDATION#
    self.rsVal.validateMultipleNames( storageElementName )
    # END VALIDATION #

    return self._deleteElement( rDict, **kwargs )

  @CheckExecution
  def deleteStorageElementScheduledStatus( self, storageElementName, statusType,
                                            status, reason, dateCreated,
                                            dateEffective, dateEnd, lastCheckTime,
                                            tokenOwner, tokenExpiration, **kwargs ):

    rDict = self.mm.localsToDict( locals() )  
    return self.mm.delete( rDict, **kwargs )

  @CheckExecution
  def deleteStorageElementHistory( self, storageElementName, statusType,
                                    status, reason, dateCreated, dateEffective,
                                    dateEnd, lastCheckTime, tokenOwner, tokenExpiration,
                                    **kwargs ):

    rDict = self.mm.localsToDict( locals() )  
    return self.mm.delete( rDict, **kwargs )

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
    rDict = self.mm.localsToDict( locals() )
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
  def addOrModifyGridSite( self, gridSiteName, gridTier, **kwargs ):

    # VALIDATION #
    self.rsVal.validateName( gridSiteName )
    self.rsVal.validateGridSiteType( gridTier )
    # END VALIDATION #

    rDict    = self.mm.localsToDict( locals() )
    sqlQuery = self.mm.select( rDict, **kwargs )
    
    if sqlQuery[ 'Value' ]:      
      return self.mm.update( rDict, **kwargs )
    else: 
      return self.mm.insert( rDict, **kwargs )  

  @CheckExecution
  def getGridSite( self, gridSiteName, gridTier, **kwargs ):

    rDict = self.mm.localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def deleteGridSite( self, gridSiteName, **kwargs ):

    #VALIDATION#
    self.rsVal.validateMultipleNames( gridSiteName )
    # END VALIDATION #

    rDict = self.mm.localsToDict( locals() )
    return self.mm.delete( rDict, **kwargs )


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
