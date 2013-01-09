# $HeadURL:  $
''' ResourceManagementClient

  Client to interact with the ResourceManagementDB.

'''

from DIRAC                                              import gLogger, S_ERROR 
from DIRAC.Core.DISET.RPCClient                         import RPCClient

__RCSID__ = '$Id:  $'

class ResourceManagementClient( object ):
  """
  The :class:`ResourceManagementClient` class exposes the :mod:`DIRAC.ResourceManagement` 
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
  XML-RPC server ( namely :class:`ResourceManagementDB` and 
  :class:`ResourceManagementHancler` ).

  You can use this client on this way

   >>> from DIRAC.ResourceManagementSystem.Client.ResourceManagementClient import ResourceManagementClient
   >>> rsClient = ResourceManagementClient()
   
  All functions calling methods exposed on the database or on the booster are 
  making use of some syntactic sugar, in this case a decorator that simplifies
  the client considerably.  
  """
  
  def __init__( self , serviceIn = None ):
    '''
    The client tries to connect to :class:ResourceManagementDB by default. If it 
    fails, then tries to connect to the Service :class:ResourceManagementHandler.
    '''
        
    if not serviceIn:
      self.gate = RPCClient( "ResourceStatus/ResourceManagement" )    
    else:
      self.gate = serviceIn    

  ##############################################################################
  # ACCOUNTING CACHE METHODS

#  def insertAccountingCache( self, name, plotType, plotName, result, 
#                             dateEffective, lastCheckTime, meta = None ):
#    '''
#    Inserts on AccountingCache a new row with the arguments given.
#    
#    :Parameters:
#      **name** - `string`
#        name of an individual of the grid topology  
#      **plotType** - `string`
#        the plotType name (e.g. 'Pilot')
#      **plotName** - `string`
#        the plot name
#      **result** - `string`
#        command result
#      **dateEffective** - `datetime`
#        time-stamp from which the result is effective
#      **lastCheckTime** - `datetime`
#        time-stamp setting last time the result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self._query( 'insert', 'AccountingCache', locals() )
#  def updateAccountingCache( self, name, plotType, plotName, result, 
#                             dateEffective, lastCheckTime, meta = None ):
#    '''
#    Updates AccountingCache with the parameters given. By default, `name`, 
#    `plotType` and `plotName` will be the parameters used to select the row.
#    
#    :Parameters:
#      **name** - `string`
#        name of an individual of the grid topology  
#      **plotType** - `string`
#        the plotType name (e.g. 'Pilot')
#      **plotName** - `string`
#        the plot name
#      **result** - `string`
#        command result
#      **dateEffective** - `datetime`
#        time-stamp from which the result is effective
#      **lastCheckTime** - `datetime`
#        time-stamp setting last time the result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self._query( 'update', 'AccountingCache', locals() )
  def selectAccountingCache( self, name = None, plotType = None, plotName = None, 
                             result = None, dateEffective = None, 
                             lastCheckTime = None, meta = None ):
    '''
    Gets from PolicyResult all rows that match the parameters given.
    
    :Parameters:
      **name** - `[, string, list]`
        name of an individual of the grid topology  
      **plotType** - `[, string, list]`
        the plotType name (e.g. 'Pilot')
      **plotName** - `[, string, list]`
        the plot name
      **result** - `[, string, list]`
        command result
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the result is effective
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'select', 'AccountingCache', locals() )
#  def deleteAccountingCache( self, name = None, plotType = None, 
#                             plotName = None, result = None, 
#                             dateEffective = None, lastCheckTime = None, 
#                             meta = None ):
#    '''
#    Deletes from PolicyResult all rows that match the parameters given.
#    
#    :Parameters:
#      **name** - `[, string, list]`
#        name of an individual of the grid topology  
#      **plotType** - `[, string, list]`
#        the plotType name (e.g. 'Pilot')
#      **plotName** - `[, string, list]`
#        the plot name
#      **result** - `[, string, list]`
#        command result
#      **dateEffective** - `[, datetime, list]`
#        time-stamp from which the result is effective
#      **lastCheckTime** - `[, datetime, list]`
#        time-stamp setting last time the result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self._query( 'delete', 'AccountingCache', locals() )
  def addOrModifyAccountingCache( self, name = None, plotType = None, 
                                  plotName = None, result = None, 
                                  dateEffective = None, lastCheckTime = None,
                                  meta = None ):
    '''
    Adds or updates-if-duplicated to AccountingCache. Using `name`, `plotType` 
    and `plotName` to query the database, decides whether to insert or update the 
    table.
    
    :Parameters:
      **name** - `string`
        name of an individual of the grid topology  
      **plotType** - `string`
        the plotType name (e.g. 'Pilot')
      **plotName** - `string`
        the plot name
      **result** - `string`
        command result
      **dateEffective** - `datetime`
        time-stamp from which the result is effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'AccountingCache', locals() )    
#  def addIfNotThereAccountingCache( self, name = None, plotType = None, 
#                                    plotName = None, result = None, 
#                                    dateEffective = None, lastCheckTime = None,
#                                    meta = None ):
#    '''
#    Adds if not there to AccountingCache. Using `name`, `plotType` and `plotName` 
#    to query the database, decides whether to insert or not.
#    
#    :Parameters:
#      **name** - `string`
#        name of an individual of the grid topology  
#      **plotType** - `string`
#        the plotType name (e.g. 'Pilot')
#      **plotName** - `string`
#        the plot name
#      **result** - `string`
#        command result
#      **dateEffective** - `datetime`
#        time-stamp from which the result is effective
#      **lastCheckTime** - `datetime`
#        time-stamp setting last time the result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    meta = { 'onlyUniqueKeys' : True }
#    return self._query( 'addIfNotThere', 'AccountingCache', locals() )     

  ##############################################################################
  # GGUSTickets CACHE Methods
  #FIXME: only one method

  def selectGGUSTicketsCache( self, gocSite = None, link = None, openTickets = None, 
                              tickets = None, lastCheckTime = None, meta = None ):
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'select', 'GGUSTicketsCache', locals() )

  def deleteGGUSTicketsCache( self, gocSite = None, link = None, openTickets = None, 
                              tickets = None, lastCheckTime = None, meta = None ):
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'delete', 'GGUSTicketsCache', locals() )

  def addOrModifyGGUSTicketsCache( self, gocSite = None, link = None, 
                                   openTickets = None, tickets = None, 
                                   lastCheckTime = None, meta = None ):
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'GGUSTicketsCache', locals() )    

  ##############################################################################
  # DOWNTIME CACHE Methods

#  def insertDowntimeCache( self, downtimeID, element, name, startDate, endDate, severity,
#                           description, link, dateEffective, lastCheckTime, meta = None ):
#    '''
#    Inserts on DowntimeCache a new row with the arguments given.
#    
#    :Parameters:
#      **downtimeID** - `string`
#        unique id for the downtime 
#      **element** - `string`
#        valid element in the topology ( Site, Resource, Node )
#      **name** - `string`
#        name of the element where the downtime applies
#      **startDate** - `datetime`
#        starting time for the downtime
#      **endDate** - `datetime`
#        ending time for the downtime    
#      **severity** - `string`
#        severity assigned by the gocdb
#      **description** - `string`
#        brief description of the downtime
#      **link** - `string`
#        url to the details    
#      **dateEffective** - `datetime`
#        time when the entry was created in this database  
#      **lastCheckTime** - `datetime`
#        time-stamp setting last time the result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument    
#    # pylint: disable-msg=W0613
#    return self._query( 'insert', 'DowntimeCache', locals() )
#  def updateDowntimeCache( self, downtimeID, element, name, startDate, endDate, severity,
#                           description, link, dateEffective, lastCheckTime, meta = None ):
#    '''
#    Updates AccountingCache with the parameters given. By default, `downtimeID` will 
#    be the parameter used to select the row.
#    
#    :Parameters:
#      **downtimeID** - `string`
#        unique id for the downtime 
#      **element** - `string`
#        valid element in the topology ( Site, Resource, Node )
#      **name** - `string`
#        name of the element where the downtime applies
#      **startDate** - `datetime`
#        starting time for the downtime
#      **endDate** - `datetime`
#        ending time for the downtime    
#      **severity** - `string`
#        severity assigned by the gocdb
#      **description** - `string`
#        brief description of the downtime
#      **link** - `string`
#        url to the details    
#      **dateEffective** - `datetime`
#        time when the entry was created in this database  
#      **lastCheckTime** - `datetime`
#        time-stamp setting last time the result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument    
#    # pylint: disable-msg=W0613
#    return self._query( 'update', 'DowntimeCache', locals() )
  def selectDowntimeCache( self, downtimeID = None, element = None, name = None, 
                           startDate = None, endDate = None, severity = None,
                           description = None, link = None, dateEffective = None, 
                           lastCheckTime = None, meta = None ):
    '''
    Gets from DowntimeCache all rows that match the parameters given.
    
    :Parameters:
      **downtimeID** - [, `string`, `list`]
        unique id for the downtime 
      **element** - [, `string`, `list`]
        valid element in the topology ( Site, Resource, Node )
      **name** - [, `string`, `list`]
        name of the element where the downtime applies
      **startDate** - [, `datetime`, `list`]
        starting time for the downtime
      **endDate** - [, `datetime`, `list`]
        ending time for the downtime    
      **severity** - [, `string`, `list`]
        severity assigned by the gocdb
      **description** - [, `string`, `list`]
        brief description of the downtime
      **link** - [, `string`, `list`]
        url to the details    
      **dateEffective** - [, `datetime`, `list`]
        time when the entry was created in this database  
      **lastCheckTime** - [, `datetime`, `list`]
        time-stamp setting last time the result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument    
    # pylint: disable-msg=W0613
    return self._query( 'select', 'DowntimeCache', locals() )
  def deleteDowntimeCache( self, downtimeID = None, element = None, name = None, 
                           startDate = None, endDate = None, severity = None,
                           description = None, link = None, dateEffective = None, 
                           lastCheckTime = None, meta = None ):
    '''
    Deletes from DowntimeCache all rows that match the parameters given.
    
    :Parameters:
      **downtimeID** - [, `string`, `list`]
        unique id for the downtime 
      **element** - [, `string`, `list`]
        valid element in the topology ( Site, Resource, Node )
      **name** - [, `string`, `list`]
        name of the element where the downtime applies
      **startDate** - [, `datetime`, `list`]
        starting time for the downtime
      **endDate** - [, `datetime`, `list`]
        ending time for the downtime    
      **severity** - [, `string`, `list`]
        severity assigned by the gocdb
      **description** - [, `string`, `list`]
        brief description of the downtime
      **link** - [, `string`, `list`]
        url to the details    
      **dateEffective** - [, `datetime`, `list`]
        time when the entry was created in this database  
      **lastCheckTime** - [, `datetime`, `list`]
        time-stamp setting last time the result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument    
    # pylint: disable-msg=W0613
    return self._query( 'delete', 'DowntimeCache', locals() )
  def addOrModifyDowntimeCache( self, downtimeID = None, element = None, name = None, 
                                startDate = None, endDate = None, severity = None,
                                description = None, link = None, dateEffective = None, 
                                lastCheckTime = None, meta = None ):
    '''
    Adds or updates-if-duplicated to DowntimeCache. Using `downtimeID` to query 
    the database, decides whether to insert or update the table.
    
    :Parameters:
      **downtimeID** - `string`
        unique id for the downtime 
      **element** - `string`
        valid element in the topology ( Site, Resource, Node )
      **name** - `string`
        name of the element where the downtime applies
      **startDate** - `datetime`
        starting time for the downtime
      **endDate** - `datetime`
        ending time for the downtime    
      **severity** - `string`
        severity assigned by the gocdb
      **description** - `string`
        brief description of the downtime
      **link** - `string`
        url to the details    
      **dateEffective** - `datetime`
        time when the entry was created in this database  
      **lastCheckTime** - `datetime`
        time-stamp setting last time the result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()    
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'DowntimeCache', locals() )    
#  def addIfNotThereDowntimeCache( self, downtimeID = None, element = None, name = None, 
#                                  startDate = None, endDate = None, severity = None,
#                                  description = None, link = None, dateEffective = None, 
#                                  lastCheckTime = None, meta = None ):
#    '''
#    Adds if not there to DowntimeCache. Using `downtimeID` to query the database, 
#    decides whether to insert or not.
#    
#    :Parameters:
#      **downtimeID** - `string`
#        unique id for the downtime 
#      **element** - `string`
#        valid element in the topology ( Site, Resource, Node )
#      **name** - `string`
#        name of the element where the downtime applies
#      **startDate** - `datetime`
#        starting time for the downtime
#      **endDate** - `datetime`
#        ending time for the downtime    
#      **severity** - `string`
#        severity assigned by the gocdb
#      **description** - `string`
#        brief description of the downtime
#      **link** - `string`
#        url to the details    
#      **dateEffective** - `datetime`
#        time when the entry was created in this database  
#      **lastCheckTime** - `datetime`
#        time-stamp setting last time the result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()   
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    meta = { 'onlyUniqueKeys' : True }
#    return self._query( 'addIfNotThere', 'DowntimeCache', locals() )  

  ##############################################################################
  # JOB CACHE Methods

#  def insertJobCache( self, site, maskStatus, efficiency, status, lastCheckTime, 
#                      meta = None ):
#    '''
#    Inserts on JobCache a new row with the arguments given.
#    
#    :Parameters:
#      **site** - `string`
#        name of the site element 
#      **maskStatus** - `string`
#        maskStatus for the site
#      **efficiency** - `float`
#        job efficiency ( successful / total )
#      **status** - `string`
#        status for the site computed  
#      **lastCheckTime** - `datetime`
#        time-stamp setting last time the result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument    
#    # pylint: disable-msg=W0613
#    return self._query( 'insert', 'JobCache', locals() )
#  def updateJobCache( self, site, maskStatus, efficiency, status, lastCheckTime,
#                      meta = None ):
#    '''
#    Updates JobCache with the parameters given. By default, `site` will 
#    be the parameter used to select the row.
#    
#    :Parameters:
#      **site** - `string`
#        name of the site element 
#      **maskStatus** - `string`
#        maskStatus for the site
#      **efficiency** - `float`
#        job efficiency ( successful / total )
#      **status** - `string`
#        status for the site computed  
#      **lastCheckTime** - `datetime`
#        time-stamp setting last time the result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument    
#    # pylint: disable-msg=W0613
#    return self._query( 'update', 'JobCache', locals() )
  def selectJobCache( self, site = None, maskStatus = None, efficiency = None, 
                      status = None, lastCheckTime = None, meta = None ):
    '''
    Gets from JobCache all rows that match the parameters given.
    
    :Parameters:
      **site** - `[, string, list ]`
        name of the site element 
      **maskStatus** - `[, string, list ]`
        maskStatus for the site
      **efficiency** - `[, float, list ]`
        job efficiency ( successful / total )
      **status** - `[, string, list ]`
        status for the site computed  
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp setting last time the result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument    
    # pylint: disable-msg=W0613
    return self._query( 'select', 'JobCache', locals() )
  def deleteJobCache( self, site = None, maskStatus = None, efficiency = None, 
                      status = None, lastCheckTime = None, meta = None ):
    '''
    Deletes from JobCache all rows that match the parameters given.
    
    :Parameters:
      **site** - `[, string, list ]`
        name of the site element 
      **maskStatus** - `[, string, list ]`
        maskStatus for the site
      **efficiency** - `[, float, list ]`
        job efficiency ( successful / total )
      **status** - `[, string, list ]`
        status for the site computed  
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp setting last time the result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument    
    # pylint: disable-msg=W0613
    return self._query( 'delete', 'JobCache', locals() )
  def addOrModifyJobCache( self, site = None, maskStatus = None, efficiency = None, 
                           status = None, lastCheckTime = None, meta = None ):
    '''
    Adds or updates-if-duplicated to JobCache. Using `site` to query 
    the database, decides whether to insert or update the table.
    
    :Parameters:
      **site** - `[, string, list ]`
        name of the site element 
      **maskStatus** - `[, string, list ]`
        maskStatus for the site
      **efficiency** - `[, float, list ]`
        job efficiency ( successful / total )
      **status** - `[, string, list ]`
        status for the site computed  
      **lastCheckTime** - `[, datetime, list ]`
        time-stamp setting last time the result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'JobCache', locals() )
#  def addIfNotThereJobCache( self, site = None, maskStatus = None,
#                             efficiency = None, status = None, lastCheckTime = None, 
#                             meta = None ):
#    '''
#    Adds if not there to JobCache. Using `site` to query the database, 
#    decides whether to insert or not.
#    
#    :Parameters:
#      **site** - `[, string, list ]`
#        name of the site element 
#      **maskStatus** - `[, string, list ]`
#        maskStatus for the site
#      **efficiency** - `[, float, list ]`
#        job efficiency ( successful / total )
#      **status** - `[, string, list ]`
#        status for the site computed  
#      **lastCheckTime** - `[, datetime, list ]`
#        time-stamp setting last time the result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    meta = { 'onlyUniqueKeys' : True }
#    return self._query( 'addIfNotThere', 'JobCache', locals() )

  ##############################################################################
  # TRANSFER CACHE Methods

#  def insertTransferCache( self, elementName, direction, metric, value, lastCheckTime,
#                           meta = None ):
#    '''
#    Inserts on TransferCache a new row with the arguments given.
#    
#    :Parameters:
#      **elementName** - `string`
#        name of the element 
#      **direction** - `string`
#        the element taken as Source or Destination of the transfer
#      **metric** - `string`
#        measured quality of failed transfers
#      **value** - `float`
#        percentage  
#      **lastCheckTime** - `datetime`
#        time-stamp setting last time the result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument    
#    # pylint: disable-msg=W0613
#    return self._query( 'insert', 'TransferCache', locals() )
#  def updateTransferCache( self, elementName, direction, metric, value, lastCheckTime,
#                           meta = None ):
#    '''
#    Updates TransferCache with the parameters given. By default, `elementName`,
#    `direction` and `metric` will be the parameter used to select the row.
#    
#    :Parameters:
#      **elementName** - `string`
#        name of the element 
#      **direction** - `string`
#        the element taken as Source or Destination of the transfer
#      **metric** - `string`
#        measured quality of failed transfers
#      **value** - `float`
#        percentage  
#      **lastCheckTime** - `datetime`
#        time-stamp setting last time the result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument    
#    # pylint: disable-msg=W0613
#    return self._query( 'update', 'TransferCache', locals() )
  def selectTransferCache( self, sourceName = None, destinationName = None, metric = None, 
                           value = None, lastCheckTime = None, meta = None ):
    '''
#    Gets from TransferCache all rows that match the parameters given.
#    
#    :Parameters:
#      **elementName** - `[, string, list ]`
#        name of the element 
#      **direction** - `[, string, list ]`
#        the element taken as Source or Destination of the transfer
#      **metric** - `[, string, list ]`
#        measured quality of failed transfers
#      **value** - `[, float, list ]`
#        percentage  
#      **lastCheckTime** - `[, float, list ]`
#        time-stamp setting last time the result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
    '''
    # Unused argument    
    # pylint: disable-msg=W0613
    return self._query( 'select', 'TransferCache', locals() )
  def deleteTransferCache( self, sourceName = None, destinationName = None, metric = None, 
                           value = None, lastCheckTime = None, meta = None ):
    '''
#    Deletes from TransferCache all rows that match the parameters given.
#    
#    :Parameters:
#      **elementName** - `[, string, list ]`
#        name of the element 
#      **direction** - `[, string, list ]`
#        the element taken as Source or Destination of the transfer
#      **metric** - `[, string, list ]`
#        measured quality of failed transfers
#      **value** - `[, float, list ]`
#        percentage  
#      **lastCheckTime** - `[, float, list ]`
#        time-stamp setting last time the result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
    '''
    # Unused argument    
    # pylint: disable-msg=W0613
    return self._query( 'delete', 'TransferCache', locals() )
  def addOrModifyTransferCache( self, sourceName = None, destinationName = None, 
                                metric = None, value = None, lastCheckTime = None, 
                                meta = None ):
    '''
#    Adds or updates-if-duplicated to TransferCache. Using `elementName`, `direction`
#    and `metric` to query the database, decides whether to insert or update the table.
#    
#    :Parameters:
#      **elementName** - `string`
#        name of the element 
#      **direction** - `string`
#        the element taken as Source or Destination of the transfer
#      **metric** - `string`
#        measured quality of failed transfers
#      **value** - `float`
#        percentage  
#      **lastCheckTime** - `datetime`
#        time-stamp setting last time the result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'TransferCache', locals() )
#  def addIfNotThereTransferCache( self, sourceName = None, destinationName = None, 
#                                  metric = None, value = None, lastCheckTime = None, 
#                                  meta = None ):
#    '''
##    Adds if not there to TransferCache. Using `elementName`, `direction` and `metric` 
##    to query the database, decides whether to insert or not.
##    
##    :Parameters:
##      **elementName** - `string`
##        name of the element 
##      **direction** - `string`
##        the element taken as Source or Destination of the transfer
##      **metric** - `string`
##        measured quality of failed transfers
##      **value** - `float`
##        percentage  
##      **lastCheckTime** - `datetime`
##        time-stamp setting last time the result was checked
##      **meta** - `[, dict]`
##        meta-data for the MySQL query. It will be filled automatically with the\
##       `table` key and the proper table name.
##
##    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    meta = { 'onlyUniqueKeys' : True }
#    return self._query( 'addIfNotThere', 'TransferCache', locals() )

  ##############################################################################
  # PILOT CACHE Methods

#  def insertPilotCache( self, site, cE, pilotsPerJob, pilotJobEff, status, lastCheckTime,
#                        meta = None ):
#    '''
#    Inserts on PilotCache a new row with the arguments given.
#    
#    :Parameters:
#      **site** - `string`
#        name of the site 
#      **cE** - `string`
#        name of the CE of 'Multiple' if all site CEs are considered
#      **pilotsPerJob** - `float`
#        measure calculated
#      **pilotJobEff** - `float`
#        percentage  
#      **status** - `string`
#        status of the CE / Site  
#      **lastCheckTime** - `datetime`
#        measure calculated
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument    
#    # pylint: disable-msg=W0613
#    return self._query( 'insert', 'PilotCache', locals() )
#  def updatePilotCache( self, site, cE, pilotsPerJob, pilotJobEff, status, lastCheckTime, 
#                        meta = None ):
#    '''
#    Updates PilotCache with the parameters given. By default, `site` and `cE`
#    will be the parameters used to select the row.
#    
#    :Parameters:
#      **site** - `string`
#        name of the site 
#      **cE** - `string`
#        name of the CE of 'Multiple' if all site CEs are considered
#      **pilotsPerJob** - `float`
#        measure calculated
#      **pilotJobEff** - `float`
#        percentage  
#      **status** - `string`
#        status of the CE / Site  
#      **lastCheckTime** - `datetime`
#        measure calculated
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument    
#    # pylint: disable-msg=W0613
#    return self._query( 'update', 'PilotCache', locals() )
  def selectPilotCache( self, site = None, cE = None, pilotsPerJob = None, 
                        pilotJobEff = None, status = None, lastCheckTime = None,
                        meta = None ): 
    '''
    Gets from TransferCache all rows that match the parameters given.
    
    :Parameters:
      **site** - `[, string, list ]`
        name of the site 
      **cE** - `[, string, list ]`
        name of the CE of 'Multiple' if all site CEs are considered
      **pilotsPerJob** - `[, float, list ]`
        measure calculated
      **pilotJobEff** - `[, float, list ]`
        percentage  
      **status** - `[, float, list ]`
        status of the CE / Site  
      **lastCheckTime** - `[, datetime, list ]`
        measure calculated
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument    
    # pylint: disable-msg=W0613
    return self._query( 'select', 'PilotCache', locals() )
  def deletePilotCache( self, site = None, cE = None, pilotsPerJob = None, 
                        pilotJobEff = None, status = None, lastCheckTime = None,
                        meta = None ):
    '''
    Deletes from TransferCache all rows that match the parameters given.
    
    :Parameters:
      **site** - `[, string, list ]`
        name of the site 
      **cE** - `[, string, list ]`
        name of the CE of 'Multiple' if all site CEs are considered
      **pilotsPerJob** - `[, float, list ]`
        measure calculated
      **pilotJobEff** - `[, float, list ]`
        percentage  
      **status** - `[, float, list ]`
        status of the CE / Site  
      **lastCheckTime** - `[, datetime, list ]`
        measure calculated
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()    '''
    # Unused argument    
    # pylint: disable-msg=W0613
    return self._query( 'delete', 'PilotCache', locals() )
  def addOrModifyPilotCache( self, site = None, cE = None, pilotsPerJob = None, 
                             pilotJobEff = None, status = None, lastCheckTime = None,
                             meta = None ):
    '''
    Adds or updates-if-duplicated to PilotCache. Using `site` and `cE`
    to query the database, decides whether to insert or update the table.
    
    :Parameters:
      **site** - `string`
        name of the site 
      **cE** - `string`
        name of the CE of 'Multiple' if all site CEs are considered
      **pilotsPerJob** - `float`
        measure calculated
      **pilotJobEff** - `float`
        percentage  
      **status** - `string`
        status of the CE / Site  
      **lastCheckTime** - `datetime`
        measure calculated
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'PilotCache', locals() )
#  def addIfNotTherePilotCache( self, site = None, cE = None, pilotsPerJob = None, 
#                               pilotJobEff = None, status = None, lastCheckTime = None,
#                               meta = None ):
#    '''
#    Adds if not there to PilotCache. Using `site` and `cE` to query the 
#    database, decides whether to insert or not.
#    
#    :Parameters:
#      **site** - `string`
#        name of the site 
#      **cE** - `string`
#        name of the CE of 'Multiple' if all site CEs are considered
#      **pilotsPerJob** - `float`
#        measure calculated
#      **pilotJobEff** - `float`
#        percentage  
#      **status** - `string`
#        status of the CE / Site  
#      **lastCheckTime** - `datetime`
#        measure calculated
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    meta = { 'onlyUniqueKeys' : True }
#    return self._query( 'addIfNotThere', 'PilotCache', locals() )
    
  ##############################################################################
  # POLICY RESULT Methods

#  def insertPolicyResult( self, element, name, policyName, statusType,
#                          status, reason, lastCheckTime, meta = None ):
#    '''
#    Inserts on PolicyResult a new row with the arguments given.
#    
#    :Parameters:
#      **granularity** - `string`
#        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
#        | `Service` | `Resource` | `StorageElement`  
#      **name** - `string`
#        name of the element
#      **policyName** - `string`
#        name of the policy
#      **statusType** - `string`
#        it has to be a valid status type for the given granularity
#      **status** - `string`
#        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
#        `Probing` | `Banned`    
#      **reason** - `string`
#        decision that triggered the assigned status
#      **lastCheckTime** - `datetime`
#        time-stamp setting last time the policy result was checked
#      **meta** - `[,dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self._query( 'insert', 'PolicyResult', locals() ) 
#  def updatePolicyResult( self, element, name, policyName, statusType,
#                          status, reason, lastCheckTime, meta = None ):
#    '''
#    Updates PolicyResult with the parameters given. By default, `name`, 
#    `policyName` and `statusType` will be the parameters used to select the row.
#    
#    :Parameters:
#      **granularity** - `string`
#        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
#        | `Service` | `Resource` | `StorageElement`  
#      **name** - `string`
#        name of the element
#      **policyName** - `string`
#        name of the policy
#      **statusType** - `string`
#        it has to be a valid status type for the given granularity
#      **status** - `string`
#        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
#        `Probing` | `Banned`    
#      **reason** - `string`
#        decision that triggered the assigned status
#      **lastCheckTime** - `datetime`
#        time-stamp setting last time the policy result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self._query( 'update', 'PolicyResult', locals() )
  def selectPolicyResult( self, element = None, name = None, policyName = None, 
                          statusType = None, status = None, reason = None, 
                          lastCheckTime = None, meta = None ):
    '''
    Gets from PolicyResult all rows that match the parameters given.
    
    :Parameters:
      **granularity** - `[, string, list]`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`  
      **name** - `[, string, list]`
        name of the element
      **policyName** - `[, string, list]`
        name of the policy
      **statusType** - `[, string, list]`
        it has to be a valid status type for the given granularity
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`    
      **reason** - `[, string, list]`
        decision that triggered the assigned status
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the policy result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'select', 'PolicyResult', locals() )
  def deletePolicyResult( self, element = None, name = None, 
                          policyName = None, statusType = None, status = None, 
                          reason = None, lastCheckTime = None, meta = None ):
    '''
    Deletes from PolicyResult all rows that match the parameters given.
    
    :Parameters:
      **granularity** - `[, string, list]`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`  
      **name** - `[, string, list]`
        name of the element
      **policyName** - `[, string, list]`
        name of the policy
      **statusType** - `[, string, list]`
        it has to be a valid status type for the given granularity
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`    
      **reason** - `[, string, list]`
        decision that triggered the assigned status
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the policy result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'delete', 'PolicyResult', locals() )
  def addOrModifyPolicyResult( self, element = None, name = None, 
                               policyName = None, statusType = None,
                               status = None, reason = None, dateEffective = None, 
                               lastCheckTime = None, meta = None ):
    '''
    Adds or updates-if-duplicated to PolicyResult. Using `name`, `policyName` and 
    `statusType` to query the database, decides whether to insert or update the table.

    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`  
      **name** - `string`
        name of the element
      **policyName** - `string`
        name of the policy
      **statusType** - `string`
        it has to be a valid status type for the given element
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`    
      **reason** - `string`
        decision that triggered the assigned status
      **dateEffective** - `datetime`
        time-stamp from which the policy result is effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the policy result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'PolicyResult', locals() )      
#  def addIfNotTherePolicyResult( self, element = None, name = None, 
#                                 policyName = None, statusType = None,
#                                 status = None, reason = None, dateEffective = None, 
#                                 lastCheckTime = None, meta = None ):
#    '''
#    Adds if not there to PolicyResult. Using `name`, `policyName` and `statusType` 
#    to query the database, decides whether to insert or not.
#
#    :Parameters:
#      **element** - `string`
#        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
#        | `Service` | `Resource` | `StorageElement`  
#      **name** - `string`
#        name of the element
#      **policyName** - `string`
#        name of the policy
#      **statusType** - `string`
#        it has to be a valid status type for the given element
#      **status** - `string`
#        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
#        `Probing` | `Banned`    
#      **reason** - `string`
#        decision that triggered the assigned status
#      **dateEffective** - `datetime`
#        time-stamp from which the policy result is effective
#      **lastCheckTime** - `datetime`
#        time-stamp setting last time the policy result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument
#    # pylint: disable-msg=W0613
#    meta = { 'onlyUniqueKeys' : True }
#    return self._query( 'addIfNotThere', 'PolicyResult', locals() )     
    
  ##############################################################################
  # POLICY RESULT LOG Methods

#  def insertPolicyResultLog( self, element, name, policyName, statusType,
#                             status, reason, lastCheckTime, meta = None ):
#    '''
#    Inserts on PolicyResult a new row with the arguments given.
#    
#    :Parameters:
#      **element** - `string`
#        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
#        | `Service` | `Resource` | `StorageElement`  
#      **name** - `string`
#        name of the element
#      **policyName** - `string`
#        name of the policy
#      **statusType** - `string`
#        it has to be a valid status type for the given element
#      **status** - `string`
#        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
#        `Probing` | `Banned`    
#      **reason** - `string`
#        decision that triggered the assigned status
#      **lastCheckTime** - `datetime`
#        time-stamp setting last time the policy result was checked
#      **meta** - `[,dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self._query( 'insert', 'PolicyResultLog', locals() ) 
#  def updatePolicyResultLog( self, element, name, policyName, statusType,
#                             status, reason, lastCheckTime, meta = None ):
#    '''
#    Updates PolicyResultLog with the parameters given. By default, `name`, 
#    `policyName`, 'statusType` and `lastCheckTime` will be the parameters used to 
#    select the row.
#    
#    :Parameters:
#      **element** - `string`
#        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
#        | `Service` | `Resource` | `StorageElement`  
#      **name** - `string`
#        name of the element
#      **policyName** - `string`
#        name of the policy
#      **statusType** - `string`
#        it has to be a valid status type for the given element
#      **status** - `string`
#        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
#        `Probing` | `Banned`    
#      **reason** - `string`
#        decision that triggered the assigned status
#      **lastCheckTime** - `datetime`
#        time-stamp setting last time the policy result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self._query( 'update', 'PolicyResultLog', locals() )
  def selectPolicyResultLog( self, element = None, name = None, 
                              policyName = None, statusType = None, status = None, 
                              reason = None, lastCheckTime = None, meta = None ):
    '''
    Gets from PolicyResultLog all rows that match the parameters given.
    
    :Parameters:
      **element** - `[, string, list]`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`  
      **name** - `[, string, list]`
        name of the element
      **policyName** - `[, string, list]`
        name of the policy
      **statusType** - `[, string, list]`
        it has to be a valid status type for the given element
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`    
      **reason** - `[, string, list]`
        decision that triggered the assigned status
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the policy result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'select', 'PolicyResultLog', locals() )
  def deletePolicyResultLog( self, element = None, name = None, 
                             policyName = None, statusType = None, status = None, 
                             reason = None, lastCheckTime = None, meta = None ):
    '''
    Deletes from PolicyResult all rows that match the parameters given.
    
    :Parameters:
      **element** - `[, string, list]`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`  
      **name** - `[, string, list]`
        name of the element
      **policyName** - `[, string, list]`
        name of the policy
      **statusType** - `[, string, list]`
        it has to be a valid status type for the given element
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`    
      **reason** - `[, string, list]`
        decision that triggered the assigned status
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the policy result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'delete', 'PolicyResultLog', locals() )
  def addOrModifyPolicyResultLog( self, element = None, name = None, 
                                  policyName = None, statusType = None,
                                  status = None, reason = None, lastCheckTime = None, 
                                  meta = None ):
    '''
    Adds or updates-if-duplicated to PolicyResultLog. Using `name`, `policyName`, 
    'statusType` to query the database, decides whether to insert or update the table.
    
    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`  
      **name** - `string`
        name of the element
      **policyName** - `string`
        name of the policy
      **statusType** - `string`
        it has to be a valid status type for the given element
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`    
      **reason** - `string`
        decision that triggered the assigned status
      **lastCheckTime** - `datetime`
        time-stamp setting last time the policy result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'PolicyResultLog', locals() )         
#  def addIfNotTherePolicyResultLog( self, element = None, name = None, 
#                                    policyName = None, statusType = None,
#                                    status = None, reason = None, 
#                                    lastCheckTime = None, meta = None ):
#    '''
#    Adds if not there to PolicyResult. Using `name`, `policyName` and `statusType` 
#    to query the database, decides whether to insert or not.
#    
#    :Parameters:
#      **element** - `string`
#        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
#        | `Service` | `Resource` | `StorageElement`  
#      **name** - `string`
#        name of the element
#      **policyName** - `string`
#        name of the policy
#      **statusType** - `string`
#        it has to be a valid status type for the given element
#      **status** - `string`
#        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
#        `Probing` | `Banned`    
#      **reason** - `string`
#        decision that triggered the assigned status
#      **lastCheckTime** - `datetime`
#        time-stamp setting last time the policy result was checked
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument
#    # pylint: disable-msg=W0613
#    meta = { 'onlyUniqueKeys' : True }
#    return self._query( 'addIfNotThere', 'PolicyResultLog', locals() )         
    
  ##############################################################################
  # SpaceTokenOccupancy CACHE Methods

#  def insertSpaceTokenOccupancyCache( self, endpoint, token, total, guaranteed, free, 
#                                      lastCheckTime, meta = None ):
#    '''
#    Inserts on SpaceTokenOccupancyCache a new row with the arguments given.
#    
#    :Parameters:
#      **endpoint** - `string`
#        srm endpoint  
#      **token** - `string`
#        name of the token  
#      **total** - `integer`
#        total terabytes
#      **guaranteed** - `integer`
#        guaranteed terabytes
#      **free** - `integer`
#        free terabytes
#      **lastCheckTime** - `datetime`
#        time-stamp from which the result is effective
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self._query( 'insert', 'SpaceTokenOccupancyCache', locals() )
#  def updateSpaceTokenOccupancyCache( self, endpoint, token, total, guaranteed, free, 
#                                      lastCheckTime, meta = None ):
#    '''
#    Updates SpaceTokenOccupancyCache with the parameters given. By default, 
#    `site` and `token` will be the parameters used to select the row.
#    
#    :Parameters:
#      **endpoint** - `string`
#        srm endpoint
#      **token** - `string`
#        name of the token  
#      **total** - `integer`
#        total terabytes
#      **guaranteed** - `integer`
#        guaranteed terabytes
#      **free** - `integer`
#        free terabytes
#      **lastCheckTime** - `datetime`
#        time-stamp from which the result is effective
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self._query( 'update', 'SpaceTokenOccupancyCache', locals() )
  def selectSpaceTokenOccupancyCache( self, endpoint = None, token = None, 
                                      total = None, guaranteed = None, free = None, 
                                      lastCheckTime = None, meta = None ):
    '''
    Gets from SpaceTokenOccupancyCache all rows that match the parameters given.
    
    :Parameters:
      **endpoint** - `[, string, list]`
        srm endpoint
      **token** - `[, string, list]`
        name of the token  
      **total** - `[, integer, list]`
        total terabytes
      **guaranteed** - `[, integer, list]`
        guaranteed terabytes
      **free** - `[, integer, list]`
        free terabytes
      **lastCheckTime** - `[, datetime, list]`
        time-stamp from which the result is effective
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'select', 'SpaceTokenOccupancyCache', locals() )
  def deleteSpaceTokenOccupancyCache( self, endpoint = None, token = None, 
                                      total = None, guaranteed = None, free = None, 
                                      lastCheckTime = None, meta = None ):
    '''
    Deletes from SpaceTokenOccupancyCache all rows that match the parameters given.
    
    :Parameters:
      **endpoint** - `[, string, list]`
        srm endpoint
      **token** - `[, string, list]`
        name of the token
      **total** - `[, integer, list]`
        total terabytes
      **guaranteed** - `[, integer, list]`
        guaranteed terabytes
      **free** - `[, integer, list]`
        free terabytes
      **lastCheckTime** - `[, datetime, list]`
        time-stamp from which the result is effective
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'delete', 'SpaceTokenOccupancyCache', locals() )  
  def addOrModifySpaceTokenOccupancyCache( self, endpoint = None, token = None, 
                                           total = None, guaranteed = None, 
                                           free = None, lastCheckTime = None, 
                                           meta = None ):
    '''
    Adds or updates-if-duplicated to SpaceTokenOccupancyCache. Using `site` and `token` 
    to query the database, decides whether to insert or update the table.
    
    :Parameters:
      **endpoint** - `[, string, list]`
        srm endpoint
      **token** - `string`
        name of the token    
      **total** - `integer`
        total terabytes
      **guaranteed** - `integer`
        guaranteed terabytes
      **free** - `integer`
        free terabytes
      **lastCheckTime** - `datetime`
        time-stamp from which the result is effective
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'SpaceTokenOccupancyCache', locals() )        
#  def addIfNotThereSpaceTokenOccupancyCache( self, endpoint = None, token = None, 
#                                             total = None, guaranteed = None, 
#                                             free = None, lastCheckTime = None, 
#                                             meta = None ):
#    '''
#    Adds if not there to PolicyResult. Using `site` and `token` to query the 
#    database, decides whether to insert or not.
#    
#    :Parameters:
#      **endpoint** - `[, string, list]`
#        srm endpoint
#      **token** - `string`
#        name of the token    
#      **total** - `integer`
#        total terabytes
#      **guaranteed** - `integer`
#        guaranteed terabytes
#      **free** - `integer`
#        free terabytes
#      **lastCheckTime** - `datetime`
#        time-stamp from which the result is effective
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    meta = { 'onlyUniqueKeys' : True }
#    return self._query( 'addIfNotThere', 'SpaceTokenOccupancyCache', locals() ) 
        
  ##############################################################################
  # USER REGISTRY CACHE Methods

#  def insertUserRegistryCache( self, login, name, email, lastCheckTime, meta = None ):
#    '''
#    Inserts on UserRegistryCache a new row with the arguments given.
#    
#    :Parameters:
#      **login** - `string`
#        user's login ID  
#      **name** - `string`
#        user's name
#      **email** - `string`
#        user's email
#      **lastCheckTime** - `datetime`
#        time-stamp from which the result is effective 
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    # Unused argument    
#    # pylint: disable-msg=W0613
#    return self._query( 'insert', 'UserRegistryCache', locals() )
#  def updateUserRegistryCache( self, login, name, email, lastCheckTime, meta = None ):
#    '''
#    Updates UserRegistryCache with the parameters given. By default, `login` 
#    will be the parameter used to select the row.
#    
#    :Parameters:
#      **login** - `string`
#        user's login ID  
#      **name** - `string`
#        user's name
#      **email** - `string`
#        user's email
#      **lastCheckTime** - `datetime`
#        time-stamp from which the result is effective  
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self._query( 'update', 'UserRegistryCache', locals() )
  def selectUserRegistryCache( self, login = None, name = None, email = None, 
                               lastCheckTime = None, meta = None ):
    '''
    Gets from UserRegistryCache all rows that match the parameters given.
    
    :Parameters:
      **login** - `[, string, list]`
        user's login ID  
      **name** - `[, string, list]`
        user's name
      **email** - `[, string, list]`
        user's email
      **lastCheckTime** - `[, datetime, list]`
        time-stamp from which the result is effective  
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'select', 'UserRegistryCache', locals() )
  def deleteUserRegistryCache( self, login = None, name = None, email = None, 
                               lastCheckTime = None, meta = None ):                                            
    '''
    Deletes from UserRegistryCache all rows that match the parameters given.
    
    :Parameters:
      **login** - `[, string, list]`
        user's login ID  
      **name** - `[, string, list]`
        user's name
      **email** - `[, string, list]`
        user's email
      **lastCheckTime** - `[, datetime, list]`
        time-stamp from which the result is effective  
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'delete', 'UserRegistryCache', locals() )
  def addOrModifyUserRegistryCache( self, login = None, name = None, 
                                    email = None, lastCheckTime = None, meta = None ):
    '''
    Adds or updates-if-duplicated to UserRegistryCache. Using `login` to query 
    the database, decides whether to insert or update the table.
    
    :Parameters:
      **login** - `string`
        user's login ID  
      **name** - `string`
        user's name
      **email** - `string`
        user's email
      **lastCheckTime** - `datetime`
        time-stamp from which the result is effective          
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'UserRegistryCache', locals() )   
#  def addIfNotThereUserRegistryCache( self, login = None, name = None, 
#                                      email = None, lastCheckTime = None, meta = None ):
#    '''
#    Adds if not there to UserRegistryCache. Using `login` to query the 
#    database, decides whether to insert or not.
#    
#    :Parameters:
#      **login** - `string`
#        user's login ID  
#      **name** - `string`
#        user's name
#      **email** - `string`
#        user's email
#      **lastCheckTime** - `datetime`
#        time-stamp from which the result is effective  
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    meta = { 'onlyUniqueKeys' : True }
#    return self._query( 'addIfNotThere', 'UserRegistryCache', locals() )   

  ##############################################################################
  # VOBOX CACHE Methods

#  def insertVOBOXCache( self, site, system, serviceUp, machineUp, lastCheckTime, 
#                        meta = None ):
#    '''
#    Inserts on VOBOXCache a new row with the arguments given.
#    
#    :Parameters:
#      **site** - `string`
#        name of the site hosting the VOBOX  
#      **system** - `string`
#        DIRAC system ( e.g. ConfigurationService )
#      **serviceUp** - `integer`
#        seconds the system has been up
#      **machineUp** - `integer`
#        seconds the machine has been up
#      **lastCheckTime** - `datetime`
#        time-stamp from which the result is effective
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self._query( 'insert', 'VOBOXCache', locals() )
#  def updateVOBOXCache( self, site, system, serviceUp, machineUp, lastCheckTime, 
#                        meta = None ):
#    '''
#    Updates VOBOXCache with the parameters given. By default, `site` and 
#    `system` will be the parameters used to select the row.
#    
#    :Parameters:
#      **site** - `string`
#        name of the site hosting the VOBOX  
#      **system** - `string`
#        DIRAC system ( e.g. ConfigurationService )
#      **serviceUp** - `integer`
#        seconds the system has been up
#      **machineUp** - `integer`
#        seconds the machine has been up
#      **lastCheckTime** - `datetime`
#        time-stamp from which the result is effective
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self._query( 'update', 'VOBOXCache', locals() )
  def selectVOBOXCache( self, site = None, system = None, serviceUp = None, 
                        machineUp = None, lastCheckTime = None, meta = None ):
    '''
    Gets from VOBOXCache all rows that match the parameters given.
    
    :Parameters:
      **site** - `[, string, list ]`
        name of the site hosting the VOBOX  
      **system** - `[, string, list ]`
        DIRAC system ( e.g. ConfigurationService )
      **serviceUp** - `[, integer, list]`
        seconds the system has been up
      **machineUp** - `[, integer, list]`
        seconds the machine has been up
      **lastCheckTime** - `[, datetime, list]`
        time-stamp from which the result is effective
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'select', 'VOBOXCache', locals() )
  def deleteVOBOXCache( self, site = None, system = None, serviceUp = None, 
                        machineUp = None, lastCheckTime = None, meta = None ):
    '''
    Deletes from VOBOXCache all rows that match the parameters given.
    
    :Parameters:
      **site** - `[, string, list ]`
        name of the site hosting the VOBOX  
      **system** - `[, string, list ]`
        DIRAC system ( e.g. ConfigurationService )
      **serviceUp** - `[, integer, list]`
        seconds the system has been up
      **machineUp** - `[, integer, list]`
        seconds the machine has been up
      **lastCheckTime** - `[, datetime, list]`
        time-stamp from which the result is effective
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self._query( 'delete', 'VOBOXCache', locals() )  
  def addOrModifyVOBOXCache( self, site = None, system = None, serviceUp = None, 
                             machineUp = None, lastCheckTime = None, meta = None ):
    '''
    Adds or updates-if-duplicated to VOBOXCache. Using `site` and `system` to query 
    the database, decides whether to insert or update the table.
    
    :Parameters:
      **site** - `string`
        name of the site hosting the VOBOX  
      **system** - `string`
        DIRAC system ( e.g. ConfigurationService )
      **serviceUp** - `integer`
        seconds the system has been up
      **machineUp** - `integer`
        seconds the machine has been up
      **lastCheckTime** - `datetime`
        time-stamp from which the result is effective
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self._query( 'addOrModify', 'VOBOXCache', locals() )   
  
#  def addIfNotThereVOBOXCache( self, site = None, system = None, serviceUp = None, 
#                               machineUp = None, lastCheckTime = None, meta = None ):
#    '''
#    Adds if not there to VOBOXCache. Using `site` and `system` to query the 
#    database, decides whether to insert or not.
#    
#    :Parameters:
#      **site** - `string`
#        name of the site hosting the VOBOX  
#      **system** - `string`
#        DIRAC system ( e.g. ConfigurationService )
#      **serviceUp** - `integer`
#        seconds the system has been up
#      **machineUp** - `integer`
#        seconds the machine has been up
#      **lastCheckTime** - `datetime`
#        time-stamp from which the result is effective
#      **meta** - `[, dict]`
#        meta-data for the MySQL query. It will be filled automatically with the\
#       `table` key and the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    meta = { 'onlyUniqueKeys' : True }
#    return self._query( 'addIfNotThere', 'VOBOXCache', locals() )   

  ################################################################################
  # Protected methods

  def _query( self, queryType, tableName, parameters ):
    '''
    It is a simple helper, this way inheriting classes can use it.
    '''
    return self.__query( queryType, tableName, parameters )
  
  ##############################################################################
  # Private methods

  def __query( self, queryType, tableName, parameters ):
    '''
      This method is a rather important one. It will format the input for the DB
      queries, instead of doing it on a decorator. Two dictionaries must be passed
      to the DB. First one contains 'columnName' : value pairs, being the key
      lower camel case. The second one must have, at lease, a key named 'table'
      with the right table name. 
    '''
    # Functions we can call, just a light safety measure.
    _gateFunctions = [ 'insert', 'update', 'select', 'delete', 'addOrModify', 'addIfNotThere'  ] 
    if not queryType in _gateFunctions:
      return S_ERROR( '"%s" is not a proper gate call' % queryType )
    
    gateFunction = getattr( self.gate, queryType )
    
    # If meta is None, we set it to {}
    meta   = ( True and parameters.pop( 'meta' ) ) or {}
#    params = parameters
    # Remove self, added by locals()
    del parameters[ 'self' ]     
        
    meta[ 'table' ] = tableName
    
    gLogger.debug( 'Calling %s, with \n params %s \n meta %s' % ( queryType, parameters, meta ) )  
    return gateFunction( parameters, meta )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF