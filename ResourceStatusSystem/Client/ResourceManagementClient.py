# $HeadURL:  $
''' ResourceManagementClient

  Client to interact with the ResourceManagementDB.

'''

from datetime import datetime

from DIRAC                                              import gLogger, S_ERROR 
from DIRAC.Core.DISET.RPCClient                         import RPCClient
from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB

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
    
    self.gate = ResourceManagementDB()
    
#    if not serviceIn:
#      self.gate = RPCClient( "ResourceStatus/ResourceManagement" )    
#    else:
#      self.gate = serviceIn    


  ##############################################################################
  # ACCOUNTING CACHE METHODS

  def insertAccountingCache( self, name, plotType, plotName, result, 
                             dateEffective, lastCheckTime, meta = None ):
    '''
    Inserts on AccountingCache a new row with the arguments given.
    
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
    return self.__query( 'insert', 'AccountingCache', locals() )
  def updateAccountingCache( self, name, plotType, plotName, result, 
                             dateEffective, lastCheckTime, meta = None ):
    '''
    Updates AccountingCache with the parameters given. By default, `name`, 
    `plotType` and `plotName` will be the parameters used to select the row.
    
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
    return self.__query( 'update', 'AccountingCache', locals() )
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
    return self.__query( 'select', 'AccountingCache', locals() )
  def deleteAccountingCache( self, name = None, plotType = None, 
                             plotName = None, result = None, 
                             dateEffective = None, lastCheckTime = None, 
                             meta = None ):
    '''
    Deletes from PolicyResult all rows that match the parameters given.
    
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
    return self.__query( 'delete', 'AccountingCache', locals() )
  #FIXME: should they be None or not ??
  def addOrModifyAccountingCache( self, name = None, plotType = None, 
                                  plotName = None, result = None, 
                                  dateEffective = None, lastCheckTime = None,
                                  meta = None ):
    '''
    Adds or updates-if-duplicated from AccountingCache
#    Using `name`, `plotType` and `plotName` to query the database, 
#    decides whether to insert or update the table.
    
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
    return self.__query( 'addOrModify', 'AccountingCache', locals() )    
  def addIfNotThereAccountingCache( self, name = None, plotType = None, 
                                    plotName = None, result = None, 
                                    dateEffective = None, lastCheckTime = None,
                                    meta = None ):
    '''
    Adds or updates-if-duplicated from AccountingCache
#    Using `name`, `plotType` and `plotName` to query the database, 
#    decides whether to insert or update the table.
    
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
    return self.__query( 'addIfNotThere', 'AccountingCache', locals() )    
    
  ##############################################################################
  # CLIENT CACHE Methods

  def insertClientCache( self, name, commandName, opt_ID, value, result,
                         dateEffective, lastCheckTime, meta = None ):
    '''
    Inserts on ClientCache a new row with the arguments given.
    
    :Parameters:
      **name** - `string`
        name of an individual of the grid topology  
      **commandName** - `string`
        name of the command executed
      **opt_ID** - `string`
        optional ID (e.g. used for downtimes)
      **value** - `string`
        it is the type of result ( e.g. `Link`, `PE_S`... )
      **result** - `string`
        output of the command ( of value type )    
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
    return self.__query( 'insert', 'ClientCache', locals() )
  def updateClientCache( self, name, commandName, opt_ID, value, result,
                         dateEffective, lastCheckTime, meta = None ):
    '''
    Updates ClientCache with the parameters given. By default, `name`, 
    `commandName` and `value` will be the parameters used to select the row.
    
    :Parameters:
      **name** - `string`
        name of an individual of the grid topology  
      **commandName** - `string`
        name of the command executed
      **opt_ID** - `string`
        optional ID (e.g. used for downtimes)
      **value** - `string`
        it is the type of result ( e.g. `Link`, `PE_S`... )
      **result** - `string`
        output of the command ( of value type )    
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
    return self.__query( 'update', 'ClientCache', locals() )
  def selectClientCache( self, name = None, commandName = None, opt_ID = None, 
                         value = None, result = None, dateEffective = None, 
                         lastCheckTime = None, meta = None ):
    '''
    Gets from ClientCache all rows that match the parameters given.
    
    :Parameters:
      **name** - `[, string, list]`
        name of an individual of the grid topology  
      **commandName** - `[, string, list]`
        name of the command executed
      **opt_ID** - `[, string, list]`
        optional ID (e.g. used for downtimes)
      **value** - `[, string, list]`
        it is the type of result ( e.g. `Link`, `PE_S`... )
      **result** - `[, string, list]`
        output of the command ( of value type )    
      **dateEffective** - `[, datetime, list]`
        time-stamp from which this result is effective
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time this result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self.__query( 'select', 'ClientCache', locals() )
  def deleteClientCache( self, name = None, commandName = None, opt_ID = None, 
                         value = None, result = None, dateEffective = None, 
                         lastCheckTime = None, meta = None ):
    '''
    Deletes from PolicyResult all rows that match the parameters given.
    
    :Parameters:
      **name** - `[, string, list]`
        name of an individual of the grid topology  
      **commandName** - `[, string, list]`
        name of the command executed
      **opt_ID** - `[, string, list]`
        optional ID (e.g. used for downtimes)
      **value** - `[, string, list]`
        it is the type of result ( e.g. `Link`, `PE_S`... )
      **result** - `[, string, list]`
        output of the command ( of value type )    
      **dateEffective** - `[, datetime, list]`
        time-stamp from which this result is effective
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time this result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self.__query( 'delete', 'ClientCache', locals() )
  def addOrModifyClientCache( self, name = None, commandName = None, opt_ID = None, 
                              value = None, result = None, dateEffective = None, 
                              lastCheckTime = None, meta = None ):
    '''
    Using `name`, `commandName` and `value` to query the database, 
    decides whether to insert or update the table.
    
    :Parameters:
      **name** - `string`
        name of an individual of the grid topology  
      **commandName** - `string`
        name of the command executed
      **opt_ID** - `string`
        optional ID (e.g. used for downtimes)
      **value** - `string`
        it is the type of result ( e.g. `Link`, `PE_S`... )
      **result** - `string`
        output of the command ( of value type )    
      **dateEffective** - `datetime`
        time-stamp from which the result is effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__addOrModifyElement( 'ClientCache', locals() )
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self.__query( 'addOrModify', 'ClientCache', locals() )    
  def addIfNotThereClientCache( self, name = None, commandName = None, opt_ID = None, 
                                value = None, result = None, dateEffective = None, 
                                lastCheckTime = None, meta = None ):
    '''
    Using `name`, `commandName` and `value` to query the database, 
    decides whether to insert or update the table.
    
    :Parameters:
      **name** - `string`
        name of an individual of the grid topology  
      **commandName** - `string`
        name of the command executed
      **opt_ID** - `string`
        optional ID (e.g. used for downtimes)
      **value** - `string`
        it is the type of result ( e.g. `Link`, `PE_S`... )
      **result** - `string`
        output of the command ( of value type )    
      **dateEffective** - `datetime`
        time-stamp from which the result is effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__addOrModifyElement( 'ClientCache', locals() )
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self.__query( 'addIfNotThere', 'ClientCache', locals() )       
    
  ##############################################################################
  # POLICY RESULT Methods

  def insertPolicyResult( self, granularity, name, policyName, statusType,
                          status, reason, dateEffective, lastCheckTime,
                          meta = None ):
    '''
    Inserts on PolicyResult a new row with the arguments given.
    
    :Parameters:
      **granularity** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`  
      **name** - `string`
        name of the element
      **policyName** - `string`
        name of the policy
      **statusType** - `string`
        it has to be a valid status type for the given granularity
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`    
      **reason** - `string`
        decision that triggered the assigned status
      **dateEffective** - `datetime`
        time-stamp from which the policy result is effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the policy result was checked
      **meta** - `[,dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613
    return self.__query( 'insert', 'PolicyResult', locals() ) 
  def updatePolicyResult( self, granularity, name, policyName, statusType,
                          status, reason, dateEffective, lastCheckTime, 
                          meta = None ):
    '''
    Updates PolicyResult with the parameters given. By default, `name`, 
    `policyName` and `statusType` will be the parameters used to select the row.
    
    :Parameters:
      **granularity** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`  
      **name** - `string`
        name of the element
      **policyName** - `string`
        name of the policy
      **statusType** - `string`
        it has to be a valid status type for the given granularity
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
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
    return self.__query( 'update', 'PolicyResult', locals() )
  def selectPolicyResult( self, granularity = None, name = None, policyName = None, 
                          statusType = None, status = None, reason = None, 
                          dateEffective = None, lastCheckTime = None, meta = None ):
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
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`    
      **reason** - `[, string, list]`
        decision that triggered the assigned status
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the policy result is effective
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the policy result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613
    return self.__query( 'select', 'PolicyResult', locals() )
  def deletePolicyResult( self, granularity = None, name = None, 
                          policyName = None, statusType = None, status = None, 
                          reason = None, dateEffective = None, 
                          lastCheckTime = None, meta = None ):
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
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`    
      **reason** - `[, string, list]`
        decision that triggered the assigned status
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the policy result is effective
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the policy result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613
    return self.__query( 'delete', 'PolicyResult', locals() )
  def addOrModifyPolicyResult( self, granularity = None, name = None, 
                               policyName = None, statusType = None,
                               status = None, reason = None, dateEffective = None, 
                               lastCheckTime = None, meta = None ):
    '''
    Using `name`, `policyName` and `statusType` to query the database, 
    decides whether to insert or update the table.

    :Parameters:
      **granularity** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`  
      **name** - `string`
        name of the element
      **policyName** - `string`
        name of the policy
      **statusType** - `string`
        it has to be a valid status type for the given granularity
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
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
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__addOrModifyElement( 'PolicyResult', locals() )
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self.__query( 'addOrModify', 'PolicyResult', locals() )      
  def addIfNotTherePolicyResult( self, granularity = None, name = None, 
                                 policyName = None, statusType = None,
                                 status = None, reason = None, dateEffective = None, 
                                 lastCheckTime = None, meta = None ):
    '''
    Using `name`, `policyName` and `statusType` to query the database, 
    decides whether to insert or update the table.

    :Parameters:
      **granularity** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`  
      **name** - `string`
        name of the element
      **policyName** - `string`
        name of the policy
      **statusType** - `string`
        it has to be a valid status type for the given granularity
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
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
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__addOrModifyElement( 'PolicyResult', locals() )
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self.__query( 'addIfNotThere', 'PolicyResult', locals() )     
    
  ##############################################################################
  # POLICY RESULT LOG Methods

  def insertPolicyResultLog( self, granularity, name, policyName, statusType,
                             status, reason, lastCheckTime, meta = None ):
    '''
    Inserts on PolicyResult a new row with the arguments given.
    
    :Parameters:
      **granularity** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`  
      **name** - `string`
        name of the element
      **policyName** - `string`
        name of the policy
      **statusType** - `string`
        it has to be a valid status type for the given granularity
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`    
      **reason** - `string`
        decision that triggered the assigned status
      **lastCheckTime** - `datetime`
        time-stamp setting last time the policy result was checked
      **meta** - `[,dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument
    # pylint: disable-msg=W0613
    return self.__query( 'insert', 'PolicyResultLog', locals() ) 
  def updatePolicyResultLog( self, granularity, name, policyName, statusType,
                             status, reason, lastCheckTime, meta = None ):
    '''
    Updates PolicyResultLog with the parameters given. By default, `name`, 
    `policyName`, 'statusType` and `lastCheckTime` will be the parameters used to 
    select the row.
    
    :Parameters:
      **granularity** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`  
      **name** - `string`
        name of the element
      **policyName** - `string`
        name of the policy
      **statusType** - `string`
        it has to be a valid status type for the given granularity
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
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
    return self.__query( 'update', 'PolicyResultLog', locals() )
  def selectPolicyResultLog( self, granularity = None, name = None, 
                              policyName = None, statusType = None, status = None, 
                              reason = None, lastCheckTime = None, meta = None ):
    '''
    Gets from PolicyResultLog all rows that match the parameters given.
    
    :Parameters:
      **granularity** - `[, string, list]`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`  
      **name** - `[, string, list]`
        name of the element
      **policyName** - `[, string, list]`
        name of the policy
      **statusType** - `[, string, list]`
        it has to be a valid status type for the given granularity
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
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
    return self.__query( 'select', 'PolicyResultLog', locals() )
  def deletePolicyResultLog( self, granularity = None, name = None, 
                             policyName = None, statusType = None, status = None, 
                             reason = None, lastCheckTime = None, meta = None ):
    '''
    Deletes from PolicyResult all rows that match the parameters given.
    
    :Parameters:
      **granularity** - `[, string, list]`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`  
      **name** - `[, string, list]`
        name of the element
      **policyName** - `[, string, list]`
        name of the policy
      **statusType** - `[, string, list]`
        it has to be a valid status type for the given granularity
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
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
    return self.__query( 'delete', 'PolicyResultLog', locals() )
  def addOrModifyPolicyResultLog( self, granularity = None, name = None, 
                                  policyName = None, statusType = None,
                                  status = None, reason = None, lastCheckTime = None, 
                                  meta = None ):
    '''
    AddsOrModifies on PolicyResult a new row with the arguments given.
    
    :Parameters:
      **granularity** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`  
      **name** - `string`
        name of the element
      **policyName** - `string`
        name of the policy
      **statusType** - `string`
        it has to be a valid status type for the given granularity
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`    
      **reason** - `string`
        decision that triggered the assigned status
      **lastCheckTime** - `datetime`
        time-stamp setting last time the policy result was checked
      **meta** - `[,dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'insert', 'PolicyResultLog', locals() )  
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self.__query( 'addOrModify', 'PolicyResultLog', locals() )         
  def addIfNotTherePolicyResultLog( self, granularity = None, name = None, 
                                    policyName = None, statusType = None,
                                    status = None, reason = None, 
                                    lastCheckTime = None, meta = None ):
    '''
    AddsIfNotThere on PolicyResult a new row with the arguments given.
    
    :Parameters:
      **granularity** - `string`
        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
        | `Service` | `Resource` | `StorageElement`  
      **name** - `string`
        name of the element
      **policyName** - `string`
        name of the policy
      **statusType** - `string`
        it has to be a valid status type for the given granularity
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
        `Probing` | `Banned`    
      **reason** - `string`
        decision that triggered the assigned status
      **lastCheckTime** - `datetime`
        time-stamp setting last time the policy result was checked
      **meta** - `[,dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__query( 'insert', 'PolicyResultLog', locals() )  
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self.__query( 'addIfNotThere', 'PolicyResultLog', locals() )         
    
  ##############################################################################
  # SpaceTokenOccupancy CACHE Methods

  def insertSpaceTokenOccupancyCache( self, site, token, total, guaranteed,
                                      free, lastCheckTime, meta = None ):
    '''
    Inserts on SpaceTokenOccupancyCache a new row with the arguments given.
    
    :Parameters:
      **site** - `string`
        name of the space token site  
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
    return self.__query( 'insert', 'SpaceTokenOccupancyCache', locals() )
  def updateSpaceTokenOccupancyCache( self, site, token, total, guaranteed,
                                      free, lastCheckTime, meta = None ):
    '''
    Updates SpaceTokenOccupancyCache with the parameters given. By default, 
    `site` and `token` will be the parameters used to select the row.
    
    :Parameters:
      **site** - `string`
        name of the space token site  
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
    return self.__query( 'update', 'SpaceTokenOccupancyCache', locals() )
  def selectSpaceTokenOccupancyCache( self, site = None, token = None, total = None, 
                                      guaranteed = None, free = None, 
                                      lastCheckTime = None, meta = None ):
    '''
    Gets from SpaceTokenOccupancyCache all rows that match the parameters given.
    
    :Parameters:
      **site** - `[, string, list]`
        name of the space token site  
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
    return self.__query( 'select', 'SpaceTokenOccupancyCache', locals() )
  def deleteSpaceTokenOccupancyCache( self, site = None, token = None, total = None, 
                                      guaranteed = None, free = None, 
                                      lastCheckTime = None, meta = None ):
    '''
    Deletes from SpaceTokenOccupancyCache all rows that match the parameters given.
    
    :Parameters:
      **site** - `[, string, list]`
        name of the space token site  
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
    return self.__query( 'delete', 'SpaceTokenOccupancyCache', locals() )  
  def addOrModifySpaceTokenOccupancyCache( self, site = None, token = None, 
                                           total = None, guaranteed = None, 
                                           free = None, lastCheckTime = None, 
                                           meta = None ):
    '''
    Using `site` and `token` to query the database, decides whether to insert or 
    update the table.
    
    :Parameters:
      **site** - `string`
        name of the space token site  
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
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__addOrModifyElement( 'SpaceTokenOccupancyCache', locals() ) 
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self.__query( 'addOrModify', 'SpaceTokenOccupancyCache', locals() )        
  def addIfNotThereSpaceTokenOccupancyCache( self, site = None, token = None, 
                                             total = None, guaranteed = None, 
                                             free = None, lastCheckTime = None, 
                                             meta = None ):
    '''
    Using `site` and `token` to query the database, decides whether to insert or 
    update the table.
    
    :Parameters:
      **site** - `string`
        name of the space token site  
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
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__addOrModifyElement( 'SpaceTokenOccupancyCache', locals() ) 
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self.__query( 'addIfNotThere', 'SpaceTokenOccupancyCache', locals() ) 
        
  ##############################################################################
  # USER REGISTRY CACHE Methods

  def insertUserRegistryCache( self, login, name, email, meta = None ):
    '''
    Inserts on UserRegistryCache a new row with the arguments given.
    
    :Parameters:
      **login** - `string`
        user's login ID  
      **name** - `string`
        user's name
      **email** - `string`
        user's email
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    # Unused argument    
    # pylint: disable-msg=W0613
    return self.__query( 'insert', 'UserRegistryCache', locals() )
  def updateUserRegistryCache( self, login, name, email, meta = None ):
    '''
    Updates UserRegistryCache with the parameters given. By default, `login` 
    will be the parameter used to select the row.
    
    :Parameters:
      **login** - `string`
        user's login ID  
      **name** - `string`
        user's name
      **email** - `string`
        user's email
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self.__query( 'update', 'UserRegistryCache', locals() )
  def selectUserRegistryCache( self, login = None, name = None, email = None, 
                            meta = None ):
    '''
    Gets from UserRegistryCache all rows that match the parameters given.
    
    :Parameters:
      **login** - `[, string, list]`
        user's login ID  
      **name** - `[, string, list]`
        user's name
      **email** - `[, string, list]`
        user's email
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self.__query( 'select', 'UserRegistryCache', locals() )
  def deleteUserRegistryCache( self, login = None, name = None, email = None, 
                               meta = None ):                                            
    '''
    Deletes from UserRegistryCache all rows that match the parameters given.
    
    :Parameters:
      **login** - `[, string, list]`
        user's login ID  
      **name** - `[, string, list]`
        user's name
      **email** - `[, string, list]`
        user's email
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    # Unused argument
    # pylint: disable-msg=W0613
    return self.__query( 'delete', 'UserRegistryCache', locals() )
  def addOrModifyUserRegistryCache( self, login = None, name = None, 
                                    email = None, meta = None ):
    '''
    Using `login` to query the database, decides whether to insert or update 
    the table.
    
    :Parameters:
      **login** - `string`
        user's login ID  
      **name** - `string`
        user's name
      **email** - `string`
        user's email
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__addOrModifyElement( 'UserRegistryCache', locals() )   
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self.__query( 'addOrModify', 'UserRegistryCache', locals() )   
  def addIfNotThereUserRegistryCache( self, login = None, name = None, 
                                       email = None, meta = None ):
    '''
    Using `login` to query the database, decides whether to insert or update 
    the table.
    
    :Parameters:
      **login** - `string`
        user's login ID  
      **name** - `string`
        user's name
      **email** - `string`
        user's email
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__addOrModifyElement( 'UserRegistryCache', locals() )   
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self.__query( 'addIfNotThere', 'UserRegistryCache', locals() )   

  ##############################################################################
  # VOBOX CACHE Methods

  def insertVOBOXCache( self, site, system, serviceUp, machineUp, 
                             lastCheckTime, meta = None ):
    '''
    Inserts on VOBOXCache a new row with the arguments given.
    
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
    return self.__query( 'insert', 'VOBOXCache', locals() )
  def updateVOBOXCache( self, site, system, serviceUp, machineUp, 
                        lastCheckTime, meta = None ):
    '''
    Updates VOBOXCache with the parameters given. By default, `site` and 
    `system` will be the parameters used to select the row.
    
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
    return self.__query( 'update', 'VOBOXCache', locals() )
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
    return self.__query( 'select', 'VOBOXCache', locals() )
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
    return self.__query( 'delete', 'VOBOXCache', locals() )  
  def addOrModifyVOBOXCache( self, site = None, system = None, serviceUp = None, 
                             machineUp = None, lastCheckTime = None, meta = None ):
    '''
    Using `site` and `system` to query the database, 
    decides whether to insert or update the table.
    
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
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__addOrModifyElement( 'VOBOXCache', locals() ) 
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self.__query( 'addOrModify', 'VOBOXCache', locals() )   
  
  def addIfNotThereVOBOXCache( self, site = None, system = None, serviceUp = None, 
                               machineUp = None, lastCheckTime = None, meta = None ):
    '''
    Using `site` and `system` to query the database, 
    decides whether to insert or update the table.
    
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
#    # Unused argument
#    # pylint: disable-msg=W0613
#    return self.__addOrModifyElement( 'VOBOXCache', locals() ) 
    # Unused argument
    # pylint: disable-msg=W0613
    meta = { 'onlyUniqueKeys' : True }
    return self.__query( 'addIfNotThere', 'VOBOXCache', locals() )   


# THIS METHOD DOES NOT WORK AS EXPECTED 
# __addOrModifyElement overwrittes the field lastCheckTime.
# Anyway, this table is a pure insert / get / delete table. No updates foreseen.
 
#  def addOrModifyPolicyResultLog( self, granularity, name, policyName, statusType,
#                                  status, reason, lastCheckTime ):
#    '''
#    Using `name`, `policyName` and `statusType` and `lastCheckTime` to query the 
#    database, decides whether to insert or update the table.
#    
#    BE CAREFUL: lastCheckTime is on the UNIQUE_TOGETHER tuple. On the other hand,
#    lastCheckTime is overwritten 
#
#    :Parameters:
#      **granularity** - `string`
#        it has to be a valid element ( ValidRes ), any of the defaults: `Site` \
#        | `Service` | `Resource` | `StorageElement`  
#      **name** - `string`
#        name of the element
#      **policyName** - `string`
#        name of the policy
#      **statusType** - `string`
#        it has to be a valid status type for the given granularity
#      **status** - `string`
#        it has to be a valid status, any of the defaults: `Active` | `Bad` | \
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
#    return self.__addOrModifyElement( 'PolicyResultLog', locals() )  

  ################################################################################
  # Protected methods

#  def _insertElement( self, element, kwargs ):
#    '''
#      Method that executes the insert method of the given element.
#    '''
#    fname = 'insert%s' % element
#    fElem = getattr( self, fname )
#    return fElem( **kwargs )
#  def _updateElement( self, element, kwargs ): 
#    '''
#      Method that executes the update method of the given element.
#    '''    
#    fname = 'update%s' % element
#    fElem = getattr( self, fname )
#    return fElem( **kwargs )
#  def _selectElement( self, element, kwargs ):
#    '''
#      Method that executes the get method of the given element.
#    '''
#    fname = 'select%s' % element
#    fElem = getattr( self, fname )
#    return fElem( **kwargs )
#  def _deleteElement( self, element, kwargs ):
#    '''
#      Method that executes the delete method of the given element.
#    '''    
#    fname = 'delete%s' % element
#    fElem = getattr( self, fname )
#    return fElem( **kwargs )

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

#  def __addOrModifyElement( self, element, kwargs ):
#    '''
#      Method that executes update if the item is not new, otherwise inserts it
#      on the element table.
#    '''
#    # Remove 'self' key added by locals() 
#    del kwargs[ 'self' ]
#          
#    kwargs[ 'meta' ] = { 'onlyUniqueKeys' : True }
#    sqlQuery = self._selectElement( element, kwargs )   
#    if not sqlQuery[ 'OK' ]:
#      return sqlQuery
#    
#    # No meta used to insert or update    
#    del kwargs[ 'meta' ]
#    
#    _now = datetime.utcnow().replace( microsecond = 0 ) 
#    if 'lastCheckTime' in kwargs:
#      kwargs[ 'lastCheckTime' ] = _now
#    
#    if sqlQuery[ 'Value' ]:             
#      
#      return self._updateElement( element, kwargs )
#    else:
#      # If we are inserting, we force dateEffective to be now
#      if 'dateEffective' in kwargs:
#        kwargs[ 'dateEffective' ] = _now
#      
#      return self._insertElement( element, kwargs ) 
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF