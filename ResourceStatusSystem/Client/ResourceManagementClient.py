################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC.Core.DISET.RPCClient                                     import RPCClient
from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB             import ResourceManagementDB

from DIRAC.ResourceStatusSystem.Utilities.Decorators import ClientFastDec

from datetime import datetime

class ResourceManagementClient:
  """
  The :class:`ResourceManagementClient` class exposes the :mod:`DIRAC.ResourceManagement` 
  API. All functions you need are on this client.
  
  It has the 'direct-db-access' functions, the ones of the type:
   - insert
   - update
   - get
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
      try:
        self.gate = ResourceManagementDB()
      except Exception:
        self.gate = RPCClient( "ResourceStatus/ResourceManagement" )        
    else:
      self.gate = serviceIn    

  @ClientFastDec
  def insertEnvironmentCache( self, hashEnv, siteName, environment, meta = {} ):
    '''
    Inserts on EnvironmentCache a new row with the arguments given.
    
    :Parameters:
      **hashEnv** - `string`
        hash for the given environment and site 
      **siteName** - `string`
        name of the site
      **environment** - `string`
        environment to be cached
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    return locals()
  @ClientFastDec
  def updateEnvironmentCache( self, hashEnv, siteName, environment, meta = {} ):
    '''
    Updates EnvironmentCache with the parameters given. By default, `hashEnv`
    will be the parameter used to select the row. 
    
    :Parameters:
      **hashEnv** - `string`
        hash for the given environment and site 
      **siteName** - `string`
        name of the site
      **environment** - `string`
        environment to be cached
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    return locals()
  @ClientFastDec
  def getEnvironmentCache( self, hashEnv = None, siteName = None, 
                           environment = None, meta = {} ):
    '''
    Gets from EnvironmentCache all rows that match the parameters given.
    
    :Parameters:
      **hashEnv** - `[, string, list]`
        hash for the given environment and site 
      **siteName** - `[,string, list]`
        name of the site
      **environment** - `[, string, list]`
        environment to be cached
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    return locals()
  @ClientFastDec
  def deleteEnvironmentCache( self, hashEnv = None, siteName = None, 
                              environment = None, meta = {} ):
    '''
    Deletes from EnvironmentCache all rows that match the parameters given.
    
    :Parameters:
      **hashEnv** - `[, string, list]`
        hash for the given environment and site 
      **siteName** - `[, string, list]`
        name of the site
      **environment** - `[, string, list]`
        environment to be cached
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    return locals()
  @ClientFastDec
  def insertPolicyResult( self, granularity, name, policyName, statusType,
                          status, reason, dateEffective, lastCheckTime,
                          meta = {} ):
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
      **dateEffective** - `datetime`
        time-stamp from which the policy result is effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the policy result was checked
      **meta** - `[,dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    return locals() 
  @ClientFastDec
  def updatePolicyResult( self, granularity, name, policyName, statusType,
                          status, reason, dateEffective, lastCheckTime, 
                          meta = {} ):
    '''
    Updates PolicyResult with the parameters given. By default, `name`, 
    `policyName` and `statusType` will be the parameters used to select the row.
    
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
      **dateEffective** - `datetime`
        time-stamp from which the policy result is effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the policy result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    return locals()
  @ClientFastDec
  def getPolicyResult( self, granularity = None, name = None, policyName = None, 
                       statusType = None, status = None, reason = None, 
                       dateEffective = None, lastCheckTime = None, meta = {} ):
    '''
    Gets from PolicyResult all rows that match the parameters given.
    
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
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the policy result is effective
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the policy result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    return locals()
  @ClientFastDec
  def deletePolicyResult( self, granularity = None, name = None, 
                          policyName = None, statusType = None, status = None, 
                          reason = None, dateEffective = None, 
                          lastCheckTime = None, meta = {} ):
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
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the policy result is effective
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the policy result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    return locals()
  @ClientFastDec
  def insertClientCache( self, name, commandName, opt_ID, value, result,
                         dateEffective, lastCheckTime, meta = {} ):
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
    return locals()
  @ClientFastDec
  def updateClientCache( self, name, commandName, opt_ID, value, result,
                         dateEffective, lastCheckTime, meta = {} ):
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
    return locals()
  @ClientFastDec
  def getClientCache( self, name = None, commandName = None, opt_ID = None, 
                      value = None, result = None, dateEffective = None, 
                      lastCheckTime = None, meta = {} ):
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
    return locals()
  @ClientFastDec 
  def deleteClientCache( self, name = None, commandName = None, opt_ID = None, 
                         value = None, result = None, dateEffective = None, 
                         lastCheckTime = None, meta = {} ):
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
    return locals()
  @ClientFastDec
  def insertAccountingCache( self, name, plotType, plotName, result, 
                             dateEffective, lastCheckTime, meta = {} ):
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
    return locals()
  @ClientFastDec
  def updateAccountingCache( self, name, plotType, plotName, result, 
                             dateEffective, lastCheckTime, meta = {} ):
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
    return locals()
  @ClientFastDec
  def getAccountingCache( self, name = None, plotType = None, plotName = None, 
                          result = None, dateEffective = None, 
                          lastCheckTime = None, meta = {} ):
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
    return locals()
  @ClientFastDec
  def deleteAccountingCache( self, name = None, plotType = None, 
                             plotName = None, result = None, 
                             dateEffective = None, lastCheckTime = None, 
                             meta = {} ):
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
    return locals()
  @ClientFastDec
  def insertUserRegistryCache( self, login, name, email, meta = {} ):
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
    return locals()
  @ClientFastDec
  def updateUserRegistryCache( self, login, name, email, meta = {} ):
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
    return locals()
  @ClientFastDec
  def getUserRegistryCache( self, login = None, name = None, email = None, 
                            meta = {} ):
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
    return locals()
  @ClientFastDec 
  def deleteUserRegistryCache( self, login = None, name = None, email = None, 
                               meta = {} ):                                            
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
    return locals()

  '''
  ##############################################################################
  # EXTENDED BASE API METHODS
  ##############################################################################
  '''

  def addOrModifyEnvironmentCache( self, hashEnv, siteName, environment ):
    '''
    Using `hashEnv` to query the database, decides whether to insert or update
    the table.
    
    :Parameters:
      **hashEnv** - `string`
        hash for the given environment and site 
      **siteName** - `string`
        name of the site
      **environment** - `string`
        environment to be cached
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    return self.__addOrModifyElement( 'EnvironmentCache', locals() )

  def addOrModifyPolicyResult( self, granularity, name, policyName, statusType,
                               status, reason, dateEffective, lastCheckTime ):
    '''
    Using `name`, `policyName` and `statusType` to query the database, 
    decides whether to insert or update the table.

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
      **dateEffective** - `datetime`
        time-stamp from which the policy result is effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the policy result was checked
      **meta** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    return self.__addOrModifyElement( 'PolicyResult', locals() )

  def addOrModifyClientCache( self, name, commandName, opt_ID, value, result,
                              dateEffective, lastCheckTime ):
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
    return self.__addOrModifyElement( 'ClientCache', locals() )

  def addOrModifyAccountingCache( self, name, plotType, plotName, result, 
                                  dateEffective, lastCheckTime ):
    '''
    Using `name`, `plotType` and `plotName` to query the database, 
    decides whether to insert or update the table.
    
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
    return self.__addOrModifyElement( 'AccountingCache', locals() )

  def addOrModifyUserRegistryCache( self, login, name, email ):
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
    return self.__addOrModifyElement( 'UserRegistryCache', locals() ) 

################################################################################

  '''
  ##############################################################################
  # Getter functions
  ##############################################################################
  '''

  def _insertElement( self, element, **kwargs ):
    
    fname = 'insert%s' % element
    f = getattr( self, fname )
    return f( **kwargs )

  def _updateElement( self, element, **kwargs ): 
    
    fname = 'update%s' % element
    f = getattr( self, fname )
    return f( **kwargs )

  def _getElement( self, element, **kwargs ):
    
    fname = 'get%s' % element
    f = getattr( self, fname )
    return f( **kwargs )

  def _deleteElement( self, element, **kwargs ):    
    fname = 'delete%s' % element
    f = getattr( self, fname )
    return f( **kwargs )

  '''
  ##############################################################################
  # addOrModify PRIVATE FUNCTIONS
  ##############################################################################
  ''' 
  def __addOrModifyElement( self, element, kwargs ):
       
    del kwargs[ 'self' ]
       
    kwargs[ 'meta' ] = { 'onlyUniqueKeys' : True }
    
    sqlQuery = self._getElement( element, **kwargs )   
        
    del kwargs[ 'meta' ]
    
    if sqlQuery[ 'Value' ]:      
      #if element in [ 'PolicyResult', 'ClientCache', 'AccountingCache' ]:
      if kwargs.has_key( 'lastCheckTime' ):
        kwargs[ 'lastCheckTime' ] = datetime.utcnow().replace( microsecond = 0 )      
      
      return self._updateElement( element, **kwargs )
    else: 
      
      if kwargs.has_key( 'lastCheckTime' ):
        kwargs[ 'lastCheckTime' ] = datetime.utcnow().replace( microsecond = 0 )
      if kwargs.has_key( 'dateEffective' ):
        kwargs[ 'dateEffective' ] = datetime.utcnow().replace( microsecond = 0 )
      
      return self._insertElement( element, **kwargs ) 
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF