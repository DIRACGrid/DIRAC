################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC.Core.DISET.RPCClient                                     import RPCClient
from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB             import ResourceManagementDB

from DIRAC.ResourceStatusSystem.Utilities.Decorators import ClientDec6

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
    
    if serviceIn == None:
      try:
        self.gate = ResourceManagementDB()
      except Exception:
        self.gate = RPCClient( "ResourceStatus/ResourceManagement" )        
    else:
      self.gate = serviceIn    

  @ClientDec6
  def insertEnvironmentCache( self, hashEnv, siteName, environment, **kwargs ):
    '''
    Inserts on EnvironmentCache a new row with the arguments given.
    
    :Parameters:
      **hashEnv** - `string`
        hash for the given environment and site 
      **siteName** - `string`
        name of the site
      **environment** - `string`
        environment to be cached
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @ClientDec6
  def updateEnvironmentCache( self, hashEnv, siteName, environment, **kwargs ):
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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    pass
  @ClientDec6
  def getEnvironmentCache( self, hashEnv = None, siteName = None, 
                           environment = None, **kwargs ):
    '''
    Gets from EnvironmentCache all rows that match the parameters given.
    
    :Parameters:
      **hashEnv** - `[, string, list]`
        hash for the given environment and site 
      **siteName** - `[,string, list]`
        name of the site
      **environment** - `[, string, list]`
        environment to be cached
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @ClientDec6
  def deleteEnvironmentCache( self, hashEnv = None, siteName = None, 
                              environment = None, **kwargs ):
    '''
    Deletes from EnvironmentCache all rows that match the parameters given.
    
    :Parameters:
      **hashEnv** - `[, string, list]`
        hash for the given environment and site 
      **siteName** - `[, string, list]`
        name of the site
      **environment** - `[, string, list]`
        environment to be cached
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @ClientDec6
  def insertPolicyResult( self, granularity, name, policyName, statusType,
                          status, reason, dateEffective, lastCheckTime,
                          **kwargs ):
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
      **\*\*kwargs** - `[,dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass 
  @ClientDec6
  def updatePolicyResult( self, granularity, name, policyName, statusType,
                          status, reason, dateEffective, lastCheckTime, 
                          **kwargs ):
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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @ClientDec6
  def getPolicyResult( self, granularity = None, name = None, policyName = None, 
                       statusType = None, status = None, reason = None, 
                       dateEffective = None, lastCheckTime = None, **kwargs ):
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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @ClientDec6
  def deletePolicyResult( self, granularity = None, name = None, 
                          policyName = None, statusType = None, status = None, 
                          reason = None, dateEffective = None, 
                          lastCheckTime = None, **kwargs ):
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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @ClientDec6
  def insertClientCache( self, name, commandName, opt_ID, value, result,
                         dateEffective, lastCheckTime, **kwargs ):
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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    pass
  @ClientDec6
  def updateClientCache( self, name, commandName, opt_ID, value, result,
                         dateEffective, lastCheckTime, **kwargs ):
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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    pass
  @ClientDec6
  def getClientCache( self, name = None, commandName = None, opt_ID = None, 
                      value = None, result = None, dateEffective = None, 
                      lastCheckTime = None, **kwargs ):
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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    pass
  @ClientDec6 
  def deleteClientCache( self, name = None, commandName = None, opt_ID = None, 
                         value = None, result = None, dateEffective = None, 
                         lastCheckTime = None, **kwargs ):
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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    pass  
  @ClientDec6
  def insertAccountingCache( self, name, plotType, plotName, result, 
                             dateEffective, lastCheckTime, **kwargs ):
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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    pass
  @ClientDec6
  def updateAccountingCache( self, name, plotType, plotName, result, 
                             dateEffective, lastCheckTime, **kwargs ):
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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    pass
  @ClientDec6
  def getAccountingCache( self, name = None, plotType = None, plotName = None, 
                          result = None, dateEffective = None, 
                          lastCheckTime = None, **kwargs ):
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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    pass
  @ClientDec6
  def deleteAccountingCache( self, name = None, plotType = None, 
                             plotName = None, result = None, 
                             dateEffective = None, lastCheckTime = None, 
                             **kwargs ):
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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    pass  
  @ClientDec6
  def insertUserRegistryCache( self, login, name, email, **kwargs ):
    '''
    Inserts on UserRegistryCache a new row with the arguments given.
    
    :Parameters:
      **login** - `string`
        user's login ID  
      **name** - `string`
        user's name
      **email** - `string`
        user's email
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    pass
  @ClientDec6
  def updateUserRegistryCache( self, login, name, email, **kwargs ):
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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    pass
  @ClientDec6
  def getUserRegistryCache( self, login = None, name = None, email = None, 
                            **kwargs ):
    '''
    Gets from UserRegistryCache all rows that match the parameters given.
    
    :Parameters:
      **login** - `[, string, list]`
        user's login ID  
      **name** - `[, string, list]`
        user's name
      **email** - `[, string, list]`
        user's email
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    pass
  @ClientDec6 
  def deleteUserRegistryCache( self, login = None, name = None, email = None, 
                               **kwargs ):                                            
    '''
    Deletes from UserRegistryCache all rows that match the parameters given.
    
    :Parameters:
      **login** - `[, string, list]`
        user's login ID  
      **name** - `[, string, list]`
        user's name
      **email** - `[, string, list]`
        user's email
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    pass

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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    args = ( hashEnv, siteName, environment )
    return self.__addOrModifyElement( 'EnvironmentCache', *args )

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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    args = ( granularity, name, policyName, statusType, status, reason, 
             dateEffective, lastCheckTime) 
    return self.__addOrModifyElement( 'PolicyResult', *args )

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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    args = ( name, commandName, opt_ID, value, result, dateEffective, 
             lastCheckTime )
    return self.__addOrModifyElement( 'ClientCache', *args )

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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    args = ( name, plotType, plotName, result, dateEffective, lastCheckTime )
    return self.__addOrModifyElement( 'AccountingCache', *args )

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
      **\*\*kwargs** - `[, dict]`
        meta-data for the MySQL query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    args = ( login, name, email )
    return self.__addOrModifyElement( 'UserRegistryCache', *args ) 

################################################################################

  '''
  ##############################################################################
  # Getter functions
  ##############################################################################
  '''

  def _insertElement( self, element, *args, **kwargs ):
    
    fname = 'insert%s' % element
    f = getattr( self, fname )
    return f( *args, **kwargs )

  def _updateElement( self, element, *args, **kwargs ):
    
    fname = 'update%s' % element
    f = getattr( self, fname )
    return f( *args, **kwargs )

  def _getElement( self, element, *args, **kwargs ):
    
    fname = 'get%s' % element
    f = getattr( self, fname )
    return f( *args, **kwargs )

  def _deleteElement( self, element, *args, **kwargs ):
    
    fname = 'delete%s' % element
    f = getattr( self, fname )
    return f( *args, **kwargs )

  '''
  ##############################################################################
  # addOrModify PRIVATE FUNCTIONS
  ##############################################################################
  ''' 
  def __addOrModifyElement( self, element, *args ):
       
    kwargs = { 'onlyUniqueKeys' : True }
    sqlQuery = self._getElement( element, *args, **kwargs )     
    
    if sqlQuery[ 'Value' ]:      
      if element in [ 'PolicyResult', 'ClientCache', 'AccountingCache' ]:
        args = list( args )
        #Force lastCheckTime to now if not set
        if args[ -1 ] is None:
          args[ -1 ] = datetime.utcnow().replace( microsecond = 0 )
        args = tuple( args )
      
      return self._updateElement( element, *args )
    else: 
      if element in [ 'PolicyResult', 'ClientCache', 'AccountingCache' ]:
        args = list( args )
        #Force dateEffective to now if not set
        if args[ -2 ] is None:
          args[ -2 ] = datetime.utcnow().replace( microsecond = 0 )
        #Force lastCheckTime to now if not set
        if args[ -1 ] is None:
          args[ -1 ] = datetime.utcnow().replace( microsecond = 0 )
        args = tuple( args )
      
      return self._insertElement( element, *args ) 


#  def insert( self, *args, **kwargs ):
#    '''
#    This method calls the insert function in :class:`ResourceManagementDB`, either
#    directly or remotely through the RPC Server :class:`ResourceManagementHandler`. 
#    It does not add neither processing nor validation. If you need to know more 
#    about this method, you must keep reading on the database documentation.     
#      
#    :param args: Tuple with the arguments for the insert function. 
#    :type  args: tuple
#    :param kwargs: Dictionary with the keyworded arguments for the insert\
#      function. At least, kwargs contains the key table, with the table in which 
#      args are going to be inserted.
#    :type kwargs: dict
#    :returns: Dictionary with key Value if execution successful, otherwise key\
#      Message with logs.
#    :rtype: S_OK || S_ERROR
#    '''
#    return self.gate.insert( args, kwargs )
#
#  def update( self, *args, **kwargs ):
#    '''
#    This method calls the update function in :class:`ResourceManagementDB`, either
#    directly or remotely through the RPC Server :class:`ResourceManagementHandler`. 
#    It does not add neither processing nor validation. If you need to know more 
#    about this method, you must keep reading on the database documentation.
#      
#    :Parameters:
#      **\*args** - `[,tuple]`
#        arguments for the mysql query ( must match table columns ! ).
#    
#      **\*\*kwargs** - `[,dict]`
#        metadata for the mysql query. It must contain, at least, `table` key
#        with the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    return self.gate.update( args, kwargs )
#
#  def get( self, *args, **kwargs ):
#    '''
#    This method calls the get function in :class:`ResourceManagementDB`, either
#    directly or remotely through the RPC Server :class:`ResourceManagementHandler`. 
#    It does not add neither processing nor validation. If you need to know more 
#    about this method, you must keep reading on the database documentation.
#      
#    :Parameters:
#      **\*args** - `[,tuple]`
#        arguments for the mysql query ( must match table columns ! ).
#    
#      **\*\*kwargs** - `[,dict]`
#        metadata for the mysql query. It must contain, at least, `table` key
#        with the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''  
#    return self.gate.get( args, kwargs )
#
#  def delete( self, *args, **kwargs ):
#    '''
#    This method calls the delete function in :class:`ResourceManagementDB`, either
#    directly or remotely through the RPC Server :class:`ResourceManagementHandler`. 
#    It does not add neither processing nor validation. If you need to know more 
#    about this method, you must keep reading on the database documentation.
#      
#    :Parameters:
#      **\*args** - `[,tuple]`
#        arguments for the mysql query ( must match table columns ! ).
#    
#      **\*\*kwargs** - `[,dict]`
#        metadata for the mysql query. It must contain, at least, `table` key
#        with the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''    
#    return self.gate.delete( args, kwargs )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF