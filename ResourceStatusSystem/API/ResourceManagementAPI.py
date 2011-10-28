################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC.ResourceStatusSystem.API.private.ResourceManagementExtendedBaseAPI \
  import ResourceManagementExtendedBaseAPI
from DIRAC.ResourceStatusSystem.Utilities.Decorators import APIDecorator

class ResourceManagementAPI( object ):
  '''
  The :class:`ResourceManagementAPI` class exposes all methods needed by RSS to
  interact with the database. This includes methods that interact directly with
  the database, and methods that actually do some processing using the outputs
  of the first ones.
  
  The methods that `directly` ( though the client ) access the database follow
  this convention:
  
    - insert + <TableName>
    - update + <TableName>
    - get + <TableName>
    - delete + <TableName>
    
  If you want to use it, you can do it as follows:
  
   >>> from DIRAC.ResourceStatusSystem.API.ResourceManagementAPI import \
   ResourceManagementAPI
   >>> rmAPI = ResourceManagementAPI()
   >>> rmAPI.getEnvironmentCache()
   
  All `direct database access` functions have the possibility of using keyword 
  arguments to tune the SQL queries.   
  '''
  
  def __init__( self ):
    self.eBaseAPI = ResourceManagementExtendedBaseAPI()
  
  '''  
  ##############################################################################    
  # BASE API METHODS
  ##############################################################################
  '''

  @APIDecorator
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
  @APIDecorator
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
  @APIDecorator
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
  @APIDecorator
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
  @APIDecorator
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
  @APIDecorator
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
  @APIDecorator
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
  @APIDecorator
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
  @APIDecorator
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
  @APIDecorator
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
  @APIDecorator
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
  @APIDecorator 
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
  @APIDecorator
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
  @APIDecorator
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
  @APIDecorator
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
  @APIDecorator
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
  @APIDecorator
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
  @APIDecorator
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
  @APIDecorator
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
  @APIDecorator 
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
  
  @APIDecorator
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
    pass
  @APIDecorator
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
    pass
  @APIDecorator
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
    pass
  @APIDecorator
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
    pass
  @APIDecorator
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
    pass  

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF      