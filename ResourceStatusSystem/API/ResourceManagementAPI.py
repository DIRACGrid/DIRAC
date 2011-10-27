################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC.ResourceStatusSystem.API.private.ResourceManagementExtendedBaseAPI import ResourceManagementExtendedBaseAPI
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
    - udpate + <TableName>
    - get + <TableName>
    - delete + <TableName>
    
  If you want to use it, you can do it as follows:
  
   >>> from DIRAC.ResourceStatusSystem.API.ResourceManagementAPI import ResourceManagementAPI
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
    :Parameters:
      **hashEnv** - `string`
        hash for the given environment and site 
      **siteName** - `string`
        name of the site
      **environment** - `string`
        environment to be cached
      **\*\*kwargs** - `[,dict]`
        metadata for the mysql query. It will be filled automatically with the\
       `table` key and the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    pass
  @APIDecorator
  def updateEnvironmentCache( self, hashEnv, siteName, environment, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def getEnvironmentCache( self, hashEnv = None, siteName = None, 
                           environment = None, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def deleteEnvironmentCache( self, hashEnv = None, siteName = None, 
                              environment = None, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def insertPolicyResult( self, granularity, name, policyName, statusType,
                          status, reason, dateEffective, lastCheckTime,
                          **kwargs ):
    '''
    '''
    pass 
  @APIDecorator
  def updatePolicyResult( self, granularity, name, policyName, statusType,
                          status, reason, dateEffective, lastCheckTime, 
                          **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def getPolicyResult( self, granularity = None, name = None, policyName = None, 
                       statusType = None, status = None, reason = None, 
                       dateEffective = None, lastCheckTime = None, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def deletePolicyResult( self, granularity = None, name = None, policyName = None, 
                          statusType = None, status = None, reason = None, 
                          dateEffective = None, lastCheckTime = None, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def insertClientCache( self, name, commandName, opt_ID, value, result,
                         dateEffective, lastCheckTime, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def updateClientCache( self, name, commandName, opt_ID, value, result,
                         dateEffective, lastCheckTime, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def getClientCache( self, name = None, commandName = None, opt_ID = None, 
                      value = None, result = None, dateEffective = None, 
                      lastCheckTime = None, **kwargs ):
    '''
    '''
    pass
  @APIDecorator 
  def deleteClientCache( self, name = None, commandName = None, opt_ID = None, 
                         value = None, result = None, dateEffective = None, 
                         lastCheckTime = None, **kwargs ):
    '''
    '''
    pass  
  @APIDecorator
  def insertAccountingCache( self, name, plotType, plotName, result, 
                             dateEffective, lastCheckTime, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def updateAccountingCache( self, name, plotType, plotName, result, 
                             dateEffective, lastCheckTime, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def getAccountingCache( self, name = None, plotType = None, plotName = None, 
                          result = None, dateEffective = None, 
                          lastCheckTime = None, **kwargs ):
    '''
    '''
    pass
  @APIDecorator
  def deleteAccountingCache( self, name = None, plotType = None, plotName = None, 
                             result = None, dateEffective = None, 
                             lastCheckTime = None, **kwargs ):
    '''
    '''
    pass  
  @APIDecorator
  def insertUserRegistryCache( self, login, name, email, **kwargs ):
    '''
    '''
    pass

  @APIDecorator
  def updateUserRegistryCache( self, login, name, email, **kwargs ):
    '''
    '''
    pass
  
  @APIDecorator
  def getUserRegistryCache( self, login = None, name = None, email = None, 
                            **kwargs ):
    '''
    '''
    pass
  
  @APIDecorator 
  def deleteUserRegistryCache( self, login = None, name = None, email = None, 
                               **kwargs ):                                            
    '''
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
    '''
    pass
  @APIDecorator
  def addOrModifyPolicyResult( self, granularity, name, policyName, statusType,
                               status, reason, dateEffective, lastCheckTime ):
    '''
    '''
    pass
  @APIDecorator
  def addOrModifyClientCache( self, name, commandName, opt_ID, value, result,
                              dateEffective, lastCheckTime ):
    '''
    '''
    pass
  @APIDecorator
  def addOrModifyAccountingCache( self, name, plotType, plotName, result, 
                                  dateEffective, lastCheckTime ):
    '''
    '''
    pass
  @APIDecorator
  def addOrModifyUserRegistryCache( self, login, name, email ):
    '''
    '''
    pass  

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF      