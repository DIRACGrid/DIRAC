from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Utilities.Decorators            import ClientDec5

from DIRAC import S_OK

class ResourceManagementBaseAPI( object ):
  
  def __init__( self ):
    self.client = ResourceManagementClient()
    
  @ClientDec5
  def insertEnvironmentCache( self, hashEnv, siteName, environment, **kwargs ):
    pass
  @ClientDec5
  def updateEnvironmentCache( self, hashEnv, siteName, environment, **kwargs ):
    pass
  @ClientDec5
  def getEnvironmentCache( self, hashEnv = None, siteName = None, 
                           environment = None, **kwargs ):
    pass
  @ClientDec5
  def deleteEnvironmentCache( self, hashEnv = None, siteName = None, 
                              environment = None, **kwargs ):
    pass
  @ClientDec5
  def insertPolicyResult( self, granularity, name, policyName, statusType,
                          status, reason, dateEffective, lastCheckTime,
                          **kwargs ):
    pass 
  @ClientDec5
  def updatePolicyResult( self, granularity, name, policyName, statusType,
                          status, reason, dateEffective, lastCheckTime, 
                          **kwargs ):
    pass
  @ClientDec5
  def getPolicyResult( self, granularity = None, name = None, policyName = None, 
                       statusType = None, status = None, reason = None, 
                       dateEffective = None, lastCheckTime = None, **kwargs ):
    pass
  @ClientDec5
  def deletePolicyResult( self, granularity = None, name = None, policyName = None, 
                          statusType = None, status = None, reason = None, 
                          dateEffective = None, lastCheckTime = None, **kwargs ):
    pass
  @ClientDec5
  def insertClientCache( self, name, commandName, opt_ID, value, result,
                         dateEffective, lastCheckTime, **kwargs ):
    pass
  @ClientDec5
  def updateClientCache( self, name, commandName, opt_ID, value, result,
                         dateEffective, lastCheckTime, **kwargs ):
    pass
  @ClientDec5
  def getClientCache( self, name = None, commandName = None, opt_ID = None, 
                      value = None, result = None, dateEffective = None, 
                      lastCheckTime = None, **kwargs ):
    pass
  @ClientDec5 
  def deleteClientCache( self, name = None, commandName = None, opt_ID = None, 
                         value = None, result = None, dateEffective = None, 
                         lastCheckTime = None, **kwargs ):
    pass  
  @ClientDec5
  def insertAccountingCache( self, name, plotType, plotName, result, 
                             dateEffective, lastCheckTime, **kwargs ):
    pass
  @ClientDec5
  def updateAccountingCache( self, name, plotType, plotName, result, 
                             dateEffective, lastCheckTime, **kwargs ):
    pass
  @ClientDec5
  def getAccountingCache( self, name = None, plotType = None, plotName = None, 
                          result = None, dateEffective = None, 
                          lastCheckTime = None, **kwargs ):
    pass
  @ClientDec5
  def deleteAccountingCache( self, name = None, plotType = None, plotName = None, 
                             result = None, dateEffective = None, 
                             lastCheckTime = None, **kwargs ):
    pass  
  @ClientDec5
  def insertUserRegistryCache( self, login, name, email, **kwargs ):
    pass

  @ClientDec5
  def updateUserRegistryCache( self, login, name, email, **kwargs ):
    pass
  
  @ClientDec5
  def getUserRegistryCache( self, login = None, name = None, email = None, 
                            **kwargs ):
    pass
  
  @ClientDec5 
  def deleteUserRegistryCache( self, login = None, name = None, email = None, 
                               **kwargs ):                                            
    pass      