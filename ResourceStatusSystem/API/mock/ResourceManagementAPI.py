from DIRAC.ResourceStatusSystem.API.mock.ResourceManagementExtendedBaseAPI \
  import ResourceManagementExtendedBaseAPI
from DIRAC.ResourceStatusSystem.Utilities.Decorators import APIDecorator

class ResourceManagementAPI( object ):
  
  def __init__( self ):
    self.eBaseAPI = ResourceManagementExtendedBaseAPI()
    
  @APIDecorator
  def insertEnvironmentCache( self, hashEnv, siteName, environment, **kwargs ):
    pass

  @APIDecorator
  def updateEnvironmentCache( self, hashEnv, siteName, environment, **kwargs ):
    pass

  @APIDecorator
  def getEnvironmentCache( self, hashEnv = None, siteName = None, 
                           environment = None, **kwargs ):
    pass

  @APIDecorator
  def deleteEnvironmentCache( self, hashEnv = None, siteName = None, 
                              environment = None, **kwargs ):
    pass

  @APIDecorator
  def insertPolicyResult( self, granularity, name, policyName, statusType,
                          status, reason, dateEffective, lastCheckTime,
                          **kwargs ):
    pass 

  @APIDecorator
  def updatePolicyResult( self, granularity, name, policyName, statusType,
                          status, reason, dateEffective, lastCheckTime, 
                          **kwargs ):
    pass

  @APIDecorator
  def getPolicyResult( self, granularity = None, name = None, policyName = None, 
                       statusType = None, status = None, reason = None, 
                       dateEffective = None, lastCheckTime = None, **kwargs ):
    pass

  @APIDecorator
  def deletePolicyResult( self, granularity = None, name = None, 
                          policyName = None, statusType = None, status = None, 
                          reason = None, dateEffective = None, 
                          lastCheckTime = None, **kwargs ):
    pass

  @APIDecorator
  def insertClientCache( self, name, commandName, opt_ID, value, result,
                         dateEffective, lastCheckTime, **kwargs ):
    pass
  
  @APIDecorator
  def updateClientCache( self, name, commandName, opt_ID, value, result,
                         dateEffective, lastCheckTime, **kwargs ):
    pass

  @APIDecorator
  def getClientCache( self, name = None, commandName = None, opt_ID = None, 
                      value = None, result = None, dateEffective = None, 
                      lastCheckTime = None, **kwargs ):
    pass

  @APIDecorator 
  def deleteClientCache( self, name = None, commandName = None, opt_ID = None, 
                         value = None, result = None, dateEffective = None, 
                         lastCheckTime = None, **kwargs ):
    pass  

  @APIDecorator
  def insertAccountingCache( self, name, plotType, plotName, result, 
                             dateEffective, lastCheckTime, **kwargs ):
    pass

  @APIDecorator
  def updateAccountingCache( self, name, plotType, plotName, result, 
                             dateEffective, lastCheckTime, **kwargs ):
    pass

  @APIDecorator
  def getAccountingCache( self, name = None, plotType = None, plotName = None, 
                          result = None, dateEffective = None, 
                          lastCheckTime = None, **kwargs ):
    pass

  @APIDecorator
  def deleteAccountingCache( self, name = None, plotType = None, 
                             plotName = None, result = None, 
                             dateEffective = None, lastCheckTime = None, 
                             **kwargs ):
    pass  

  @APIDecorator
  def insertUserRegistryCache( self, login, name, email, **kwargs ):
    pass

  @APIDecorator
  def updateUserRegistryCache( self, login, name, email, **kwargs ):
    pass

  @APIDecorator
  def getUserRegistryCache( self, login = None, name = None, email = None, 
                            **kwargs ):
    pass

  @APIDecorator 
  def deleteUserRegistryCache( self, login = None, name = None, email = None, 
                               **kwargs ):                                            
    pass

  @APIDecorator
  def addOrModifyEnvironmentCache( self, hashEnv, siteName, environment ):
    pass

  @APIDecorator
  def addOrModifyPolicyResult( self, granularity, name, policyName, statusType,
                               status, reason, dateEffective, lastCheckTime ):
    pass

  @APIDecorator
  def addOrModifyClientCache( self, name, commandName, opt_ID, value, result,
                              dateEffective, lastCheckTime ):
    pass

  @APIDecorator
  def addOrModifyAccountingCache( self, name, plotType, plotName, result, 
                                  dateEffective, lastCheckTime ):
    pass

  @APIDecorator
  def addOrModifyUserRegistryCache( self, login, name, email ):
    pass  
      