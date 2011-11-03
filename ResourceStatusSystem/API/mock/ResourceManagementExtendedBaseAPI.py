from DIRAC.ResourceStatusSystem.API.mock.ResourceManagementBaseAPI import \
  ResourceManagementBaseAPI

from DIRAC import S_OK

class ResourceManagementExtendedBaseAPI( ResourceManagementBaseAPI ):
  
  def addOrModifyEnvironmentCache( self, hashEnv, siteName, environment ):
    return S_OK()
  
  def addOrModifyPolicyResult( self, granularity, name, policyName, statusType,
                               status, reason, dateEffective, lastCheckTime ):
    return S_OK()
  
  def addOrModifyClientCache( self, name, commandName, opt_ID, value, result,
                              dateEffective, lastCheckTime ):
    return S_OK()
  
  def addOrModifyAccountingCache( self, name, plotType, plotName, result, 
                                  dateEffective, lastCheckTime ):
    return S_OK()
    
  def addOrModifyUserRegistryCache( self, login, name, email ):
    return S_OK()  