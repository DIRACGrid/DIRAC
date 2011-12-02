from DIRAC.ResourceStatusSystem.DB.mock.ResourceManagementDB import ResourceManagementDB

from DIRAC.ResourceStatusSystem.Utilities.Decorators import ClientFastDec

class ResourceManagementClient( object ):
  
  def __init__( self, serviceIn = None ):
    
    if serviceIn is None:
      self.gate = ResourceManagementDB()
    else:
      self.gate = serviceIn  
    
  @ClientFastDec
  def insertEnvironmentCache( self, hashEnv, siteName, environment, **kwargs ):
    return locals()

  @ClientFastDec
  def updateEnvironmentCache( self, hashEnv, siteName, environment, **kwargs ):
    return locals()
  @ClientFastDec
  def getEnvironmentCache( self, hashEnv = None, siteName = None, 
                           environment = None, **kwargs ):
    return locals()
  @ClientFastDec
  def deleteEnvironmentCache( self, hashEnv = None, siteName = None, 
                              environment = None, **kwargs ):
    return locals()
  @ClientFastDec
  def insertPolicyResult( self, granularity, name, policyName, statusType,
                          status, reason, dateEffective, lastCheckTime,
                          **kwargs ):
    return locals() 
  @ClientFastDec
  def updatePolicyResult( self, granularity, name, policyName, statusType,
                          status, reason, dateEffective, lastCheckTime, 
                          **kwargs ):
    return locals()
  @ClientFastDec
  def getPolicyResult( self, granularity = None, name = None, policyName = None, 
                       statusType = None, status = None, reason = None, 
                       dateEffective = None, lastCheckTime = None, **kwargs ):
    return locals()
  @ClientFastDec
  def deletePolicyResult( self, granularity = None, name = None, 
                          policyName = None, statusType = None, status = None, 
                          reason = None, dateEffective = None, 
                          lastCheckTime = None, **kwargs ):
    return locals()
  @ClientFastDec
  def insertClientCache( self, name, commandName, opt_ID, value, result,
                         dateEffective, lastCheckTime, **kwargs ):
    return locals()
  @ClientFastDec
  def updateClientCache( self, name, commandName, opt_ID, value, result,
                         dateEffective, lastCheckTime, **kwargs ): 
    return locals()
  @ClientFastDec
  def getClientCache( self, name = None, commandName = None, opt_ID = None, 
                      value = None, result = None, dateEffective = None, 
                      lastCheckTime = None, **kwargs ):
    return locals()
  @ClientFastDec 
  def deleteClientCache( self, name = None, commandName = None, opt_ID = None, 
                         value = None, result = None, dateEffective = None, 
                         lastCheckTime = None, **kwargs ):
    return locals()  
  @ClientFastDec
  def insertAccountingCache( self, name, plotType, plotName, result, 
                             dateEffective, lastCheckTime, **kwargs ): 
    return locals()
  @ClientFastDec
  def updateAccountingCache( self, name, plotType, plotName, result, 
                             dateEffective, lastCheckTime, **kwargs ):
    return locals()
  @ClientFastDec
  def getAccountingCache( self, name = None, plotType = None, plotName = None, 
                          result = None, dateEffective = None, 
                          lastCheckTime = None, **kwargs ):  
    return locals()
  @ClientFastDec
  def deleteAccountingCache( self, name = None, plotType = None, 
                             plotName = None, result = None, 
                             dateEffective = None, lastCheckTime = None, 
                             **kwargs ): 
    return locals()  
  @ClientFastDec
  def insertUserRegistryCache( self, login, name, email, **kwargs ): 
    return locals()
  @ClientFastDec
  def updateUserRegistryCache( self, login, name, email, **kwargs ):
    return locals()
  @ClientFastDec
  def getUserRegistryCache( self, login = None, name = None, email = None, 
                            **kwargs ):
    return locals()
  @ClientFastDec 
  def deleteUserRegistryCache( self, login = None, name = None, email = None, 
                               **kwargs ):                                             
    return locals()

  '''
  ##############################################################################
  # EXTENDED BASE API METHODS
  ##############################################################################
  '''

  def addOrModifyEnvironmentCache( self, hashEnv, siteName, environment ):
    return { 'OK' : True, 'Value' : [] }

  def addOrModifyPolicyResult( self, granularity, name, policyName, statusType,
                               status, reason, dateEffective, lastCheckTime ):
    return { 'OK' : True, 'Value' : [] }
  def addOrModifyClientCache( self, name, commandName, opt_ID, value, result,
                              dateEffective, lastCheckTime ):
    return { 'OK' : True, 'Value' : [] }
  
  def addOrModifyAccountingCache( self, name, plotType, plotName, result, 
                                  dateEffective, lastCheckTime ):
    return { 'OK' : True, 'Value' : [] }
  
  def addOrModifyUserRegistryCache( self, login, name, email ):
    return { 'OK' : True, 'Value' : [] }