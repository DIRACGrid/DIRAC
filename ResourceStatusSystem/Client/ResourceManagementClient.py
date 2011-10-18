################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC.Core.DISET.RPCClient                                     import RPCClient

from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB             import ResourceManagementDB
from DIRAC.ResourceStatusSystem.Utilities.Decorators                import ClientDec
from DIRAC.ResourceStatusSystem.Utilities.ResourceManagementBooster import ResourceManagementBooster

class ResourceManagementClient:
  """
  The ResourceManagementClient class exposes the ResourceStatus API. All functions
  you need are on this client.
  
  It has the 'direct-db-access' functions, the ones of the type:
    o insert
    o update
    o get
    o delete 
  
  plus a set of functions of the type:
    o getValid 
    
  that return parts of the RSSConfiguration stored on the CS, and used everywhere
  on the RSS module. Finally, and probably more interesting, it exposes a set
  of functions, badly called 'boosters'. They are 'home made' functions using the
  basic database functions that are interesting enough to be exposed.  
  
  The client will ALWAYS try to connect to the DB, and in case of failure, to the
  XML-RPC server ( namely ResourceManagementDB and ResourceManagementHandler ).

  You can use this client on this way

   >>> from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import \
         ResourceManagementClient
   >>> rsClient = ResourceManagementClient()
   
  If you want to know more about ResourceManagementClient, scroll down to the end of
  the file.  
  """
  
  def __init__( self , serviceIn = None ):
 
    if serviceIn == None:
      try:
        self.gate = ResourceManagementDB()
      except Exception:
        self.gate = RPCClient( "ResourceStatus/ResourceManagement" )        
    else:
      self.gate = serviceIn
      
    self.booster = ResourceManagementBooster( self )  

################################################################################
# DB ###########################################################################

  '''
  ##############################################################################
  # ENVIRONMENT CACHE FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertEnvironmentCache( self, hashEnv, siteName, environment ):
    pass
  @ClientDec
  def updateEnvironmentCache( self, hashEnv, siteName, environment ):
    pass
  @ClientDec
  def getEnvironmentCache( self, hashEnv = None, siteName = None, 
                           environment = None, **kwargs ):
    pass
  @ClientDec
  def deleteEnvironmentCache( self, hashEnv = None, siteName = None, 
                              environment = None, **kwargs ):
    pass

# DB ###########################################################################
# DB ###########################################################################
  
  '''
  ##############################################################################
  # POLICY RESULT FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertPolicyResult( self, granularity, name, policyName, statusType,
                               status, reason, dateEffective, lastCheckTime ):
    pass 
  @ClientDec
  def updatePolicyResult( self, granularity, name, policyName, statusType,
                               status, reason, dateEffective, lastCheckTime ):
    pass
  @ClientDec
  def getPolicyResult( self, granularity = None, name = None, policyName = None, 
                       statusType = None, status = None, reason = None, 
                       dateEffective = None, lastCheckTime = None, **kwargs ):
    pass
  @ClientDec
  def deletePolicyResult( self, granularity = None, name = None, policyName = None, 
                          statusType = None, status = None, reason = None, 
                          dateEffective = None, lastCheckTime = None, **kwargs ):
    pass

# DB ###########################################################################
# DB ###########################################################################

  '''
  ##############################################################################
  # CLIENT CACHE FUNCTIONS
  ##############################################################################
  '''    
  @ClientDec
  def insertClientCache( self, name, commandName, opt_ID, value, result,
                         dateEffective, lastCheckTime ):
    pass
  @ClientDec
  def updateClientCache( self, name, commandName, opt_ID, value, result,
                         dateEffective, lastCheckTime ):
    pass
  @ClientDec
  def getClientCache( self, name = None, commandName = None, opt_ID = None, 
                      value = None, result = None, dateEffective = None, 
                      lastCheckTime = None, **kwargs ):
    pass
  @ClientDec 
  def deleteClientCache( self, name = None, commandName = None, opt_ID = None, 
                         value = None, result = None, dateEffective = None, 
                         lastCheckTime = None, **kwargs ):
    pass

# DB ###########################################################################
# DB ###########################################################################

  '''
  ##############################################################################
  # ACCOUNTING CACHE FUNCTIONS
  ##############################################################################
  '''
  @ClientDec
  def insertAccountingCache( self, name, plotType, plotName, result, 
                             dateEffective, lastCheckTime ):
    pass
  @ClientDec
  def updateAccountingCache( self, name, plotType, plotName, result, 
                             dateEffective, lastCheckTime ):
    pass
  @ClientDec
  def getAccountingCache( self, name = None, plotType = None, plotName = None, 
                          result = None, dateEffective = None, 
                          lastCheckTime = None, **kwargs ):
    pass
  @ClientDec
  def deleteAccountingCache( self, name = None, plotType = None, plotName = None, 
                             result = None, dateEffective = None, 
                             lastCheckTime = None, **kwargs ):
    pass

# DB ###########################################################################
# DB ###########################################################################
  
  '''
  ##############################################################################
  # USER REGISTRY FUNCTIONS
  ##############################################################################
  '''  
  
  @ClientDec
  def insertUserRegistryCache( self, login, name, email ):
    pass

  @ClientDec
  def updateUserRegistryCache( self, login, name, email ):
    pass
  
  @ClientDec
  def getUserRegistryCache( self, login = None, name = None, email = None, 
                            **kwargs ):
    pass
  
  @ClientDec 
  def deleteUserRegistryCache( self, login = None, name = None, email = None, 
                               **kwargs ):                                            
    pass

# DB ###########################################################################
# BOOSTER ######################################################################

  '''
  ##############################################################################
  # DB specific Boosters
  ##############################################################################
  '''
  @ClientDec
  def addOrModifyEnvironmentCache( self, hashEnv, siteName, environment ):
    pass
  @ClientDec
  def addOrModifyPolicyResult( self, granularity, name, policyName, statusType,
                               status, reason, dateEffective, lastCheckTime ):
    pass
  @ClientDec
  def addOrModifyClientCache( self, name, commandName, opt_ID, value, result,
                              dateEffective, lastCheckTime ):
    pass
  @ClientDec
  def addOrModifyAccountingCache( self, name, plotType, plotName, result, 
                                  dateEffective, lastCheckTime ):
    pass
  @ClientDec
  def addOrModifyUserRegistryCache( self, login, name, email ):
    pass  

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF