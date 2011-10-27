################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC.Core.DISET.RPCClient                                     import RPCClient
from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB             import ResourceManagementDB

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

  def insert( self, *args, **kwargs ):
    '''
    This method calls the insert function in :class:`ResourceManagementDB`, either
    directly or remotely through the RPC Server :class:`ResourceManagementHandler`. 
    It does not add neither processing nor validation. If you need to know more 
    about this method, you must keep reading on the database documentation.     
      
    :param args: Tuple with the arguments for the insert function. 
    :type  args: tuple
    :param kwargs: Dictionary with the keyworded arguments for the insert\
      function. At least, kwargs contains the key table, with the table in which 
      args are going to be inserted.
    :type kwargs: dict
    :returns: Dictionary with key Value if execution successful, otherwise key\
      Message with logs.
    :rtype: S_OK || S_ERROR
    '''
    return self.gate.insert( args, kwargs )

  def update( self, *args, **kwargs ):
    '''
    This method calls the update function in :class:`ResourceManagementDB`, either
    directly or remotely through the RPC Server :class:`ResourceManagementHandler`. 
    It does not add neither processing nor validation. If you need to know more 
    about this method, you must keep reading on the database documentation.
      
    :Parameters:
      **\*args** - `[,tuple]`
        arguments for the mysql query ( must match table columns ! ).
    
      **\*\*kwargs** - `[,dict]`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''
    return self.gate.update( args, kwargs )

  def get( self, *args, **kwargs ):
    '''
    This method calls the get function in :class:`ResourceManagementDB`, either
    directly or remotely through the RPC Server :class:`ResourceManagementHandler`. 
    It does not add neither processing nor validation. If you need to know more 
    about this method, you must keep reading on the database documentation.
      
    :Parameters:
      **\*args** - `[,tuple]`
        arguments for the mysql query ( must match table columns ! ).
    
      **\*\*kwargs** - `[,dict]`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''  
    return self.gate.get( args, kwargs )

  def delete( self, *args, **kwargs ):
    '''
    This method calls the delete function in :class:`ResourceManagementDB`, either
    directly or remotely through the RPC Server :class:`ResourceManagementHandler`. 
    It does not add neither processing nor validation. If you need to know more 
    about this method, you must keep reading on the database documentation.
      
    :Parameters:
      **\*args** - `[,tuple]`
        arguments for the mysql query ( must match table columns ! ).
    
      **\*\*kwargs** - `[,dict]`
        metadata for the mysql query. It must contain, at least, `table` key
        with the proper table name.

    :return: S_OK() || S_ERROR()
    '''    
    return self.gate.delete( args, kwargs )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF

#################################################################################
## DB ###########################################################################
#
#  '''
#  ##############################################################################
#  # ENVIRONMENT CACHE FUNCTIONS
#  ##############################################################################
#  '''
#  @ClientDec3
#  def insertEnvironmentCache( self, hashEnv, siteName, environment ):
#    pass
#  @ClientDec3
#  def updateEnvironmentCache( self, hashEnv, siteName, environment ):
#    pass
#  @ClientDec3
#  def getEnvironmentCache( self, hashEnv = None, siteName = None, 
#                           environment = None, **kwargs ):
#    pass
#  @ClientDec3
#  def deleteEnvironmentCache( self, hashEnv = None, siteName = None, 
#                              environment = None, **kwargs ):
#    pass
#
## DB ###########################################################################
## DB ###########################################################################
#  
#  '''
#  ##############################################################################
#  # POLICY RESULT FUNCTIONS
#  ##############################################################################
#  '''
#  @ClientDec3
#  def insertPolicyResult( self, granularity, name, policyName, statusType,
#                               status, reason, dateEffective, lastCheckTime ):
#    pass 
#  @ClientDec3
#  def updatePolicyResult( self, granularity, name, policyName, statusType,
#                               status, reason, dateEffective, lastCheckTime ):
#    pass
#  @ClientDec3
#  def getPolicyResult( self, granularity = None, name = None, policyName = None, 
#                       statusType = None, status = None, reason = None, 
#                       dateEffective = None, lastCheckTime = None, **kwargs ):
#    pass
#  @ClientDec3
#  def deletePolicyResult( self, granularity = None, name = None, policyName = None, 
#                          statusType = None, status = None, reason = None, 
#                          dateEffective = None, lastCheckTime = None, **kwargs ):
#    pass
#
## DB ###########################################################################
## DB ###########################################################################
#
#  '''
#  ##############################################################################
#  # CLIENT CACHE FUNCTIONS
#  ##############################################################################
#  '''    
#  @ClientDec3
#  def insertClientCache( self, name, commandName, opt_ID, value, result,
#                         dateEffective, lastCheckTime ):
#    pass
#  @ClientDec3
#  def updateClientCache( self, name, commandName, opt_ID, value, result,
#                         dateEffective, lastCheckTime ):
#    pass
#  @ClientDec3
#  def getClientCache( self, name = None, commandName = None, opt_ID = None, 
#                      value = None, result = None, dateEffective = None, 
#                      lastCheckTime = None, **kwargs ):
#    pass
#  @ClientDec3 
#  def deleteClientCache( self, name = None, commandName = None, opt_ID = None, 
#                         value = None, result = None, dateEffective = None, 
#                         lastCheckTime = None, **kwargs ):
#    pass
#
## DB ###########################################################################
## DB ###########################################################################
#
#  '''
#  ##############################################################################
#  # ACCOUNTING CACHE FUNCTIONS
#  ##############################################################################
#  '''
#  @ClientDec3
#  def insertAccountingCache( self, name, plotType, plotName, result, 
#                             dateEffective, lastCheckTime ):
#    pass
#  @ClientDec3
#  def updateAccountingCache( self, name, plotType, plotName, result, 
#                             dateEffective, lastCheckTime ):
#    pass
#  @ClientDec3
#  def getAccountingCache( self, name = None, plotType = None, plotName = None, 
#                          result = None, dateEffective = None, 
#                          lastCheckTime = None, **kwargs ):
#    pass
#  @ClientDec3
#  def deleteAccountingCache( self, name = None, plotType = None, plotName = None, 
#                             result = None, dateEffective = None, 
#                             lastCheckTime = None, **kwargs ):
#    pass
#
## DB ###########################################################################
## DB ###########################################################################
#  
#  '''
#  ##############################################################################
#  # USER REGISTRY FUNCTIONS
#  ##############################################################################
#  '''  
#  
#  @ClientDec3
#  def insertUserRegistryCache( self, login, name, email ):
#    pass
#
#  @ClientDec3
#  def updateUserRegistryCache( self, login, name, email ):
#    pass
#  
#  @ClientDec3
#  def getUserRegistryCache( self, login = None, name = None, email = None, 
#                            **kwargs ):
#    pass
#  
#  @ClientDec3 
#  def deleteUserRegistryCache( self, login = None, name = None, email = None, 
#                               **kwargs ):                                            
#    pass
#
## DB ###########################################################################
## BOOSTER ######################################################################
#
#  '''
#  ##############################################################################
#  # DB specific Boosters
#  ##############################################################################
#  '''
#  @ClientDec3
#  def addOrModifyEnvironmentCache( self, hashEnv, siteName, environment ):
#    pass
#  @ClientDec3
#  def addOrModifyPolicyResult( self, granularity, name, policyName, statusType,
#                               status, reason, dateEffective, lastCheckTime ):
#    pass
#  @ClientDec3
#  def addOrModifyClientCache( self, name, commandName, opt_ID, value, result,
#                              dateEffective, lastCheckTime ):
#    pass
#  @ClientDec3
#  def addOrModifyAccountingCache( self, name, plotType, plotName, result, 
#                                  dateEffective, lastCheckTime ):
#    pass
#  @ClientDec3
#  def addOrModifyUserRegistryCache( self, login, name, email ):
#    pass  
#
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF