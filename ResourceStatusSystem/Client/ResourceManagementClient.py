""" 
ResourceManagementClient class is a client for requesting info from the ResourceManagementService.
"""
# it crashes epydoc
# __docformat__ = "restructuredtext en"

from DIRAC.Core.DISET.RPCClient                                     import RPCClient
#from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException
#from DIRAC.ResourceStatusSystem.Utilities.Utils import where

from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB             import ResourceManagementDB
from DIRAC.ResourceStatusSystem.Utilities.Decorators                import ClientExecution
from DIRAC.ResourceStatusSystem.Utilities.ResourceManagementBooster import ResourceManagementBooster

class ResourceManagementClient:
  
################################################################################

  def __init__( self , serviceIn = None ):
    """ Constructor of the ResourceStatusClient class
    """
 
    if serviceIn == None:
      try:
        self.gate = ResourceManagementDB()
      except Exception, x:
        self.gate = RPCClient( "ResourceStatus/ResourceManagement" )
        
    else:
      self.gate = serviceIn
      
    self.booster = ResourceManagementBooster( self )  

################################################################################

################################################################################
# EnvironmentCache functions
################################################################################

################################################################################

  @ClientExecution
  def addOrModifyEnvironmentCache( self, hashEnv, siteName, environment ):
    pass
  
  @ClientExecution
  def getEnvironmentCache( self, hashEnv = None, siteName = None, environment = None, 
                           **kwargs ):
    pass
  
  @ClientExecution
  def deleteEnvironmentCache( self, hashEnv = None, siteName = None, environment = None, 
                              **kwargs ):
    pass

################################################################################

################################################################################
# PolicyResult functions
################################################################################

################################################################################
  
  @ClientExecution
  def addOrModifyPolicyResult( self, granularity, name, policyName, statusType,
                               status, reason, dateEffective, lastCheckTime ):
    pass
  
  @ClientExecution
  def getPolicyResult( self, granularity = None, name = None, policyName = None, 
                       statusType = None, status = None, reason = None, 
                       dateEffective = None, lastCheckTime = None, **kwargs ):
    pass
  
  @ClientExecution
  def deletePolicyResult( self, granularity = None, name = None, policyName = None, 
                          statusType = None, status = None, reason = None, 
                          dateEffective = None, lastCheckTime = None, **kwargs ):
    pass
  
################################################################################

################################################################################
# PolicyResult functions
################################################################################

################################################################################  
  
  @ClientExecution
  def addOrModifyClientCache( self, name, commandName, opt_ID, value, result,
                              dateEffective, lastCheckTime ):
    pass
  
  @ClientExecution
  def getClientCache( self, name = None, commandName = None, opt_ID = None, value = None, 
                      result = None, dateEffective = None, lastCheckTime = None, **kwargs ):
    pass
   
  @ClientExecution 
  def deleteClientCache( self, name = None, commandName = None, opt_ID = None, 
                         value = None, result = None, dateEffective = None, 
                         lastCheckTime = None, **kwargs ):
    pass

################################################################################

################################################################################
# PolicyResult functions
################################################################################

################################################################################
  
  @ClientExecution
  def addOrModifyAccountingCache( self, name, plotType, plotName, result, dateEffective,
                                  lastCheckTime ):
    pass
  
  @ClientExecution
  def getAccountingCache( self, name = None, plotType = None, plotName = None, 
                          result = None, dateEffective = None, lastCheckTime = None, 
                          **kwargs ):
    pass
  
  @ClientExecution
  def deleteAccountingCache( self, name = None, plotType = None, plotName = None, 
                             result = None, dateEffective = None, lastCheckTime = None, 
                             **kwargs ):
    pass

################################################################################

################################################################################
# PolicyResult functions
################################################################################

################################################################################
  
  @ClientExecution
  def addOrModifyUserRegistryCache( self, login, name, email ):
    pass
  
  @ClientExecution
  def getUserRegistryCache( self, login = None, name = None, email = None, 
                            **kwargs ):
    pass
  
  @ClientExecution 
  def deleteUserRegistryCache( self, login = None, name = None, email = None, 
                               **kwargs ):                                            
    pass

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF

##############################################################################
#  
#  def getEnvironmentCache( self, hash, siteName ):
#    
#    res = self.rsM.getEnvironmentCache( hash, siteName )
#    if not res['OK']:
#      raise RSSException, where( self, self.getEnvironmentCache) + " " + res['Message']
##    
##    return res['Value'] 
#    return res
#
##############################################################################
#  
#  def addOrModifyEnvironmentCache( self, hash, siteName, environment ):
#    
#    res = self.rsM.addOrModifyEnvironmentCache( hash, siteName, environment )
#    if not res['OK']:
#      raise RSSException, where( self, self.addOrModifyEnvironmentCache) + " " + res['Message']
#
#    return res 
#   
##############################################################################
#
#  def getCachedAccountingResult(self, name, plotType, plotName):
#    """ 
#    Returns a cached accounting plot
#        
#    :Parameters:
#      `name`
#        string, should be the name of the res
#      
#      `plotType`
#        string, plot type
#    
#      `plotName`
#        string, should be the plot name
#      
#    :returns:
#      a plot
#    """
#
#    res = self.rsM.getCachedAccountingResult(name, plotType, plotName)
#    if not res['OK']:
#      raise RSSException, where(self, self.getCachedAccountingResult) + " " + res['Message'] 
#
#    return res     
#  
##############################################################################
#
#  def getCachedResult(self, name, commandName, value, opt_ID = 'NULL'):
#    """ 
#    Returns a cached result;
#        
#    :Parameters:
#      `name`
#        string, name of site or resource
#    
#      `commandName`
#        string
#      
#      `value`
#        string
#      
#      `opt_ID`
#        optional string
#      
#    :returns:
#      (result, )
#    """
#
#    res = self.rsM.getCachedResult(name, commandName, value, opt_ID)
#    if not res['OK']:
#      raise RSSException, where(self, self.getCachedResult) + " " + res['Message'] 
#
#    return res
#
##############################################################################
#
#  def getCachedIDs(self, name, commandName):
#    """ 
#    Returns a cached result;
#        
#    :Parameters:
#      `name`
#        string, name of site or resource
#    
#      `commandName`
#        string
#      
#    :returns: (e.g.)
#      [78805473L, 78805473L, 78805473L, 78805473L]
#    """
#
#    res = self.rsM.getCachedIDs(name, commandName)
#    if not res['OK']:
#      raise RSSException, where(self, self.getCachedIDs) + " " + res['Message'] 
#  
##    ID_list = [x for x in res['Value']]
#  
##    return ID_list
#    return res   
#  
##############################################################################
#