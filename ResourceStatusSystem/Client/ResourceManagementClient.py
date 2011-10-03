""" 
ResourceManagementClient class is a client for requesting info from the ResourceManagementService.
"""
# it crashes epydoc
# __docformat__ = "restructuredtext en"

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException
from DIRAC.ResourceStatusSystem.Utilities.Utils import where

from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB

class ResourceManagementClient:
  
#############################################################################

  def __init__(self, serviceIn = None, timeout = None ):
    """ Constructor of the ResourceManagementClient class
    """
    if serviceIn == None:
#      try:
#        self.rsM = ResourceManagementDB()
#      except:
      self.rsM = RPCClient( "ResourceStatus/ResourceManagement", timeout = timeout )
    else:
      self.rsM = serviceIn

#############################################################################
  
  def getEnvironmentCache( self, hash, siteName ):
    
    res = self.rsM.getEnvironmentCache( hash, siteName )
    if not res['OK']:
      raise RSSException, where( self, self.getEnvironmentCache) + " " + res['Message']
#    
#    return res['Value'] 
    return res

#############################################################################
  
  def addOrModifyEnvironmentCache( self, hash, siteName, environment ):
    
    res = self.rsM.addOrModifyEnvironmentCache( hash, siteName, environment )
    if not res['OK']:
      raise RSSException, where( self, self.addOrModifyEnvironmentCache) + " " + res['Message']

    return res 
   
#############################################################################

  def getCachedAccountingResult(self, name, plotType, plotName):
    """ 
    Returns a cached accounting plot
        
    :Parameters:
      `name`
        string, should be the name of the res
      
      `plotType`
        string, plot type
    
      `plotName`
        string, should be the plot name
      
    :returns:
      a plot
    """

    res = self.rsM.getCachedAccountingResult(name, plotType, plotName)
    if not res['OK']:
      raise RSSException, where(self, self.getCachedAccountingResult) + " " + res['Message'] 

    return res     
  
#############################################################################

  def getCachedResult(self, name, commandName, value, opt_ID = 'NULL'):
    """ 
    Returns a cached result;
        
    :Parameters:
      `name`
        string, name of site or resource
    
      `commandName`
        string
      
      `value`
        string
      
      `opt_ID`
        optional string
      
    :returns:
      (result, )
    """

    res = self.rsM.getCachedResult(name, commandName, value, opt_ID)
    if not res['OK']:
      raise RSSException, where(self, self.getCachedResult) + " " + res['Message'] 

    return res

#############################################################################

  def getCachedIDs(self, name, commandName):
    """ 
    Returns a cached result;
        
    :Parameters:
      `name`
        string, name of site or resource
    
      `commandName`
        string
      
    :returns: (e.g.)
      [78805473L, 78805473L, 78805473L, 78805473L]
    """

    res = self.rsM.getCachedIDs(name, commandName)
    if not res['OK']:
      raise RSSException, where(self, self.getCachedIDs) + " " + res['Message'] 
  
#    ID_list = [x for x in res['Value']]
  
#    return ID_list
    return res   
  
#############################################################################
