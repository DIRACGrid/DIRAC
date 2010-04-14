""" 
ResourceStatusClient class is a client for requesting info from the ResourceStatusService.
"""

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class ResourceStatusClient:
  
#############################################################################

  def __init__(self, serviceIn = None):
    """ Constructor of the ResourceStatusClient class
    """
    if serviceIn == None:
      self.rsS = RPCClient("ResourceStatus/ResourceStatus")
    else:
      self.rsS = serviceIn

#############################################################################

  def getServiceStats(self, granularity, name):
    """ 
    Returns simple statistics of active, probing and banned services of a site;
        
    :params:
      :attr:`granularity`: string, has to be 'Site'
    
      :attr:`name`: string - a service name
    
    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """

    if granularity not in ('Site', 'Sites'):
      raise InvalidRes, where(self, self.getServiceStats)

    res = self.rsS.getServiceStats(name)
    if not res['OK']:
      raise RSSException, where(self, self.getServiceStats) + " " + res['Message'] 

    return res['Value']

#############################################################################

  def getResourceStats(self, granularity, name):
    """ 
    Returns simple statistics of active, probing and banned resources of a site or a service;
        
    :params:
      :attr:`granularity` string, should be in ('Site', 'Service')
      
      :attr:`name`: string, name of site or service
    
    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """

    res = self.rsS.getResourceStats(granularity, name)
    if not res['OK']:
      raise RSSException, where(self, self.getResourceStats) + " " + res['Message'] 
  
    return res['Value']

#############################################################################

  def getStorageElementsStats(self, granularity, name):
    """ 
    Returns simple statistics of active, probing and banned storageElements of a site or a resource;
        
    :params:
      :attr:`granularity` string, should be in ['Site', 'Resource']
      
      :attr:`name`: string, name of site or resource
    
    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """

    res = self.rsS.getStorageElementsStats(granularity, name)
    if not res['OK']:
      raise RSSException, where(self, self.getStorageElementsStats) + " " + res['Message'] 

    return res['Value']
    
#############################################################################

  
  def getPeriods(self, granularity, name, status, hours):
    """ 
    Returns a list of periods of time where name was in status

    :returns:
      {
        'Periods':[list of periods]
      }
    """
    #da migliorare!
    
    if granularity not in ValidRes:
      raise InvalidRes, where(self, self.getPeriods)
    
    res = self.rsS.getPeriods(granularity, name, status, hours)
    if not res['OK']:
      raise RSSException, where(self, self.getPeriods) + " " + res['Message'] 
  
    return {'Periods':res['Value']}

#############################################################################

  def getGeneralName(self, granularity, name, toGranularity):
    """ 
    Returns simple statistics of active, probing and banned storageElements of a site or a resource;
        
    :params:
      :attr:`granularity` string, should be a ValidRes
      
      :attr:`name`: string, name of site or resource
    
      :attr:`toGranularity` string, should be a ValidRes
      
    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """

    res = self.rsS.getGeneralName(granularity, name, toGranularity)
    if not res['OK']:
      raise RSSException, where(self, self.getGeneralName) + " " + res['Message'] 
  
    return res['Value']
  
  
#############################################################################

  def getMonitoredStatus(self, granularity, name):
    """ 
    Returns RSS status of name
        
    :params:
      :attr:`granularity` string, should be a ValidRes
      
      :attr:`name`: string, name of the ValidRes
      
    :returns:
      'Active'|'Probing'|'Banned'|None
    """

    if granularity in ('Site', 'Sites'):
      res = self.rsS.getSitesStatusWeb({'SiteName':name}, [], 0, 1)
    elif granularity in ('Service', 'Services'):
      res = self.rsS.getServicesStatusWeb({'ServiceName':name}, [], 0, 1)
    elif granularity in ('Resource', 'Resources'):
      res = self.rsS.getResourcesStatusWeb({'ResourceName':name}, [], 0, 1)
    elif granularity in ('StorageElement', 'StorageElements'):
      res = self.rsS.getStorageElementsStatusWeb({'StorageElementName':name}, [], 0, 1)
    else:
      raise InvalidRes, where(self, self.getMonitoredStatus)
    
    if not res['OK']:
      raise RSSException, where(self, self.getMonitoredStatus) + " " + res['Message'] 
    else:
      try:
        if granularity in ('Resource', 'Resources'):
          return res['Value']['Records'][0][5]
        else:
          return res['Value']['Records'][0][4]
      except IndexError:
        return None
    

#############################################################################

#  
#  def addOrModifySite(self, siteName, siteType, description, status, reason, dateEffective, operatorCode, dateEnd):
#    try:
#      server = RPCClient('ResourceStatus/ResourceStatus', useCertificates=self.useCerts, timeout=120)
#      result = server.addOrModifySite(siteName, siteType, description, status, reason, dateEffective, operatorCode, dateEnd)
#      return result
#    except Exception, x:
#      errorStr = "ResourceStatusClient.addOrModifySite failed"
#      gLogger.exception(errorStr,lException=x)
#      return S_ERROR(errorStr+": "+str(x))
#
#  def addOrModifyResource(self, resourceName, resourceType, siteName, status, reason, dateEffective, operatorCode, dateEnd):
#    try:
#      server = RPCClient('ResourceStatus/ResourceStatus', useCertificates=self.useCerts, timeout=120)
#      result = server.addOrModifyResource(resourceName, resourceType, siteName, status, reason, dateEffective, operatorCode, dateEnd)
#      return result
#    except Exception, x:
#      errorStr = "ResourceStatusClient.addOrModifyResource failed"
#      gLogger.exception(errorStr,lException=x)
#      return S_ERROR(errorStr+": "+str(x))

