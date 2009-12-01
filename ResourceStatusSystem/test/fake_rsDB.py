""" fake ResourceStatusDB class. 
    Every function can simply return S_OK() (or nothing)
"""

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC import S_OK


#############################################################################

class RSSDBException(RSSException):
  pass

#############################################################################

class NotAllowedDate(RSSException):
  pass

#############################################################################

class ResourceStatusDB:
  
  def __init__(self, *args, **kwargs):
    pass

  ######
  #Site#
  ######
  
  def setSiteStatus(self, siteName, status, reason, operatorCode):
    pass    

  def addOrModifySite(self, siteName, siteType, status, reason, dateEffective, operatorCode, dateEnd):
    pass
  
  def addSiteType(self, siteType, description=''):
    pass

  def removeSite(self, site):
    pass

  def removeSiteType(self, siteType):
    pass

  def getSitesList(self, paramsList, siteName=None):
    return []

  def getSitesStatusWeb(self, selectDict, sortList, startItem, maxItems):
    return []

  def getSitesHistory(self, siteName=None):
    return []
  
  def getSiteTypeList(self):
    return []

  def getSitesToCheck(self, a, b, c):
    return []
    

  ##########
  #Resource#
  ##########
  
  def getResourcesList(self, paramsList, resName=None):
    return []

  def getResourcesStatusWeb(self, selectDict, sortList, startItem, maxItems):
    return []

  def setResourceStatus(self, resourceName, status, reason, operatorCode):
    pass    

  def addOrModifyResource(self, resourceName, resourceType, serviceName, siteName, status, reason, dateEffective, operatorCode, dateEnd):
    pass
  
  def addResourceType(self, resourceType, description=''):
    pass
  
  def removeResourceType(self, resourceType):
    pass

  def removeResource(self, site):
    pass
  
  def getResourceTypeList(self):
    return []

  def getResourcesHistory(self, resourceName=None):
    return []

  def getResourcesToCheck(self, a, b, c):
    return []
    
  def getResourceStats(self, gran, name):
    return []

  #########
  #Service#
  #########
  
  def setServiceStatus(self, serviceName, status, reason, operatorCode):
    pass    

  def addOrModifyService(self, serviceName, serviceType, description, status, reason, dateEffective, operatorCode, dateEnd):
    pass
  
  def addServiceType(self, serviceType, description=''):
    pass

  def removeService(self, service):
    pass

  def removeServiceType(self, serviceType):
    pass

  def getServicesList(self, paramsList, serviceName=None):
    return []

  def getServicesStatusWeb(self, selectDict, sortList, startItem, maxItems):
    return []

  def getServicesHistory(self, serviceName=None):
    return []
  
  def getServiceTypeList(self):
    return []

  def getServicesToCheck(self, a, b, c):
    return []
    
  def getServiceStats(self, name):
    return []


  
  #######
  #Mixed#
  #######
  
  def getStatusList(self):
    return []

  def getEndings(self, table):
    return []

  def getTablesWithHistory(self):
    a = ['Sites']
    return a
  
  def transact2History(self, table, row):
    pass
  
  def getPeriods(self, granularity, name, status, hours):
    return []

  def getGeneralName(self, name, from_g, to_g):
    return 'LCG.Bari.it'

  def getServiceStats(self, site):
    return {}

  def addStatus(self, status, description=''):
    pass

  def removeStatus(self, status):
    pass
