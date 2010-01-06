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

  def getMonitoredsList(self, granularity, paramsList = None, siteName = None, serviceName = None, resourceName = None, storageElementName = None, status = None, siteType = None, resourceType = None, serviceType = None):
    return []
  
  def getMonitoredsStatusWeb(self, granularity, selectDict, sortList, startItem, maxItems):
    return []
  
  def getMonitoredsHistory(self, granularity, paramsList = None, name = None):
    return []
  
  def setLastMonitoredCheckTime(self, granularity, name):
    pass
  
  def setMonitoredReason(self, granularity, name, reason, operatorCode):
    pass
  
  def setSiteStatus(self, siteName, status, reason, operatorCode):
    pass
  
  def addOrModifySite(self, siteName, siteType, status, reason, dateEffective, operatorCode, dateEnd):
    pass
  
  def _addSiteRow(self, siteName, siteType, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    pass
  
  def _addSiteHistoryRow(self, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    pass
  
  def removeSite(self, siteName):
    pass
  
  def setResourceStatus(self, resourceName, status, reason, operatorCode):
    pass
  
  def addOrModifyResource(self, resourceName, resourceType, serviceName, siteName, status, reason, dateEffective, operatorCode, dateEnd):
    pass
  
  def _addResourcesRow(self, resourceName, resourceType, serviceName, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    pass
  
  def _addResourcesHistoryRow(self, resourceName, serviceName, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    pass
  
  def addResourceType(self, resourceType, description=''):
    pass
  
  def removeResource(self, resourceName = None, serviceName = None, siteName = None):
    pass
  
  def setServiceStatus(self, serviceName, status, reason, operatorCode):
    pass
  
  def addOrModifyService(self, serviceName, serviceType, siteName, status, reason, dateEffective, operatorCode, dateEnd):
    pass
  
  def _addServiceRow(self, serviceName, serviceType, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    pass
  
  def _addServiceHistoryRow(self, serviceName, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    pass

  def removeService(self, serviceName = None, siteName = None):
    pass

  def setServiceToBeChecked(self, granularity, name):
    pass

  def getResourceStats(self, granularity, name):
    return []
    
  def setStorageElementStatus(self, storageElementName, status, reason, operatorCode):
    pass

  def addOrModifyStorageElement(self, storageElementName, resourceName, siteName, status, reason, dateEffective, operatorCode, dateEnd):
    pass

  def _addStorageElementRow(self, storageElementName, resourceName, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    pass

  def _addStorageElementHistoryRow(self, storageElementName, resourceName, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    pass

  def removeStorageElement(self, storageElementName = None, resourceName = None, siteName = None):
    pass

  def removeRow(self, granularity, name, dateEffective):
    pass

  def getTypesList(self, granularity, type=None):
    return []
    
  def removeType(self, granularity, type):
    pass

  def getStatusList(self):
    return []
    
  def getGeneralName(self, name, from_g, to_g):
    return 'LCG.Bari.it'

  def getEndings(self, table):
    return []
    
  def getPeriods(self, granularity, name, status, hours = None, days = None):
    return []
    
  def getTablesWithHistory(self):
    a = ['Sites']
    return a

  def getServiceStats(self, siteName):
    return {}

  def transact2History(self, *args):
    pass

  def setDateEnd(self, granularity, name, dateEffective):
    pass

  def addStatus(self, status, description=''):
    pass

  def removeStatus(self, status):
    pass

  def unique(self, table, ID):
    pass

  def syncWithCS(self, a, b):
    pass

  def getStuffToCheck(self, granularity, checkFrequency = None, maxN = None):
    return []
    
  def rankRes(self, granularity, days, startingDate = None):
    pass

  def __convertTime(self, t):
    pass


  
  ######
  #Site#
  ######
  
#  def setSiteStatus(self, siteName, status, reason, operatorCode):
#    pass    
#
#  def addOrModifySite(self, siteName, siteType, status, reason, dateEffective, operatorCode, dateEnd):
#    pass
#  
#  def addSiteType(self, siteType, description=''):
#    pass
#
#  def removeSite(self, site):
#    pass
#
#  def removeSiteType(self, siteType):
#    pass
#
#  def getSitesList(self, paramsList, siteName=None):
#    return []
#
#  def getSitesStatusWeb(self, selectDict, sortList, startItem, maxItems):
#    return []
#
#  def getSitesHistory(self, siteName=None):
#    return []
#  
#  def getSiteTypeList(self):
#    return []
#    
#
#  #########
#  Resource#
#  #########
#  
#  def getResourcesList(self, paramsList, resName=None):
#    return []
#
#  def getResourcesStatusWeb(self, selectDict, sortList, startItem, maxItems):
#    return []
#
#  def setResourceStatus(self, resourceName, status, reason, operatorCode):
#    pass    
#
#  def addOrModifyResource(self, resourceName, resourceType, serviceName, siteName, status, reason, dateEffective, operatorCode, dateEnd):
#    pass
#  
#  def addResourceType(self, resourceType, description=''):
#    pass
#  
#  def removeResourceType(self, resourceType):
#    pass
#
#  def removeResource(self, resourceName = None, serviceName = None, siteName = None):
#    pass
#  
#  def getResourceTypeList(self):
#    return []
#
#  def getResourcesHistory(self, resourceName=None):
#    return []
#
#  def getResourceStats(self, gran, name):
#    return []
#
#  ########
#  Service#
#  ########
#  
#  def setServiceStatus(self, serviceName, status, reason, operatorCode):
#    pass    
#
#  def addOrModifyService(self, serviceName, serviceType, description, status, reason, dateEffective, operatorCode, dateEnd):
#    pass
#  
#  def addServiceType(self, serviceType, description=''):
#    pass
#
#  def removeService(self, service):
#    pass
#
#  def removeServiceType(self, serviceType):
#    pass
#
#  def getServicesList(self, paramsList, serviceName=None):
#    return []
#
#  def getServicesStatusWeb(self, selectDict, sortList, startItem, maxItems):
#    return []
#
#  def getServicesHistory(self, serviceName=None):
#    return []
#  
#  def getServiceTypeList(self):
#    return []
#    
#  def getServiceStats(self, name):
#    return []
#
#
#  
#  ######
#  Mixed#
#  ######
#  
#  def getStatusList(self):
#    return []
#
#  def getEndings(self, table):
#    return []
#
#  def getTablesWithHistory(self):
#    a = ['Sites']
#    return a
#  
#  def transact2History(self, table, row):
#    pass
#  
#  def getPeriods(self, granularity, name, status, hours):
#    return []
#
#  def getGeneralName(self, name, from_g, to_g):
#    return 'LCG.Bari.it'
#
#  def getServiceStats(self, site):
#    return {}
#
#  def addStatus(self, status, description=''):
#    pass
#
#  def removeStatus(self, status):
#    pass
#
#  def getStuffToCheck(self, a, b = None, maxN = None):
#    return []
#    
#  def syncWithCS(self, a, b):
#    pass
