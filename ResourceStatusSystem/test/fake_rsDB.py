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

  def getMonitoredsList(self, granularity, paramsList = None, siteName = None, 
                        serviceName = None, resourceName = None, storageElementName = None,
                        status = None, siteType = None, resourceType = None, serviceType = None):
    return [('NAME', 'Banned'), ('NAME', 'Active')]
  
  def getMonitoredsStatusWeb(self, granularity, selectDict, sortList, startItem, maxItems):
    if granularity in ('Resource', 'Resources'):
      return {'TotalRecords': 1, 
              'ParameterNames': ['ResourceName', 'Status', 'SiteName', 'ResourceType', 'Country', 
                                 'DateEffective', 'FormerStatus', 'Reason'], 
              'Extras': None, 
              'Records': [['grid0.fe.infn.it', 'Active', 'LCG.Ferrara.it', 'CE', 'it', 
                           '2009-12-15 12:47:31', 'Banned', 'DT:None|PilotsEff:Good']]}
    else:
      return {'TotalRecords': 1, 
              'ParameterNames': ['SiteName', 'Tier', 'GridType', 'Country', 'Status', 
                                 'DateEffective', 'FormerStatus', 'Reason'], 
              'Extras': None, 
              'Records': [['LCG.Ferrara.it', 'T2', 'LCG', 'it', 'Active', 
                           '2009-12-15 12:47:31', 'Banned', 'DT:None|PilotsEff:Good']]}
  
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
  
  def addOrModifyResource(self, resourceName, resourceType, serviceName, siteName, status, 
                          reason, dateEffective, operatorCode, dateEnd):
    pass
  
  def _addResourcesRow(self, resourceName, resourceType, serviceName, siteName, status, 
                       reason, dateCreated, dateEffective, dateEnd, operatorCode):
    pass
  
  def _addResourcesHistoryRow(self, resourceName, serviceName, siteName, status, reason, 
                              dateCreated, dateEffective, dateEnd, operatorCode):
    pass
  
  def addType(self, granularity, type, description=''):
    pass
  
  def removeResource(self, resourceName = None, serviceName = None, siteName = None):
    pass
  
  def setServiceStatus(self, serviceName, status, reason, operatorCode):
    pass
  
  def addOrModifyService(self, serviceName, serviceType, siteName, status, reason, 
                         dateEffective, operatorCode, dateEnd):
    pass
  
  def _addServiceRow(self, serviceName, serviceType, siteName, status, reason, 
                     dateCreated, dateEffective, dateEnd, operatorCode):
    pass
  
  def _addServiceHistoryRow(self, serviceName, siteName, status, reason, dateCreated, 
                            dateEffective, dateEnd, operatorCode):
    pass

  def removeService(self, serviceName = None, siteName = None):
    pass

  def setMonitoredToBeChecked(self, monitored, granularity, name):
    pass

  def getResourceStats(self, granularity, name):
    return []
    
  def setStorageElementStatus(self, storageElementName, status, reason, operatorCode):
    pass

  def addOrModifyStorageElement(self, storageElementName, resourceName, siteName, 
                                status, reason, dateEffective, operatorCode, dateEnd):
    pass

  def _addStorageElementRow(self, storageElementName, resourceName, siteName, status, 
                            reason, dateCreated, dateEffective, dateEnd, operatorCode):
    pass

  def _addStorageElementHistoryRow(self, storageElementName, resourceName, siteName,
                                    status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
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
    return 'LCG.PIC.es'

  def getEndings(self, table):
    return []
    
  def getPeriods(self, granularity, name, status, hours = None, days = None):
    return []
    
  def getTablesWithHistory(self):
    a = ['Sites']
    return a

  def getServiceStats(self, siteName):
    return {}

  def getStorageElementsStats(self, granularity, name):
    return {}

  def addOrModifyPolicyRes(self, granularity, name, policyName, 
                           status, reason, dateEffective = None):
    pass
  
  def getPolicyRes(self, name, policyName, lastCheckTime = False):
    return ('Active', 'DT:None')
  
  def addOrModifyClientsCacheRes(self, name, commandName, result, dateEffective = None):
    pass
  
  def getClientsCacheRes(self, name, commandName, lastCheckTime = False):
    return ('Bad', )
  
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

  def getStuffToCheck(self, granularity, checkFrequency = None, maxN = None, name = None):
    return []
    
  def rankRes(self, granularity, days, startingDate = None):
    pass

  def __convertTime(self, t):
    pass
