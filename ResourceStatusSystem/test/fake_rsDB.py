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

  def getEndings(self, table):
    return []

  def getTablesWithHistory(self):
    a = ['Sites']
    return a
  
  def getSitesToCheck(self, a, b, c):
    return []
    
  def getResourcesToCheck(self, a, b, c):
    return []
    
  def transact2History(self, table, row):
    pass
  
  def setSiteStatus(self, siteName, status, reason, operatorCode):
    pass    

  def setResourceStatus(self, resourceName, status, reason, operatorCode):
    pass    

  def addOrModifyResource(self, resourceName, resourceType, siteName, status, reason, dateEffective, operatorCode, dateEnd):
    pass
  
  def addOrModifySite(self, siteName, siteType, description, status, reason, dateEffective, operatorCode, dateEnd):
    pass
  
  def addSiteType(self, siteType, description=''):
    pass

  def addResourceType(self, resourceType, description=''):
    pass
  
  def addStatus(self, status, description=''):
    pass

  def removeSite(self, site):
    pass

  def removeResource(self, site):
    pass

  def removeSiteType(self, siteType):
    pass

  def removeResourceType(self, resourceType):
    pass

  def removeStatus(self, status):
    pass
  
  def getSitesList(self, paramsList, siteName=None):
    return []

  def getResourcesList(self, paramsList, resName=None):
    return []

  def getSitesStatusWeb(self, selectDict, sortList, startItem, maxItems):
    return []

  def getResourcesStatusWeb(self, selectDict, sortList, startItem, maxItems):
    return []

  def getSitesListByStatus(self, status):
    return []

  def getResourcesListByStatus(self, status):
    return []

  def getSitesHistory(self, siteName=None):
    return []
  
  def getSiteTypeList(self):
    return []

  def getResourceTypeList(self):
    return []

  def getStatusList(self):
    return []

  def getResourcesHistory(self, resName=None):
    return []

  def getPeriods(self, granularity, name, status, hours):
    return []

  def getGeneralName(self, name, from_g, to_g):
    return 'LCG.Bari.it'

  def getServiceStats(self, site):
    return {}
