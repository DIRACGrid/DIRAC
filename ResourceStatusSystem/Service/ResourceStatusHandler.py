""" ``ResourceStatusHandler`` exposes the service of the Resource Status System. 
    It uses :mod:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB` for database persistence. 
    
    To use this service
      
    >>> from DIRAC.Core.DISET.RPCClient import RPCCLient
    >>> server = RPCCLient("ResourceStatus/ResourceStatus")

"""


from datetime import datetime, timedelta

from types import *
from DIRAC import gLogger
from DIRAC import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

rsDB = False

def initializeResourceStatusHandler(serviceInfo):
  global rsDB
  rsDB = ResourceStatusDB()
  return S_OK()

class ResourceStatusHandler(RequestHandler):

  def initialize(self):
    pass

#############################################################################

  types_setSiteStatus = [StringType, StringType, StringType, StringType]
  def export_setSiteStatus(self, siteName, status, reason, operatorCode):
    """ 
    set Site status to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.setSiteStatus`

    :params:
      :attr:`siteName`: a string representing the site name
      
      :attr:`status`: a string representing the status

      :attr:`reason`: a string representing the reason

      :attr:`operatorCode`: a string representing the operator Code
      (can be a user name, or ``RS_SVC`` for the service itself
    """
    try:
      gLogger.info("ResourceStatusHandler.setSiteStatus: Attempting to modify site %s status" % siteName)
      try:
        rsDB.setSiteStatus(siteName, status, reason, operatorCode)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.setSiteStatus: Set site %s status." % (siteName))
      return S_OK()
    except Exception, x:
      errorStr = where(self, self.export_setSiteStatus)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

  types_addOrModifySite = [StringType, StringType, StringType, StringType, StringType, Time._dateTimeType, StringType, Time._dateTimeType]
  def export_addOrModifySite(self, siteName, siteType, description, status, reason, dateEffective, operatorCode, dateEnd):
    """ add or modify a Site of the ResourceStatusDB
    """
    try:
      gLogger.info("ResourceStatusHandler.addOrModifySite: Attempting to add or modify site %s" % siteName)
      try:
        rsDB.addOrModifySite(siteName, siteType, description, status, reason, dateEffective, operatorCode, dateEnd)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.addOrModifySite: Added (or modified) site %s." % (siteName))
      return S_OK()
    except Exception, x:
      errorStr = where(self, self.export_addOrModifySite)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

  types_removeSite = [StringType]
  def export_removeSite(self, siteName):
    """ remove a Site from those monitored
    """
    try:
      gLogger.info("ResourceStatusHandler.removeSite: Attempting to remove modify site %s" % siteName)
      try:
        rsDB.removeSite(siteName)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.removeSite: removed site %s." % (siteName))
      return S_OK()
    except Exception, x:
      errorStr = where(self, self.export_removeSite)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

  types_setResourceStatus = [StringType, StringType, StringType, StringType]
  def export_setResourceStatus(self, resourceName, status, reason, operatorCode):
    """ set Resource status to the ResourceStatusDB
    """
    try:
      gLogger.info("ResourceStatusHandler.setResourceStatus: Attempting to modify resource %s status" % resourceName)
      try:
        rsDB.setResourceStatus(resourceName, status, reason, operatorCode)
        return S_OK()
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.setResourceStatus: Set resource %s status." % (resourceName))
      return S_OK()
    except Exception, x:
      errorStr = where(self, self.export_setResourceStatus)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

  types_addOrModifyResource = [StringType, StringType, StringType, StringType, StringType, Time._dateTimeType, StringType, Time._dateTimeType]
  def export_addOrModifyResource(self, resourceName, resourceType, siteName, status, reason, dateEffective, operatorCode, dateEnd=datetime(9999, 12, 31, 23, 59, 59)):
    """ add or modify a Resource of the ResourceStatusDB
    """
    try:
      gLogger.info("ResourceStatusHandler.addOrModifyResource: Attempting to add or modify resource %s %s" % (resourceName, siteName))
      try:
        rsDB.addOrModifyResource(resourceName, resourceType, siteName, status, reason, dateEffective, operatorCode, dateEnd)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.addOrModifyResource: Added (or modified) resource %s of site %s" % (resourceName, siteName))
      return S_OK()
    except Exception, x:
      errorStr = where(self, self.export_addOrModifyResource)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

  types_addSiteType = [StringType, StringType]
  def export_addSiteType(self, siteType, description=''):
    """ 
    Add a site type to the ResourceStatusDB
    
    :Params:
      :attr:`siteType`: a string
      :attr:`description`: an optional string
    """
    try:
      gLogger.info("ResourceStatusHandler.addSiteType: Attempting to add site type %s" % (siteType))
      try:
        rsDB.addSiteType(siteType, description)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.addSiteType: Added site type %s" % (siteType))
      return S_OK()
    except Exception, x:
      errorStr = where(self, self.export_addSiteType)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

  types_getSiteTypeList = []
  def export_getSiteTypeList(self):
    """
    Get site type list from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getSiteTypeList`
    """
    try:
      gLogger.info("ResourceStatusHandler.getSiteTypeList: Attempting to get SiteType list")
      try:
        res = rsDB.getSiteTypeList()
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getSiteTypeList: got SiteType list")
      return S_OK(res)
    except Exception, x:
      errorStr = where(self, self.export_getSiteTypeList)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

  types_removeResource = [StringType]
  def export_removeResource(self, resourceName):
    """ remove a Resource from those monitored
    """
    try:
      gLogger.info("ResourceStatusHandler.Resource: Attempting to remove modify Resource %s" % resourceName)
      try:
        rsDB.removeSite(resourceName)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.Resource: removed Resource %s." % (resourceName))
      return S_OK()
    except Exception, x:
      errorStr = where(self, self.export_removeResource)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

  types_removeSiteType = [StringType]
  def export_removeSiteType(self, siteType):
    """ remove a site type to the ResourceStatusDB
    """
    try:
      gLogger.info("ResourceStatusHandler.removeSiteType: Attempting to remove site type %s" % (siteType))
      try:
        rsDB.removeSiteType(siteType)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.removeSiteType: Removed site type %s" % (siteType))
      return S_OK()
    except Exception, x:
      errorStr = where(self, self.export_removeSiteType)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################


  types_addResourceType = [StringType, StringType]
  def export_addResourceType(self, resourceType, description=''):
    """
    Add a resource type to the ResourceStatusDB. 
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.addResourceType`
    
    :Params:
      :attr:`resourceType`: a string
      :attr:`description`: an optional long string
    """
    try:
      gLogger.info("ResourceStatusHandler.addResourceType: Attempting to add resource type %s" % (resourceType))
      try:
        rsDB.addResourceType(resourceType, description)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.addResourceType: Added resource type %s" % (resourceType))
      return S_OK()
    except Exception, x:
      errorStr = where(self, self.export_addResourceType)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

  types_getResourceTypeList = []
  def export_getResourceTypeList(self):
    """
    Get resource type list from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getResourceTypeList`
    """
    try:
      gLogger.info("ResourceStatusHandler.getResourceTypeList: Attempting to get ResourceType list")
      try:
        res = rsDB.getResourceTypeList()
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getResourceTypeList: got ResourceType list")
      return S_OK(res)
    except Exception, x:
      errorStr = where(self, self.export_getResourceTypeList)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

  types_removeResourceType = [StringType]
  def export_removeResourceType(self, resourceType):
    """ remove a resource type to the ResourceStatusDB
    """
    try:
      gLogger.info("ResourceStatusHandler.removeResourceType: Attempting to remove resource type %s" % (resourceType))
      try:
        rsDB.removeResourceType(resourceType)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.removeResourceType: Removed resource type %s" % (resourceType))
      return S_OK()
    except Exception, x:
      errorStr = where(self, self.export_removeResourceType)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################


  types_addStatus = [StringType, StringType]
  def export_addStatus(self, status, description=''):
    """ 
    Add a status to the ResourceStatusDB. 
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.addStatus`
    
    :Params:
      :attr:`status`: a string
      :attr:`description`: an optional long string
    """
    try:
      gLogger.info("ResourceStatusHandler.addStatus: Attempting to add status %s" % (status))
      try:
        rsDB.addStatus(status, description)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.addStatus: Added status %s" % (status))
      return S_OK()
    except Exception, x:
      errorStr = where(self, self.export_addStatus)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

  types_getStatusList = []
  def export_getStatusList(self):
    """
    Get status list from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getStatusList`
    """
    try:
      gLogger.info("ResourceStatusHandler.getStatusList: Attempting to get status list")
      try:
        res = rsDB.getStatusList()
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getStatusList: got status list")
      return S_OK(res)
    except Exception, x:
      errorStr = where(self, self.export_getStatusList)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

  types_removeStatus = [StringType]
  def export_removeStatus(self, status):
    """ remove a status from the ResourceStatusDB
    """
    try:
      gLogger.info("ResourceStatusHandler.removeStatus: Attempting to remove status %s" % (status))
      try:
        rsDB.removeStatus(status)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.removeStatus: Removed status %s" % (status))
      return S_OK()
    except Exception, x:
      errorStr = where(self, self.export_addStatus)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

  types_getSitesStatusWeb = [DictType, ListType, IntType, IntType]
  def export_getSitesStatusWeb(self, selectDict, sortList, startItem, maxItems):
    """ get present sites status list, for the web
    
        :Params:
          selectDict = 
            {
              'SiteName':'name of a site' --- present status
              'ExpandSiteHistory':'name of a site' --- site status history
            }
          
          sortList = [] (no sorting provided)
          
          startItem
          
          maxItems
    
        :return:
        {
          'OK': XX, 

          'rpcStub': XX, 'getSitesStatusWeb', ({}, [], X, X)), 

          Value': 
          {

            'ParameterNames': ['SiteName', 'Tier', 'GridType', 'Country', 'Status', 'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'], 

            'Records': [[], [], ...]

            'TotalRecords': X, 

            'Extras': {}, 
          }
        }
    """
    try:
      gLogger.info("ResourceStatusHandler.getSitesStatusWeb: Attempting to get sites list")
      try:
        res = rsDB.getSitesStatusWeb(selectDict, sortList, startItem, maxItems)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getSitesStatusWeb: got sites list")
      return S_OK(res)
    except Exception, x:
      errorStr = where(self, self.export_getSitesStatusWeb)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################


  types_getResourcesStatusWeb = [DictType, ListType, IntType, IntType]
  def export_getResourcesStatusWeb(self, selectDict, sortList, startItem, maxItems):
    """ get present resources status list

        Input:
        selectDict: {'ResourceName':'name of a resource' --- present status
        
        'ExpandResourceHistory':'name of a resource' --- resource status history }
        
        sortList = [] (no sorting provided)
        
        startItem
        
        maxItems
          
        Output: { 'OK': XX, 'rpcStub': XX, 'getSitesStatusWeb', ({}, [], X, X)), 
        
        Value': { 'ParameterNames': ['ResourceName', 'SiteName', 'ServiceExposed', 'Country', 'Status', 'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'], 
            
        'Records': [[], [], ...]
            
        'TotalRecords': X, 
           
        'Extras': {} } }
    """
    try:
      gLogger.info("ResourceStatusHandler.getResourcesStatusWeb: Attempting to get resources list")
      try:
        res = rsDB.getResourcesStatusWeb(selectDict, sortList, startItem, maxItems)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getResourcesStatusWeb: got resources list")
      return S_OK(res)
    except Exception, x:
      errorStr = where(self, self.export_getResourcesStatusWeb)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)


#############################################################################
  types_getSitesList = []
  def export_getSitesList(self):
    """
    Get sites list from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getSitesList`
    """
    try:
      gLogger.info("ResourceStatusHandler.getSitesList: Attempting to get sites list")
      try:
        r = rsDB.getSitesList(paramsList = ['SiteName'])
        res = []
        for x in r:
          res.append(x[0])
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getSitesList: got sites list")
      return S_OK(res)
    except Exception, x:
      errorStr = where(self, self.export_getSitesList)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

#  types_getSitesListByStatus = [StringType]
#  def export_getSitesListByStatus(self, status):
#    """ get present sites status list, by status
#    """
#    try:
#      gLogger.info("ResourceStatusHandler.getSitesListByStatus: Attempting to get sites list by status")
#      try:
#        res = rsDB.getSitesListByStatus(status)
#      except RSSDBException, x:
#        gLogger.error(whoRaised(x))
#      except RSSException, x:
#        gLogger.error(whoRaised(x))
#      gLogger.info("ResourceStatusHandler.getSitesListByStatus: got sites list")
#      return S_OK(res)
#    except Exception, x:
#      errorStr = where(self, self.export_getSitesListByStatus)
#      gLogger.exception(errorStr,lException=x)
#      return S_ERROR(errorStr)

#############################################################################

  types_getResourcesList = []
  def export_getResourcesList(self):
    """ 
    Get resources list from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getResourcesList`
    """
    try:
      gLogger.info("ResourceStatusHandler.getResourcesList: Attempting to get resources list")
      try:
        r = rsDB.getResourcesList(paramsList = ['ResourceName'])
        res = []
        for x in r:
          res.append(x[0])
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getResourcesList: got resources list")
      return S_OK(res)
    except Exception, x:
      errorStr = where(self, self.export_getResourcesList)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

#  types_getResourcesListByStatus = [StringType]
#  def export_getResourcesListByStatus(self, status):
#    """ get present resources list, by status
#    """
#    try:
#      gLogger.info("ResourceStatusHandler.getResourcesListByStatus: Attempting to get resources with status %s" % (status))
#      try:
#        res = rsDB.getResourcesListByStatus(status)
#      except RSSDBException, x:
#        gLogger.error(whoRaised(x))
#      except RSSException, x:
#        gLogger.error(whoRaised(x))
#      gLogger.info("ResourceStatusHandler.getResourcesListByStatus: got resources list at status %s" % (status))
#      return S_OK(res)
#    except Exception, x:
#      errorStr = where(self, self.export_getResourcesListByStatus)
#      gLogger.exception(errorStr,lException=x)
#      return S_ERROR(errorStr)


#############################################################################

  types_getSitesHistory = [StringType]
  def export_getSitesHistory(self, site):
    """ get sites history
    """
    try:
      gLogger.info("ResourceStatusHandler.getSitesHistory: Attempting to get site %s history" % (site))
      try:
        res = rsDB.getSitesHistory(site)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getSitesHistory: got site %s history" % (site))
      return S_OK(res)
    except Exception, x:
      errorStr = where(self, self.export_getSitesHistory)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

  types_getResourcesHistory = [StringType]
  def export_getResourcesHistory(self, resource):
    """ get resources history
    """
    try:
      gLogger.info("ResourceStatusHandler.getResourcesHistory: Attempting to get resource %s history" % (resource))
      try:
        res = rsDB.getResourcesHistory(resource)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getResourcesHistory: got resource %s history" % (resource))
      return S_OK(res)
    except Exception, x:
      errorStr = where(self, self.export_getResourcesHistory)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

  types_getPeriods = [StringType, StringType, StringType, IntType]
  def export_getPeriods(self, granularity, name, status, hours):
    """ get periods of time when name was in status (for a total of hours hours)
    """
    try:
      gLogger.info("ResourceStatusHandler.getPeriods: Attempting to get %s periods when it was in %s" % (name, status))
      try:
        res = rsDB.getPeriods(granularity, name, status, int(hours))
#        res = rsDB.getPeriods(granularity)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getPeriods: got %s periods" % (name))
      return S_OK(res)
    except Exception, x:
      errorStr = where(self, self.export_getPeriods)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

  types_getServiceStats = [StringType]
  def export_getServiceStats(self, siteName):
    """ returns simple statistics of active, probing and banned nodes of services;
            
        input:
          siteName : string - a site name
        
        output:
          {
            'OK':True|False
            'Value':
            {
              'Computing: {'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz} (optional)
              'Storage: {'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz} (optional)
            }
          }
    """

    try:
      gLogger.info("ResourceStatusHandler.getServiceStats: Attempting to get service stats for site %s" %(siteName))
      try:
        res = rsDB.getServiceStats(siteName)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getServiceStats: got service stats")
      return S_OK(res)
    except Exception, x:
      errorStr = where(self, self.export_getServiceStats)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################
