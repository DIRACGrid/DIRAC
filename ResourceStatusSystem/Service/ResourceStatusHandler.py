""" ``ResourceStatusHandler`` exposes the service of the Resource Status System. 
    It uses :mod:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB` for database persistence. 
    
    To use this service
      
    >>> from DIRAC.Core.DISET.RPCClient import RPCCLient
    >>> server = RPCCLient("ResourceStatus/ResourceStatus")

"""
__docformat__ = "restructuredtext en"

import datetime

from types import *
from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger, gConfig

from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time

from DIRAC.ResourceStatusSystem.Utilities.CS import *

from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import *
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Utilities.Publisher import Publisher 
from DIRAC.ResourceStatusSystem.Command.CommandCaller import CommandCaller
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ResourceStatusSystem.Utilities.InfoGetter import InfoGetter
from DIRAC.ResourceStatusSystem.Utilities.Synchronizer import Synchronizer

rsDB = False

def initializeResourceStatusHandler(serviceInfo):
  
  global rsDB
  rsDB = ResourceStatusDB()

  cc = CommandCaller()

  global VOExtension
  VOExtension = getExt()

  ig = InfoGetter(VOExtension)
  
  WMSAdmin = RPCClient("WorkloadManagement/WMSAdministrator")

  global publisher
  publisher = Publisher(VOExtension, rsDBIn = rsDB, commandCallerIn = cc, 
                        infoGetterIn = ig, WMSAdminIn = WMSAdmin)

  sync_O = Synchronizer(rsDB)
  gConfig.addListenerToNewVersionEvent( sync_O.sync )
    
  return S_OK()

class ResourceStatusHandler(RequestHandler):

  def initialize(self):
    pass
    
#############################################################################

#############################################################################
# Sites functions
#############################################################################

#############################################################################

  types_getSitesList = []
  def export_getSitesList(self):
    """
    Get sites list from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
    """
    try:
      gLogger.info("ResourceStatusHandler.getSitesList: Attempting to get sites list")
      try:
        r = rsDB.getMonitoredsList('Site', paramsList = ['SiteName'])
        res = []
        for x in r:
          res.append(x[0])
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getSitesList: got sites list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getSitesList)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_getGridSitesList = []
  def export_getGridSitesList(self):
    """
    Get sites list from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getGridSitesList`
    """
    try:
      gLogger.info("ResourceStatusHandler.getGridSitesList: Attempting to get sites list")
      try:
        r = rsDB.getGridSitesList(paramsList = ['GridSiteName'])
        res = []
        for x in r:
          res.append(x[0])
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getGridSitesList: got sites list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getGridSitesList)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_getSitesStatusList = []
  def export_getSitesStatusList(self):
    """
    Get sites list from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
    """
    try:
      gLogger.info("ResourceStatusHandler.getSitesList: Attempting to get sites list")
      try:
        r = rsDB.getMonitoredsList('Site', paramsList = ['SiteName', 'Status'])
        res = []
        for x in r:
          res.append(x)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getSitesList: got sites and status list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getSitesList)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_getSitesStatusWeb = [DictType, ListType, IntType, IntType]
  def export_getSitesStatusWeb(self, selectDict, sortList, startItem, maxItems):
    """ get present sites status list, for the web
        Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsStatusWeb`
    
        :Parameters:
          `selectDict` 
            {
              'SiteName':'name of a site' --- present status
              'ExpandSiteHistory':'name of a site' --- site status history
            }
          
          `sortList`
            (no sorting provided)
          
          `startItem`
          
          `maxItems`
    
        :return:
        {
          'OK': XX, 

          'rpcStub': XX, 'getSitesStatusWeb', ({}, [], X, X)), 

          Value': 
          {

            'ParameterNames': ['SiteName', 'Tier', 'GridType', 'Country', 'Status',
             'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'], 

            'Records': [[], [], ...]

            'TotalRecords': X, 

            'Extras': {}, 
          }
        }
    """
    try:
      gLogger.info("ResourceStatusHandler.getSitesStatusWeb: Attempting to get sites list")
      try:
        res = rsDB.getMonitoredsStatusWeb('Site', selectDict, sortList, startItem, maxItems)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getSitesStatusWeb: got sites list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getSitesStatusWeb)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_setSiteStatus = [StringType, StringType, StringType, StringType]
  def export_setSiteStatus(self, siteName, status, reason, tokenOwner):
    """ 
    Set Site status to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.setSiteStatus`

    :Parameters
      `siteName`
        a string representing the site name
      
      `status`
        a string representing the status

      `reason`
        a string representing the reason

      `tokenOwner`
        a string representing the operator Code
        (can be a user name, or ``RS_SVC`` for the service itself)
    """
    try:
      gLogger.info("ResourceStatusHandler.setSiteStatus: Attempting to modify site %s status" % siteName)
      try:
        rsDB.setSiteStatus(siteName, status, reason, tokenOwner)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.setSiteStatus: Set site %s status." % (siteName))
      return S_OK()
    except Exception:
      errorStr = where(self, self.export_setSiteStatus)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
  types_addOrModifySite = [StringType, StringType, StringType, StringType,  
                           StringType, Time._dateTimeType, StringType, Time._dateTimeType]
  def export_addOrModifySite(self, siteName, siteType, gridSiteName, status, reason, dateEffective, 
                             tokenOwner, dateEnd):
    """ 
    Add or modify a site to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.addOrModifySite`
    
    :Parameters
      `siteName`
        string - name of the site (DIRAC name)
    
      `siteType`
        string - ValidSiteType: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      `gridSiteName`
        string - name of the site in the GOC DB
    
      `status`
        string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      `reason`
        string - free
      
      `dateEffective`
        datetime.datetime - date from which the site status is effective

      `tokenOwner`
        string - free

      `dateEnd`
        datetime.datetime - date from which the site status ends to be effective
    """
    try:
      gLogger.info("ResourceStatusHandler.addOrModifySite: Attempting to add or modify site %s" % siteName)
      try:
        rsDB.addOrModifySite(siteName, siteType, gridSiteName, status, reason, 
                             dateEffective, tokenOwner, dateEnd)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.addOrModifySite: Added (or modified) site %s." % (siteName))
      return S_OK()
    except Exception:
      errorStr = where(self, self.export_addOrModifySite)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
  types_removeSite = [StringType]
  def export_removeSite(self, siteName):
    """ 
    Remove a site type.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.removeSite`
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
    except Exception:
      errorStr = where(self, self.export_removeSite)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
  types_getSitesHistory = [StringType]
  def export_getSitesHistory(self, site):
    """ 
    Get sites history
    """
    try:
      gLogger.info("ResourceStatusHandler.getSitesHistory: Attempting to get site %s history" % (site))
      try:
        res = rsDB.getMonitoredsHistory('Site', name = site)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getSitesHistory: got site %s history" % (site))
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getSitesHistory)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
  types_getSiteTypeList = []
  def export_getSiteTypeList(self):
    """
    Get site type list from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getSiteTypeList`
    """
    try:
      gLogger.info("ResourceStatusHandler.getSiteTypeList: Attempting to get SiteType list")
      try:
        res = rsDB.getTypesList('Site')
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getSiteTypeList: got SiteType list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getSiteTypeList)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_removeSiteType = [StringType]
  def export_removeSiteType(self, siteType):
    """ 
    Remove a SiteType from those monitored.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.removeSiteType`
    """
    try:
      gLogger.info("ResourceStatusHandler.removeSiteType: Attempting to remove site type %s" % (siteType))
      try:
        rsDB.removeType('Site', siteType)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.removeSiteType: Removed site type %s" % (siteType))
      return S_OK()
    except Exception:
      errorStr = where(self, self.export_removeSiteType)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

#############################################################################
# Services functions
#############################################################################

#############################################################################

  #ok
  types_getServicesList = []
  def export_getServicesList(self):
    """
    Get services list from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
    """
    try:
      gLogger.info("ResourceStatusHandler.getServicesList: Attempting to get services list")
      try:
        r = rsDB.getMonitoredsList('Service', paramsList = ['ServiceName'])
        res = []
        for x in r:
          res.append(x[0])
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getServicesList: got services list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getServicesList)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
  types_getServicesStatusWeb = [DictType, ListType, IntType, IntType]
  def export_getServicesStatusWeb(self, selectDict, sortList, startItem, maxItems):
    """ 
    Get present services status list, for the web.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsStatusWeb`
    
    :Parameters
      `selectDict`
        { 'ServiceName':['XX', ...] , 'ExpandServiceHistory': ['XX', ...], 'Status': ['XX', ...]} 
      
      `sortList` 
      
      `startItem` 
      
      `maxItems`
      
    :return: { 
      `ParameterNames`: ['ServiceName', 'ServiceType', 'Site', 'GridType', 'Country', 
      'Status', 'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'], 
      
      'Records': [[], [], ...], 
      
      'TotalRecords': X,
       
      'Extras': {}
      
      }
    """
    try:
      gLogger.info("ResourceStatusHandler.getServicesStatusWeb: Attempting to get services list")
      try:
        res = rsDB.getMonitoredsStatusWeb('Service', selectDict, sortList, startItem, maxItems)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getServicesStatusWeb: got services list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getServicesStatusWeb)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_setServiceStatus = [StringType, StringType, StringType, StringType]
  def export_setServiceStatus(self, serviceName, status, reason, tokenOwner):
    """ 
    Set Service status to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.setServiceStatus`

    :Parameters
      `serviceName`
        a string representing the service name
      
      `status`
        a string representing the status

      `reason`
        a string representing the reason

      `tokenOwner`
        a string representing the operator Code
      (can be a user name, or ``RS_SVC`` for the service itself)
    """
    try:
      gLogger.info("ResourceStatusHandler.setServiceStatus: Attempting to modify service %s status" % serviceName)
      try:
        rsDB.setServiceStatus(serviceName, status, reason, tokenOwner)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.setServiceStatus: Set service %s status." % (serviceName))
      return S_OK()
    except Exception:
      errorStr = where(self, self.export_setServiceStatus)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################
  
  #ok
  types_addOrModifyService = [StringType, StringType, StringType, StringType, StringType, 
                              Time._dateTimeType, StringType, Time._dateTimeType]
  def export_addOrModifyService(self, serviceName, serviceType, siteName, status, 
                                reason, dateEffective, tokenOwner, dateEnd):
    """ 
    Add or modify a service to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.addOrModifyService`
    
    :Parameters
      `serviceName`
        string - name of the service (DIRAC name)
    
      `serviceType`
        string - ValidServiceType: 
        see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      `siteName`
        string - name of the site (DIRAC name)

      `status`
        string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      `reason`
        string - free
      
      `dateEffective`
        datetime.datetime - date from which the service status is effective

      `tokenOwner`
        string - free

      `dateEnd`
        datetime.datetime - date from which the service status ends to be effective
    
    """
    try:
      gLogger.info("ResourceStatusHandler.addOrModifyService: Attempting to add or modify service %s" % serviceName)
      try:
        rsDB.addOrModifyService(serviceName, serviceType, siteName, status, reason, 
                                dateEffective, tokenOwner, dateEnd)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.addOrModifyService: Added (or modified) service %s." % (serviceName))
      return S_OK()
    except Exception:
      errorStr = where(self, self.export_addOrModifyService)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #tested
  types_removeService = [StringType]
  def export_removeService(self, serviceName):
    """ 
    Remove a Service from those monitored
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.removeService`
    """
    try:
      gLogger.info("ResourceStatusHandler.removeService: Attempting to remove modify service %s" % serviceName)
      try:
        rsDB.removeService(serviceName)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.removeService: removed service %s." % (serviceName))
      return S_OK()
    except Exception:
      errorStr = where(self, self.export_removeService)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
  types_getServicesHistory = [StringType]
  def export_getServicesHistory(self, service):
    """ 
    Get services history
    """
    try:
      gLogger.info("ResourceStatusHandler.getServicesHistory: Attempting to get service %s history" % (service))
      try:
        res = rsDB.getMonitoredsHistory('Service', name = service)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getServicesHistory: got service %s history" % (service))
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getServicesHistory)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
#  types_addServiceType = [StringType, StringType]
#  def export_addServiceType(self, serviceType, description=''):
#    """ 
#    Add a service type to the ResourceStatusDB
#    
#    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.addServiceType`
#    
#    :Params:
#      `serviceType`: a string
#
#      `description`: an optional string
#    """
#    try:
#      gLogger.info("ResourceStatusHandler.addServiceType: Attempting to add service type %s" % (serviceType))
#      try:
#        rsDB.addServiceType(serviceType, description)
#      except RSSDBException, x:
#        gLogger.error(whoRaised(x))
#      except RSSException, x:
#        gLogger.error(whoRaised(x))
#      gLogger.info("ResourceStatusHandler.addServiceType: Added service type %s" % (serviceType))
#      return S_OK()
#    except Exception, x:
#      errorStr = where(self, self.export_addServiceType)
#      gLogger.exception(errorStr,lException=x)
#      return S_ERROR(errorStr)

#############################################################################

  #ok
  types_getServiceTypeList = []
  def export_getServiceTypeList(self):
    """
    Get service type list from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getServiceTypeList`
    """
    try:
      gLogger.info("ResourceStatusHandler.getServiceTypeList: Attempting to get ServiceType list")
      try:
        res = rsDB.getTypesList('Service')
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getServiceTypeList: got ServiceType list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getServiceTypeList)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)


#############################################################################

  types_getServiceStats = [StringType]
  def export_getServiceStats(self, siteName):
    """ 
    Returns simple statistics of active, probing and banned services of a site;
        
    :Parameters
      `siteName`
        string - a site name
    
    :returns:
      S_OK { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
      or S_Error
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
    except Exception:
      errorStr = where(self, self.export_getServiceStats)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_removeServiceType = [StringType]
  def export_removeServiceType(self, serviceType):
    """ 
    Remove a service type to the ResourceStatusDB
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.removeServiceType`
    """
    try:
      gLogger.info("ResourceStatusHandler.removeServiceType: Attempting to remove service type %s" % (serviceType))
      try:
        rsDB.removeType('Service', serviceType)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.removeServiceType: Removed service type %s" % (serviceType))
      return S_OK()
    except Exception:
      errorStr = where(self, self.export_removeServiceType)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

#############################################################################
# Resources functions
#############################################################################

#############################################################################

  types_getResourcesStatusWeb = [DictType, ListType, IntType, IntType]
  def export_getResourcesStatusWeb(self, selectDict, sortList, startItem, maxItems):
    """ get present resources status list
        Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsStatusWeb`


        :Parameters:
          `selectDict`
            {'ResourceName':'name of a resource' --- present status
          
          `ExpandResourceHistory`
            'name of a resource' --- resource status history }
          
          `sortList`
            [] (no sorting provided)
          
          `startItem`
          
          `maxItems`
            
        `return`: { 'OK': XX, 'rpcStub': XX, 'getSitesStatusWeb', ({}, [], X, X)), 
          
          'Value': { 'ParameterNames': ['ResourceName', 'SiteName', 'ServiceExposed', 'Country', 
          'Status', 'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'], 
              
          'Records': [[], [], ...]
              
          'TotalRecords': X, 
             
          'Extras': {} } }
    """
    try:
      gLogger.info("ResourceStatusHandler.getResourcesStatusWeb: Attempting to get resources list")
      try:
        res = rsDB.getMonitoredsStatusWeb('Resource', selectDict, sortList, startItem, maxItems)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getResourcesStatusWeb: got resources list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getResourcesStatusWeb)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)


#############################################################################

  types_setResourceStatus = [StringType, StringType, StringType, StringType]
  def export_setResourceStatus(self, resourceName, status, reason, tokenOwner):
    """ 
    Set Resource status to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.setResourceStatus`

    :Parameters
      `resourceName`
        a string representing the resource name
      
      `status`
        a string representing the status

      `reason`
        a string representing the reason

      `tokenOwner`
        a string representing the operator Code
        (can be a user name, or ``RS_SVC`` for the service itself)
    """
    try:
      gLogger.info("ResourceStatusHandler.setResourceStatus: Attempting to modify resource %s status" % resourceName)
      try:
        rsDB.setResourceStatus(resourceName, status, reason, tokenOwner)
        return S_OK()
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.setResourceStatus: Set resource %s status." % (resourceName))
      return S_OK()
    except Exception:
      errorStr = where(self, self.export_setResourceStatus)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
  types_addOrModifyResource = [StringType, StringType, StringType, StringType, StringType, StringType, 
                               StringType, Time._dateTimeType, StringType, Time._dateTimeType]
  def export_addOrModifyResource(self, resourceName, resourceType, serviceType, siteName, gridSiteName, 
                                 status, reason, dateEffective, tokenOwner, dateEnd):
    """ 
    Add or modify a resource to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.addOrModifyResource`
    
    :Parameters:
      `resourceName`
        string - name of the resource (DIRAC name)
    
      `resourceType`
        string - ValidResourceType: 
        see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      `resourceType`
        string - ValidServiceType: 
        see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      `siteName`
        string - name of the site (DIRAC name, can be 'NULL')

      `gridSiteName`
        string - name of the site (Grid name, found in GOC DB)

      `status`
        string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      `reason`
        string - free
      
      `dateEffective`
        datetime.datetime - date from which the resource status is effective

      `tokenOwner`
        string - free

      `dateEnd`
        datetime.datetime - date from which the resource status ends to be effective
        
    """
    try:
      gLogger.info("ResourceStatusHandler.addOrModifyResource: Attempting to add or modify resource %s %s" % (resourceName, siteName))
      try:
        rsDB.addOrModifyResource(resourceName, resourceType, serviceType, siteName, gridSiteName, 
                                 status, reason, dateEffective, tokenOwner, dateEnd)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.addOrModifyResource: Added (or modified) resource %s of site %s" % (resourceName, siteName))
      return S_OK()
    except Exception:
      errorStr = where(self, self.export_addOrModifyResource)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_removeResource = [StringType]
  def export_removeResource(self, resourceName):
    """ 
    Remove a Resource from those monitored
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.removeResource`
    """
    try:
      gLogger.info("ResourceStatusHandler.Resource: Attempting to remove modify Resource %s" % resourceName)
      try:
        rsDB.removeResource(resourceName = resourceName)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.Resource: removed Resource %s." % (resourceName))
      return S_OK()
    except Exception:
      errorStr = where(self, self.export_removeResource)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
#  types_addResourceType = [StringType, StringType]
#  def export_addResourceType(self, resourceType, description=''):
#    """
#    Add a resource type to the ResourceStatusDB. 
#    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.addResourceType`
#    
#    :Params:
#      `resourceType`: a string
#      `description`: an optional long string
#    """
#    try:
#      gLogger.info("ResourceStatusHandler.addResourceType: Attempting to add resource type %s" % (resourceType))
#      try:
#        rsDB.addResourceType(resourceType, description)
#      except RSSDBException, x:
#        gLogger.error(whoRaised(x))
#      except RSSException, x:
#        gLogger.error(whoRaised(x))
#      gLogger.info("ResourceStatusHandler.addResourceType: Added resource type %s" % (resourceType))
#      return S_OK()
#    except Exception, x:
#      errorStr = where(self, self.export_addResourceType)
#      gLogger.exception(errorStr,lException=x)
#      return S_ERROR(errorStr)

#############################################################################

  #ok
  types_getResourceTypeList = []
  def export_getResourceTypeList(self):
    """
    Get resource type list from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getResourceTypeList`
    """
    try:
      gLogger.info("ResourceStatusHandler.getResourceTypeList: Attempting to get ResourceType list")
      try:
        res = rsDB.getTypesList('Resource')
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getResourceTypeList: got ResourceType list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getResourceTypeList)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_removeResourceType = [StringType]
  def export_removeResourceType(self, resourceType):
    """ 
    Remove a resource type to the ResourceStatusDB
    """
    try:
      gLogger.info("ResourceStatusHandler.removeResourceType: Attempting to remove resource type %s" % (resourceType))
      try:
        rsDB.removeType('Resource', resourceType)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.removeResourceType: Removed resource type %s" % (resourceType))
      return S_OK()
    except Exception:
      errorStr = where(self, self.export_removeResourceType)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_getResourcesList = []
  def export_getResourcesList(self):
    """ 
    Get resources list from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
    """
    try:
      gLogger.info("ResourceStatusHandler.getResourcesList: Attempting to get resources list")
      try:
        r = rsDB.getMonitoredsList('Resource', paramsList = ['ResourceName'])
        res = []
        for x in r:
          res.append(x[0])
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getResourcesList: got resources list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getResourcesList)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_getCEsList = []
  def export_getCEsList(self):
    """ 
    Get CEs list from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
    """
    try:
      gLogger.info("ResourceStatusHandler.getCEsList: Attempting to get CEs list")
      try:
        r = rsDB.getMonitoredsList('Resource', paramsList = ['ResourceName'], 
                                   resourceType = ['CE', 'CREAMCE'])
        res = []
        for x in r:
          res.append(x[0])
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getCEsList: got CEs list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getResourcesList)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_getResourcesHistory = [StringType]
  def export_getResourcesHistory(self, resource):
    """ get resources history
    """
    try:
      gLogger.info("ResourceStatusHandler.getResourcesHistory: Attempting to get resource %s history" % (resource))
      try:
        res = rsDB.getMonitoredsHistory('Resource', name = resource)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getResourcesHistory: got resource %s history" % (resource))
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getResourcesHistory)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
  types_getResourceStats = [StringType, StringType]
  def export_getResourceStats(self, granularity, name):
    """ 
    Returns simple statistics of active, probing and banned resources of a site or service;
        
    :Parameters:
      `granularity`
        string, should be in ['Site', 'Service']
      
      `name`
        string, name of site or service
    
    :return:
      S_OK { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
      or S_ERROR
    """

    try:
      gLogger.info("ResourceStatusHandler.getResourceStats: Attempting to get resource stats for site %s" %(name))
      try:
        res = rsDB.getResourceStats(granularity, name)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getResourceStats: got resource stats")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getResourceStats)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

#############################################################################
# StorageElements functions
#############################################################################

#############################################################################

  #Ok
  types_getSESitesList = []
  def export_getSESitesList(self):
    """
    Get sites list of the storage elements from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
    """
    try:
      gLogger.info("ResourceStatusHandler.getSESitesList: Attempting to get SE sites list")
      try:
        r = rsDB.getMonitoredsList('StorageElement', paramsList = ['GridSiteName'])
        res = []
        for gridSite in r:
          DIRACsites = getDIRACSiteName(gridSite[0])
          if not DIRACsites['OK']:
            raise RSSException, "No DIRAC site name" + where(self, self.export_getSESitesList)
          DIRACsites = DIRACsites['Value']
          for DIRACsite in DIRACsites:
            if DIRACsite not in res:
              res.append(DIRACsite)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getSESitesList: got SE sites list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getSitesList)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################
  #Ok
  types_getStorageElementsList = []
  def export_getStorageElementsList(self):
    """
    Get sites list from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
    """
    try:
      gLogger.info("ResourceStatusHandler.getStorageElementsList: Attempting to get sites list")
      try:
        r = rsDB.getMonitoredsList('StorageElement', paramsList = ['StorageElementName'])
        res = []
        for x in r:
          res.append(x[0])
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getStorageElementsList: got sites list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getStorageElementsList)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #Ok
  types_getStorageElementsStatusWeb = [DictType, ListType, IntType, IntType]
  def export_getStorageElementsStatusWeb(self, selectDict, sortList, startItem, maxItems):
    """ Get present sites status list, for the web
        Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsStatusWeb`

    
        :Parameters:
          `selectDict` 
            {
              'StorageElementName':'name of a site' --- present status
              'ExpandStorageElementHistory':'name of a site' --- site status history
            }
          
          `sortList`
            [] (no sorting provided)
          
          `startItem`
          
          `maxItems`
    
        :return:
        {
          'OK': XX, 

          'rpcStub': XX, 'getStorageElementsStatusWeb', ({}, [], X, X)), 

          Value': 
          {

            'ParameterNames': ['StorageElementName', 'Tier', 'GridType', 'Country', 'Status',
             'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'], 

            'Records': [[], [], ...]

            'TotalRecords': X, 

            'Extras': {}, 
          }
        }
    """
    try:
      gLogger.info("ResourceStatusHandler.getStorageElementsStatusWeb: Attempting to get SEs list")
      try:
        res = rsDB.getMonitoredsStatusWeb('StorageElement', selectDict, sortList, startItem, maxItems)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getStorageElementsStatusWeb: got SEs list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getStorageElementsStatusWeb)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_setStorageElementStatus = [StringType, StringType, StringType, StringType]
  def export_setStorageElementStatus(self, seName, status, reason, tokenOwner):
    """ 
    Set StorageElement status to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.setStorageElementStatus`

    :Parameters
      `seName`
        a string representing the se name
      
      `status`
        a string representing the status

      `reason`
        a string representing the reason

      `tokenOwner`
        a string representing the operator Code
        (can be a user name, or ``RS_SVC`` for the service itself)
    """
    try:
      gLogger.info("ResourceStatusHandler.setStorageElementStatus: Attempting to modify se %s status" % seName)
      try:
        rsDB.setStorageElementStatus(seName, status, reason, tokenOwner)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.setStorageElementStatus: Set SE %s status." % (seName))
      return S_OK()
    except Exception:
      errorStr = where(self, self.export_setStorageElementStatus)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
  types_addOrModifyStorageElement = [StringType, StringType, StringType, StringType, StringType, 
                                     Time._dateTimeType, StringType, Time._dateTimeType]
  def export_addOrModifyStorageElement(self, seName, resourceName, gridSiteName, status, reason, 
                                       dateEffective, tokenOwner, dateEnd):
    """ 
    Add or modify a site to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.addOrModifyStorageElement`
    
    :Parameters
      `seName`
        string - name of the se (DIRAC name)
    
      `resourceName`
        string - name of the node (resource)
    
      `gridSiteName`
        string - name of the site (GOC DB name)
    
      `status`
        string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      `reason`
        string - free
      
      `dateEffective`
        datetime.datetime - date from which the site status is effective

      `tokenOwner`
        string - free

      `dateEnd`
        datetime.datetime - date from which the site status ends to be effective
    """
    try:
      gLogger.info("ResourceStatusHandler.addOrModifyStorageElement: Attempting to add or modify se %s" % seName)
      try:
        rsDB.addOrModifyStorageElement(seName, resourceName, gridSiteName, status, reason, 
                                       dateEffective, tokenOwner, dateEnd)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.addOrModifyStorageElement: Added (or modified) SE %s." % (seName))
      return S_OK()
    except Exception:
      errorStr = where(self, self.export_addOrModifyStorageElement)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
  types_removeStorageElement = [StringType]
  def export_removeStorageElement(self, seName):
    """ 
    Remove a site type.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.removeStorageElement`
    """
    try:
      gLogger.info("ResourceStatusHandler.removeStorageElement: Attempting to remove modify SE %s" % seName)
      try:
        rsDB.removeStorageElement(seName)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.removeStorageElement: removed SE %s." % (seName))
      return S_OK()
    except Exception:
      errorStr = where(self, self.export_removeStorageElement)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
  types_getStorageElementsHistory = [StringType]
  def export_getStorageElementsHistory(self, se):
    """ 
    Get sites history
    """
    try:
      gLogger.info("ResourceStatusHandler.getStorageElementsHistory: Attempting to get SE %s history" % (se))
      try:
        res = rsDB.getMonitoredsHistory('StorageElement', name = se)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getStorageElementsHistory: got SE %s history" % (se))
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getStorageElementsHistory)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
  types_getStorageElementsStats = [StringType, StringType]
  def export_getStorageElementsStats(self, granularity, name):
    """ 
    Returns simple statistics of active, probing and banned storageElementss of a site or resource;
        
    :Parameters:
      `granularity`
        string, should be in ['Site', 'Resource']
      
      `name`
        string, name of site or service
    
    :return:
      S_OK { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
      or S_Error
    """

    try:
      gLogger.info("StorageElementsStatusHandler.getStorageElementsStats: Attempting to get storageElements stats for %s" %(name))
      try:
        res = rsDB.getStorageElementsStats(granularity, name)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("StorageElementsStatusHandler.getStorageElementsStats: got storageElements stats")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getStorageElementsStats)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

#############################################################################
# Mixed functions
#############################################################################

#############################################################################

  #ok
  types_addStatus = [StringType, StringType]
  def export_addStatus(self, status, description=''):
    """ 
    Add a status to the ResourceStatusDB. 
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.addStatus`
    
    :Parameters:
      `status`
        a string
      
      `description`
        an optional long string
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
    except Exception:
      errorStr = where(self, self.export_addStatus)
      gLogger.exception(errorStr)
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
    except Exception:
      errorStr = where(self, self.export_getStatusList)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_getCountries = [StringType]
  def export_getCountries(self, countries):
    """
    Get countries list from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getCountries`
    """
    try:
      gLogger.info("ResourceStatusHandler.getCountries: Attempting to get countries list")
      try:
        res = rsDB.getCountries(countries)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getCountries: got countries list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getCountries)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_removeStatus = [StringType]
  def export_removeStatus(self, status):
    """ 
    Remove a status from the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.removeStatus`
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
    except Exception:
      errorStr = where(self, self.export_addStatus)
      gLogger.exception(errorStr)
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
    except Exception:
      errorStr = where(self, self.export_getPeriods)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
  types_syncWithCS = [StringType, StringType]
  def export_syncWithCS(self, p1, p2):
    """ 
    Synchronize DB with CS - parameters are fake
    """
    try:
      gLogger.info("ResourceStatusHandler.syncWithCS: Attempting to sync DB with CS")
      try:
        res = rsDB.syncWithCS(p1, p2)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.syncWithCS: DB sync-ed")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_syncWithCS)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_getPolicyRes = [StringType, StringType, BooleanType]
  def export_getPolicyRes(self, name, policyName, lastCheckTime):
    """ get Policy Result
    """
    try:
      gLogger.info("ResourceStatusHandler.getPolicyRes: Attempting to get result of %s for %s" % (policyName, name))
      try:
        res = rsDB.getPolicyRes(name, policyName, lastCheckTime)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getPolicyRes: got result of %s for %s" % (policyName, name))
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getPolicyRes)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)
    
#############################################################################

  types_getGeneralName = [StringType, StringType, StringType]
  def export_getGeneralName(self, granularity, name, toGranularity):
    """ get General Name
    """
    try:
      gLogger.info("ResourceStatusHandler.getGeneralName: Attempting to get %s general name" % (name))
      try:
        res = rsDB.getGeneralName(name, granularity, toGranularity)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getGeneralName: got %s general name" % (name))
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getGeneralName)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_getCachedAccountingResult = [StringType, StringType, StringType]
  def export_getCachedAccountingResult(self, name, plotType, plotName):
    """ get a cached accounting result
    """
    try:
      gLogger.info("ResourceStatusHandler.getCachedAccountingResult: Attempting to get %s: %s, %s accounting cached result" % (name, plotType, plotName))
      try:
        res = rsDB.getAccountingCacheStuff(['Result'], name = name, plotType = plotType, 
                                           plotName = plotName)
        if not (res == []):
          res = res[0]
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getCachedAccountingResult: got %s: %s %s cached result" % (name, plotType, plotName))
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getCachedAccountingResult)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_getCachedResult = [StringType, StringType, StringType, StringType]
  def export_getCachedResult(self, name, command, value, opt_ID):
    """ get a cached result
    """
    try:
      gLogger.info("ResourceStatusHandler.getCachedResult: Attempting to get %s: %s, %s cached result" % (name, value, command))
      try:
        if opt_ID == 'NULL':
          opt_ID = None
        res = rsDB.getClientsCacheStuff(['Result'], name = name, commandName = command, 
                                        value = value, opt_ID = opt_ID)
        if not (res == []):
          res = res[0]  
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getCachedResult: got %s: %s %s cached result" % (name, value, command))
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getCachedResult)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_getCachedIDs = [StringType, StringType]
  def export_getCachedIDs(self, name, command):
    """ get a cached IDs
    """
    try:
      gLogger.info("ResourceStatusHandler.getCachedIDs: Attempting to get %s: %s cached IDs" % (name, command))
      try:
        dt_ID = []
        res = rsDB.getClientsCacheStuff('opt_ID', name = name, commandName = command)
        for tuple_dt_ID in res:
          if tuple_dt_ID[0] not in dt_ID:
            dt_ID.append(tuple_dt_ID[0])
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getCachedIDs: got %s: %s cached result" % (name, command))
      return S_OK(dt_ID)
    except Exception:
      errorStr = where(self, self.export_getCachedIDs)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_getDownTimesWeb = [DictType, ListType, IntType, IntType]
  def export_getDownTimesWeb(self, selectDict, sortList, startItem, maxItems):
    """ get down times as registered with the policies.
        Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getDownTimesWeb`
    
        :Parameters:
          `selectDict` 
            {
              'Granularity':'Site', 'Resource', or a list with both
              'Severity':'OUTAGE', 'AT_RISK', or a list with both
            }
          
          `sortList`
            [] (no sorting provided)
          
          `startItem`
          
          `maxItems`
    
        :return:
        {
          'OK': XX, 

          'rpcStub': XX, 'getDownTimesWeb', ({}, [], X, X)), 

          Value': 
          {

            'ParameterNames': ['Granularity', 'Name', 'Severity', 'When'], 

            'Records': [[], [], ...]

            'TotalRecords': X, 

            'Extras': {}, 
          }
        }
    """
    try:
      gLogger.info("ResourceStatusHandler.getDownTimesWeb: Attempting to get down times list")
      try:
        try:
          granularity = selectDict['Granularity']
        except KeyError:
          granularity = []
          
        if not isinstance(granularity, list):
          granularity = [granularity]
        commands = []
        if granularity == []:
          commands = ['DTEverySites', 'DTEveryResources']
        elif 'Site' in granularity:
          commands.append('DTEverySites')
        elif 'Resource' in granularity:
          commands.append('DTEveryResources')
  
        try:
          severity = selectDict['Severity']
        except KeyError:
          severity = []
        if not isinstance(severity, list):
          severity = [severity]
        if severity == []:
          severity = ['AT_RISK', 'OUTAGE']
  
        res = rsDB.getClientsCacheStuff(['Name', 'Opt_ID', 'Value', 'Result', 'CommandName'], 
                                        commandName = commands)
        records = []
        
        if not ( res == () ):
          made_IDs = []
          
          for dt_tuple in res:
            considered_ID = dt_tuple[1]
            if considered_ID not in made_IDs:
              name = dt_tuple[0]
              if dt_tuple[4] == 'DTEverySites':
                granularity = 'Site'
              elif dt_tuple[4] == 'DTEveryResources':
                granularity = 'Resource'
              toTake = ['Severity', 'StartDate', 'EndDate', 'Description']
              
              for dt_t in res:
                if considered_ID == dt_t[1]:
                  if toTake != []:
                    if dt_t[2] in toTake:
                      if dt_t[2] == 'Severity':
                        sev = dt_t[3]
                        toTake.remove('Severity')
                      if dt_t[2] == 'StartDate':
                        startDate = dt_t[3]
                        toTake.remove('StartDate')
                      if dt_t[2] == 'EndDate':
                        endDate = dt_t[3]
                        toTake.remove('EndDate')
                      if dt_t[2] == 'Description':
                        description = dt_t[3]
                        toTake.remove('Description')
              
              now = datetime.datetime.utcnow().replace(microsecond = 0, second = 0)
              startDate_datetime = datetime.datetime.strptime(startDate, '%Y-%m-%d %H:%M')
              endDate_datetime = datetime.datetime.strptime(endDate, '%Y-%m-%d %H:%M')
              
              if endDate_datetime < now:
                when = 'Finished'
              else:
                if startDate_datetime < now:
                  when = 'OnGoing'
                else:
                  hours = str(convertTime(startDate_datetime - now, 'hours'))
                  when = 'In ' + hours + ' hours.'
              
              if sev in severity:
                records.append([ considered_ID, granularity, name, sev,
                                when, startDate, endDate, description ])
              
              made_IDs.append(considered_ID)
        
        # adding downtime links to the GOC DB page in Extras
        DT_links = []
        for record in records:
          DT_link = rsDB.getClientsCacheStuff(['Result'], opt_ID = record[0], value = 'Link')
          DT_link = DT_link[0][0]
          DT_links.append({ record[0] : DT_link } )
          
        paramNames = ['ID', 'Granularity', 'Name', 'Severity', 'When', 'Start', 'End', 'Description']
    
        finalDict = {}
        finalDict['TotalRecords'] = len(records)
        finalDict['ParameterNames'] = paramNames
    
        # Return all the records if maxItems == 0 or the specified number otherwise
        if maxItems:
          finalDict['Records'] = records[startItem:startItem+maxItems]
        else:
          finalDict['Records'] = records
    
        finalDict['Extras'] = DT_links
            
        
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getDownTimesWeb: got DT list")
      return S_OK(finalDict)
    except Exception:
      errorStr = where(self, self.export_getDownTimesWeb)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_enforcePolicies = [StringType, StringType, BooleanType]
  def export_enforcePolicies(self, granularity, name, useNewRes = True):
    """ Enforce all the policies. If `useNewRes` is False, use cached results only (where available).
    """
    try:
      gLogger.info("ResourceStatusHandler.enforcePolicies: Attempting to enforce policies for %s %s" % (granularity, name))
      try:
        reason = serviceType = resourceType = None 

        res = rsDB.getStuffToCheck(granularity, name = name)[0]
        status = res[1]
        formerStatus = res[2]
        siteType = res[3]
        tokenOwner = res[len(res)-1]
        if granularity == 'Resource':
          resourceType = res[4]
        elif granularity == 'Service':
          serviceType = res[4]
        
        from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
        pep = PEP(VOExtension, granularity, name, status, formerStatus, reason, siteType, 
                  serviceType, resourceType, tokenOwner, useNewRes)
        pep.enforce(rsDBIn = rsDB)
        
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.enforcePolicies: enforced for %s: %s" % (granularity, name))
      return S_OK("ResourceStatusHandler.enforcePolicies: enforced for %s: %s" % (granularity, name))
    except Exception:
      errorStr = where(self, self.export_getCachedResult)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_publisher = [StringType, StringType, BooleanType]
  def export_publisher(self, granularity, name, useNewRes = False):
    """ get a view
    
    :Parameters:
      `granularity`
        string - a ValidRes
    
      `name`
        string - name of the res

      `useNewRes`
        boolean. When set to true, will get new results, 
        otherwise it will get cached results (where available).
    """
    try:
      gLogger.info("ResourceStatusHandler.publisher: Attempting to get info for %s: %s" % (granularity, name))
      try:
        if useNewRes == True:
          from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
          gLogger.info("ResourceStatusHandler.publisher: Recalculating policies for %s: %s" % (granularity, name))
          if granularity in ('Site', 'Sites'):
            res = rsDB.getStuffToCheck(granularity, name = name)[0]
            status = res[1]
            formerStatus = res[2]
            siteType = res[3]
            tokenOwner = res[4]
            
            pep = PEP(VOExtension, granularity, name, status, formerStatus, None, siteType, 
                      None, None, tokenOwner, useNewRes)
            pep.enforce(rsDBIn = rsDB)

            res = rsDB.getMonitoredsList('Service', paramsList = ['ServiceName'], siteName = name)
            services = [x[0] for x in res]
            for s in services:
              res = rsDB.getStuffToCheck('Service', name = s)[0]
              status = res[1]
              formerStatus = res[2]
              siteType = res[3]
              serviceType = res[4]
              
              pep = PEP(VOExtension, 'Service', s, status, formerStatus, None, siteType, 
                        serviceType, None, tokenOwner, useNewRes)
              pep.enforce(rsDBIn = rsDB)
          else:
            reason = serviceType = resourceType = None 
  
            res = rsDB.getStuffToCheck(granularity, name = name)[0]
            status = res[1]
            formerStatus = res[2]
            siteType = res[3]
            tokenOwner = res[len(res)-1]
            if granularity == 'Resource':
              resourceType = res[4]
            elif granularity == 'Service':
              serviceType = res[4]
            
            from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
            pep = PEP(VOExtension, granularity, name, status, formerStatus, reason, siteType, 
                      serviceType, resourceType, tokenOwner, useNewRes)
            pep.enforce(rsDBIn = rsDB)
            
        res = publisher.getInfo(granularity, name, useNewRes)
      except InvalidRes, x:
        errorStr = "Invalid granularity"
        gLogger.exception(whoRaised(x) + errorStr)
        return S_ERROR(errorStr)
      except RSSException, x:
        errorStr = "RSSException"
        gLogger.exception(whoRaised(x) + errorStr)
      gLogger.info("ResourceStatusHandler.publisher: got info for %s: %s" % (granularity, name))
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_publisher)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_reAssignToken = [StringType, StringType, StringType]
  def export_reAssignToken(self, granularity, name, requester):
    """ 
    Re-assign a token: if it was assigned to a human, assign it to 'RS_SVC' and viceversa.
    """
    try:
      str = "ResourceStatusHandler.reAssignToken: attempting to re-assign token "
      str = str + "%s: %s: %s" % (granularity, name, requester)
      gLogger.info(str)
      try:
        token = rsDB.getTokens(granularity, name)
        tokenOwner = token[0][1]
        if tokenOwner == 'RS_SVC':
          if requester != 'RS_SVC':
            rsDB.setToken(granularity, name, requester, datetime.datetime.utcnow() + datetime.timedelta(hours = 24))
        else:
          rsDB.setToken(granularity, name, 'RS_SVC', datetime.datetime(9999, 12, 31, 23, 59, 59))
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.reAssignToken: re-assigned token %s: %s: %s" % (granularity, name, requester))
      return S_OK()
    except Exception:
      errorStr = where(self, self.export_reAssignToken)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)
    
    
#############################################################################

  types_extendToken = [StringType, StringType, IntType]
  def export_extendToken(self, granularity, name, hrs):
    """ 
    Extend the duration of token by the number of provided hours. 
    """
    try:
      str = "ResourceStatusHandler.extendToken: attempting to extend token "
      str = str + "%s: %s for %i hours" % (granularity, name, hrs)
      gLogger.info(str)
      try:
        token = rsDB.getTokens(granularity, name)
        tokenOwner = token[0][1]
        tokenExpiration = token[0][2]
        try:
          tokenNewExpiration = tokenExpiration + datetime.timedelta(hours = hrs)
        except OverflowError:
          pass
        rsDB.setToken(granularity, name, tokenOwner, tokenNewExpiration)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.extendToken: extended token %s: %s for %i hours" % (granularity, name, hrs))
      return S_OK()
    except Exception:
      errorStr = where(self, self.export_extendToken)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)
    
#############################################################################

  types_whatIs = [StringType]
  def export_whatIs(self, name):
    """ 
    Find which is the granularity of name. 
    """
    try:
      str = "ResourceStatusHandler.whatIs: attempting to find granularity of %s" %name
      gLogger.info(str)
      try:
        g = rsDB.whatIs(name)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.whatIs: got %s granularity" % (name))
      return S_OK(g)
    except Exception:
      errorStr = where(self, self.export_whatIs)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_getGridSiteName = [StringType, StringType]
  def export_getGridSiteName(self, granularity, name):
    """
    Get Grid Site Name, given granularity and a name.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getGridSiteName`
    """
    try:
      gLogger.info("ResourceStatusHandler.getGridSiteName: Attempting to get the Grid Site Name")
      try:
        res = rsDB.getGridSiteName(granularity, name)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getGridSiteName: got GridSiteName list")
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getGridSiteName)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################
  