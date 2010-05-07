""" ``ResourceStatusHandler`` exposes the service of the Resource Status System. 
    It uses :mod:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB` for database persistence. 
    
    To use this service
      
    >>> from DIRAC.Core.DISET.RPCClient import RPCCLient
    >>> server = RPCCLient("ResourceStatus/ResourceStatus")

"""


from datetime import datetime, timedelta

from types import *
from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger, gConfig
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Policy import Configurations
from DIRAC.ResourceStatusSystem.Utilities.Publisher import Publisher 
from DIRAC.ResourceStatusSystem.Client.Command.CommandCaller import CommandCaller

rsDB = False

def initializeResourceStatusHandler(serviceInfo):
  
  global rsDB
  rsDB = ResourceStatusDB()

  cc = CommandCaller()

  global publisher
  publisher = Publisher(rsDBIn = rsDB, commandCallerIn = cc)

  gConfig.addListenerToNewVersionEvent( rsDB.syncWithCS )
  return S_OK()

class ResourceStatusHandler(RequestHandler):

  def initialize(self):
    #Listerer -> Listener
#    gConfig.addListenerToNewVersionEvent( self.export_syncWithCS )
    pass
    
#############################################################################

#############################################################################
# Sites functions
#############################################################################

#############################################################################

  #Ok
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

  #Ok
  types_getSitesStatusWeb = [DictType, ListType, IntType, IntType]
  def export_getSitesStatusWeb(self, selectDict, sortList, startItem, maxItems):
    """ get present sites status list, for the web
        Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsStatusWeb`
    
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
  def export_setSiteStatus(self, siteName, status, reason, operatorCode):
    """ 
    Set Site status to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.setSiteStatus`

    :params:
      :attr:`siteName`: a string representing the site name
      
      :attr:`status`: a string representing the status

      :attr:`reason`: a string representing the reason

      :attr:`operatorCode`: a string representing the operator Code
      (can be a user name, or ``RS_SVC`` for the service itself)
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
    except Exception:
      errorStr = where(self, self.export_setSiteStatus)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
  types_addOrModifySite = [StringType, StringType, StringType, StringType, Time._dateTimeType, 
                           StringType, Time._dateTimeType]
  def export_addOrModifySite(self, siteName, siteType, status, reason, dateEffective, 
                             operatorCode, dateEnd):
    """ 
    Add or modify a site to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.addOrModifySite`
    
    :params:
      :attr:`siteName`: string - name of the site (DIRAC name)
    
      :attr:`siteType`: string - ValidSiteType: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`status`: string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateEffective`: datetime - date from which the site status is effective

      :attr:`operatorCode`: string - free

      :attr:`dateEnd`: datetime - date from which the site status ends to be effective
    """
    try:
      gLogger.info("ResourceStatusHandler.addOrModifySite: Attempting to add or modify site %s" % siteName)
      try:
        rsDB.addOrModifySite(siteName, siteType, status, reason, dateEffective, operatorCode, dateEnd)
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
    
    :params:
      :attr:`selectDict`: { 'ServiceName':['XX', ...] , 'ExpandServiceHistory': ['XX', ...], 'Status': ['XX', ...]} 
      
      :attr:`sortList` 
      
      :attr:`startItem` 
      
      :attr:`maxItems`
      
    :return: { 
      :attr:`ParameterNames`: ['ServiceName', 'ServiceType', 'Site', 'GridType', 'Country', 
      'Status', 'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'], 
      
      :attr:'Records': [[], [], ...], 
      
      :attr:'TotalRecords': X,
       
      :attr:'Extras': {}
      
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
  def export_setServiceStatus(self, serviceName, status, reason, operatorCode):
    """ 
    Set Service status to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.setServiceStatus`

    :params:
      :attr:`serviceName`: a string representing the service name
      
      :attr:`status`: a string representing the status

      :attr:`reason`: a string representing the reason

      :attr:`operatorCode`: a string representing the operator Code
      (can be a user name, or ``RS_SVC`` for the service itself)
    """
    try:
      gLogger.info("ResourceStatusHandler.setServiceStatus: Attempting to modify service %s status" % serviceName)
      try:
        rsDB.setServiceStatus(serviceName, status, reason, operatorCode)
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
  types_addOrModifyService = [StringType, StringType, StringType, StringType, StringType, Time._dateTimeType, StringType, Time._dateTimeType]
  def export_addOrModifyService(self, serviceName, serviceType, siteName, status, 
                                reason, dateEffective, operatorCode, 
                                dateEnd=datetime(9999, 12, 31, 23, 59, 59)):
    """ 
    Add or modify a service to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.addOrModifyService`
    
    :params:
      :attr:`serviceName`: string - name of the service (DIRAC name)
    
      :attr:`serviceType`: string - ValidServiceType: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`status`: string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateEffective`: datetime - date from which the service status is effective

      :attr:`operatorCode`: string - free

      :attr:`dateEnd`: datetime - date from which the service status ends to be effective
    
    """
    try:
      gLogger.info("ResourceStatusHandler.addOrModifyService: Attempting to add or modify service %s" % serviceName)
      try:
        rsDB.addOrModifyService(serviceName, serviceType, siteName, status, reason, 
                                dateEffective, operatorCode, dateEnd)
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
#      :attr:`serviceType`: a string
#
#      :attr:`description`: an optional string
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
        
    :params:
      :attr:`siteName`: string - a site name
    
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

  #ok
  types_getResourcesStatusWeb = [DictType, ListType, IntType, IntType]
  def export_getResourcesStatusWeb(self, selectDict, sortList, startItem, maxItems):
    """ get present resources status list
        Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsStatusWeb`


        Input:
        selectDict: {'ResourceName':'name of a resource' --- present status
        
        'ExpandResourceHistory':'name of a resource' --- resource status history }
        
        sortList = [] (no sorting provided)
        
        startItem
        
        maxItems
          
        Output: { 'OK': XX, 'rpcStub': XX, 'getSitesStatusWeb', ({}, [], X, X)), 
        
        Value': { 'ParameterNames': ['ResourceName', 'SiteName', 'ServiceExposed', 'Country', 
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
  def export_setResourceStatus(self, resourceName, status, reason, operatorCode):
    """ 
    Set Resource status to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.setResourceStatus`

    :params:
      :attr:`resourceName`: a string representing the resource name
      
      :attr:`status`: a string representing the status

      :attr:`reason`: a string representing the reason

      :attr:`operatorCode`: a string representing the operator Code
      (can be a user name, or ``RS_SVC`` for the service itself)
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
    except Exception:
      errorStr = where(self, self.export_setResourceStatus)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
  types_addOrModifyResource = [StringType, StringType, StringType, StringType, StringType, 
                               StringType, Time._dateTimeType, StringType, Time._dateTimeType]
  def export_addOrModifyResource(self, resourceName, resourceType, serviceName, siteName, 
                                 status, reason, dateEffective, operatorCode, 
                                 dateEnd=datetime(9999, 12, 31, 23, 59, 59)):
    """ 
    Add or modify a resource to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.addOrModifyResource`
    
    :params:
      :attr:`resourceName`: string - name of the resource (DIRAC name)
    
      :attr:`resourceType`: string - ValidResourceType: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`status`: string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateEffective`: datetime - date from which the resource status is effective

      :attr:`operatorCode`: string - free

      :attr:`dateEnd`: datetime - date from which the resource status ends to be effective
    """
    try:
      gLogger.info("ResourceStatusHandler.addOrModifyResource: Attempting to add or modify resource %s %s" % (resourceName, siteName))
      try:
        rsDB.addOrModifyResource(resourceName, resourceType, serviceName, siteName, status, reason, dateEffective, operatorCode, dateEnd)
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
#      :attr:`resourceType`: a string
#      :attr:`description`: an optional long string
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

  #ok
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

  #ok
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
  types_getResourceStats = [StringType]
  def export_getResourceStats(self, granularity, name):
    """ 
    Returns simple statistics of active, probing and banned resources of a site or service;
        
    :params:
      :attr:`granularity` string, should be in ['Site', 'Service']
      
      :attr:`name`: string, name of site or service
    
    :returns:
      S_OK { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
      or S_Error
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
        r = rsDB.getMonitoredsList('StorageElement', paramsList = ['SiteName'])
        res = []
        for x in r:
          res.append(x[0])
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

    
        :Params:
          :attr:`selectDict` = 
            {
              'StorageElementName':'name of a site' --- present status
              'ExpandStorageElementHistory':'name of a site' --- site status history
            }
          
          :attr:`sortList` = [] (no sorting provided)
          
          :attr:`startItem`
          
          :attr:`maxItems`
    
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
  def export_setStorageElementStatus(self, seName, status, reason, operatorCode):
    """ 
    Set StorageElement status to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.setStorageElementStatus`

    :params:
      :attr:`seName`: a string representing the se name
      
      :attr:`status`: a string representing the status

      :attr:`reason`: a string representing the reason

      :attr:`operatorCode`: a string representing the operator Code
      (can be a user name, or ``RS_SVC`` for the service itself)
    """
    try:
      gLogger.info("ResourceStatusHandler.setStorageElementStatus: Attempting to modify se %s status" % seName)
      try:
        rsDB.setStorageElementStatus(seName, status, reason, operatorCode)
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
  def export_addOrModifyStorageElement(self, seName, resourceName, siteName, status, reason, 
                                       dateEffective, operatorCode, dateEnd):
    """ 
    Add or modify a site to the ResourceStatusDB.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.addOrModifyStorageElement`
    
    :params:
      :attr:`seName`: string - name of the se (DIRAC name)
    
      :attr:`resourceName`: string - name of the node (resource)
    
      :attr:`siteName`: string - name of the site (DIRAC name)
    
      :attr:`status`: string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateEffective`: datetime - date from which the site status is effective

      :attr:`operatorCode`: string - free

      :attr:`dateEnd`: datetime - date from which the site status ends to be effective
    """
    try:
      gLogger.info("ResourceStatusHandler.addOrModifyStorageElement: Attempting to add or modify se %s" % seName)
      try:
        rsDB.addOrModifyStorageElement(seName, resourceName, siteName, status, reason, 
                                       dateEffective, operatorCode, dateEnd)
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
  types_getStorageElementsStats = [StringType]
  def export_getStorageElementsStats(self, granularity, name):
    """ 
    Returns simple statistics of active, probing and banned storageElementss of a site or resource;
        
    :params:
      :attr:`granularity` string, should be in ['Site', 'Resource']
      
      :attr:`name`: string, name of site or service
    
    :returns:
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
    except Exception:
      errorStr = where(self, self.export_addStatus)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  #ok
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

  types_getCachedResult = [StringType, StringType]
  def export_getCachedResult(self, name, command):
    """ get a cached result
    """
    try:
      gLogger.info("ResourceStatusHandler.getCachedResulr: Attempting to get %s: %s cached result" % (name, command))
      try:
        res = rsDB.getClientsCacheRes(name, command)
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      gLogger.info("ResourceStatusHandler.getCachedResult: got %s: %s cached result" % (name, command))
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_getCachedResult)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  types_publisher = [StringType, StringType, StringType]
  def export_publisher(self, granularity, name, view):
    """ get a view
    
    :params:
      :attr:`granularity`: string - a ValidRes
    
      :attr:`name`: string - name of the res

      :attr:`view`: string - name of the view
    """
    try:
      gLogger.info("ResourceStatusHandler.publisher: Attempting to get view %s for %s" % (view, name))
      try:
        res = publisher.getInfo(granularity, name, view)
      except InvalidRes, x:
        errorStr = "Invalid granularity"
        gLogger.exception(whoRaised(x) + errorStr)
        return S_ERROR(errorStr)
      except InvalidView, x:
        errorStr = "Invalid view"
        gLogger.exception(whoRaised(x) + errorStr)
        return S_ERROR(errorStr)
      except RSSException, x:
        errorStr = "RSSException"
        gLogger.exception(whoRaised(x) + errorStr)
      gLogger.info("ResourceStatusHandler.publisher: got view %s for %s" % (view, name))
      return S_OK(res)
    except Exception:
      errorStr = where(self, self.export_publisher)
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################
