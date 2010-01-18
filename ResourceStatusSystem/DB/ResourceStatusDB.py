""" 
The ResourcesStatusDB module contains a couple of exception classes, and a 
class to interact with the ResourceStatus DB.
"""

from types import *
from datetime import datetime, timedelta
from DIRAC import gLogger, gConfig
from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *

#############################################################################

class RSSDBException(RSSException):
  """ 
  DB exception 
  """
  pass

#############################################################################

class NotAllowedDate(RSSException):
  """ 
  Exception that signals a not allowed date 
  """
  pass

#############################################################################

class ResourceStatusDB:
  """ 
  The ResourcesStatusDB class is a front-end to the Resource Status Database.
  
  The simplest way to instantiate an object of type :class:`ResourceStatusDB` 
  is simply by calling 

   >>> rsDB = ResourceStatusDB()

  This way, it will use the standard :mod:`DIRAC.Core.Base.DB`. 
  But there's the possibility to use other DB classes. 
  For example, we could pass custom DB instantiations to it, 
  provided the interface is the same exposed by :mod:`DIRAC.Core.Base.DB`.

   >>> AnotherDB = AnotherDBClass()
   >>> rsDB = ResourceStatusDB(DBin = AnotherDB)

  Alternatively, for testing purposes, you could do:

   >>> from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
   >>> mockDB = Mock()
   >>> rsDB = ResourceStatusDB(DBin = mockDB)

  Or, if you want to work with a local DB, providing it's mySQL:

   >>> rsDB = ResourceStatusDB(DBin = ['UserName', 'Password'])

  """

  
  def __init__(self, *args, **kwargs):

    if len(args) == 1:
      if isinstance(args[0], str):
        systemInstance=args[0]
        maxQueueSize=10
      if isinstance(args[0], int):
        maxQueueSize=args[0]
        systemInstance='Default'
    elif len(args) == 2:
      systemInstance=args[0]
      maxQueueSize=args[1]
    elif len(args) == 0:
      systemInstance='Default'
      maxQueueSize=10
    
    if 'DBin' in kwargs.keys():
      DBin = kwargs['DBin']
      if isinstance(DBin, Mock):
        self.db = DBin
      elif isinstance(DBin, list):
        from DIRAC.Core.Utilities.MySQL import MySQL
        self.db = MySQL('localhost', DBin[0], DBin[1], 'ResourceStatusDB')
    else:
      from DIRAC.Core.Base.DB import DB
      self.db = DB('ResourceStatusDB','ResourceStatus/ResourceStatusDB',maxQueueSize)
  
#  def __init__(self, DBin=None, systemInstance='Default', maxQueueSize=10):
#    
#    if not isinstance(DBin, Mock):
#      from DIRAC.Core.Base.DB import DB
#      self.db = DB('ResourceStatusDB','ResourceStatus/ResourceStatusDB',maxQueueSize)
#    else:
#      self.db = DBin
#    self.lock = threading.Lock()
    

#############################################################################

#############################################################################
# Monitored functions
#############################################################################

#############################################################################

  def getMonitoredsList(self, granularity, paramsList = None, siteName = None, 
                        serviceName = None, resourceName = None, storageElementName = None, 
                        status = None, siteType = None, resourceType = None, serviceType = None):
    """ 
    Get Present Sites/Services/Resources/StorageElements lists. 
    
    :params:
      :attr:`granularity`: a ValidRes
    
      :attr:`paramsList`: a list of parameters can be entered. If not given, 
      a custom list is used. 
      
      :attr:`siteName`, `serviceName`, `resourceName`, `storageElementName`: 
      a string or a list representing the site/service/resource/storageElement name. 
      If not given, fetch all.
      
      :attr:`status`: a string or a list representing the status. If not given, fetch all.
      
      :attr:`siteType`: a string or a list representing the site type. 
      If not given, fetch all.
      
      :attr:`serviceType`: a string or a list representing the service type. 
      If not given, fetch all.
      
      :attr:`resourceType`: a string or a list representing the resource type.
      If not given, fetch all.
      
      See :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils` for these parameters.
      
    :return:
      list of monitored paramsList's values
    """
    
    #get the parameters of the query
    
    getInfo = []
    
    if granularity in ('Site', 'Sites'):
      DBname = 'SiteName'
      DBtable = 'PresentSites'
      getInfo = getInfo + ['SiteName', 'SiteType']
    elif granularity in ('Service', 'Services'):
      DBname = 'ServiceName'
      DBtable = 'PresentServices'
      getInfo = getInfo + ['SiteName', 'ServiceName', 'ServiceType']
    elif granularity in ('Resource', 'Resources'):
      DBname = 'ResourceName'
      DBtable = 'PresentResources'
      getInfo = getInfo + ['SiteName', 'ResourceName', 'ResourceType']
    elif granularity in ('StorageElement', 'StorageElements'):
      DBname = 'StorageElementName'
      DBtable = 'PresentStorageElements'
      getInfo = getInfo + ['SiteName', 'StorageElementName']
    else:
      raise InvalidRes, where(self, self.getMonitoredsList)

    #paramsList
    if (paramsList == None or paramsList == []):
      params = DBname + ', Status, FormerStatus, DateEffective, LastCheckTime '
    else:
      if type(paramsList) is not type([]):
        paramsList = [paramsList]
      params = ','.join([x.strip()+' ' for x in paramsList])

    #siteName
    if 'SiteName' in getInfo:
      if (siteName == None or siteName == []):
        r = "SELECT SiteName FROM PresentSites"
        resQuery = self.db._query(r)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.getMonitoredsList)+resQuery['Message']
        if not resQuery['Value']:
          siteName = []
        siteName = [ x[0] for x in resQuery['Value']]
        siteName = ','.join(['"'+x.strip()+'"' for x in siteName])
      else:
        if type(siteName) is not type([]):
          siteName = [siteName]
        siteName = ','.join(['"'+x.strip()+'"' for x in siteName])
    
    #serviceName
    if 'ServiceName' in getInfo:
      if (serviceName == None or serviceName == []): 
        r = "SELECT ServiceName FROM PresentServices"
        resQuery = self.db._query(r)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.getMonitoredsList)+resQuery['Message']
        if not resQuery['Value']:
          serviceName = []
        serviceName = [ x[0] for x in resQuery['Value']]
        serviceName = ','.join(['"'+x.strip()+'"' for x in serviceName])
      else:
        if type(serviceName) is not type([]):
          serviceName = [serviceName]
        serviceName = ','.join(['"'+x.strip()+'"' for x in serviceName])
    
    #resourceName
    if 'ResourceName' in getInfo:
      if (resourceName == None or resourceName == []): 
        r = "SELECT ResourceName FROM PresentResources"
        resQuery = self.db._query(r)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.getMonitoredsList)+resQuery['Message']
        if not resQuery['Value']:
          resourceName = []
        resourceName = [ x[0] for x in resQuery['Value']]
        resourceName = ','.join(['"'+x.strip()+'"' for x in resourceName])
      else:
        if type(resourceName) is not type([]):
          resourceName = [resourceName]
        resourceName = ','.join(['"'+x.strip()+'"' for x in resourceName])
      
    #storageElementName
    if 'StorageElementName' in getInfo:
      if (storageElementName == None or storageElementName == []): 
        r = "SELECT StorageElementName FROM PresentStorageElements"
        resQuery = self.db._query(r)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.getMonitoredsList)+resQuery['Message']
        if not resQuery['Value']:
          storageElementName = []
        storageElementName = [ x[0] for x in resQuery['Value']]
        storageElementName = ','.join(['"'+x.strip()+'"' for x in storageElementName])
      else:
        if type(storageElementName) is not type([]):
          storageElementName = [storageElementName]
        storageElementName = ','.join(['"'+x.strip()+'"' for x in storageElementName])
      
    #status
    if (status == None or status == []):
      status = ValidStatus
    else:
      if type(status) is not type([]):
        status = [status]
    status = ','.join(['"'+x.strip()+'"' for x in status])

    #siteType
    if 'SiteType' in getInfo:
      if (siteType == None or siteType == []):
        siteType = ValidSiteType
      else:
        if type(siteType) is not type([]):
          siteType = [siteType]
      siteType = ','.join(['"'+x.strip()+'"' for x in siteType])

    #serviceType
    if 'ServiceType' in getInfo:
      if (serviceType == None or serviceType == []):
        serviceType = ValidServiceType
      else:
        if type(serviceType) is not type([]):
          serviceType = [serviceType]
      serviceType = ','.join(['"'+x.strip()+'"' for x in serviceType])

    #resourceType
    if 'ResourceType' in getInfo:
      if (resourceType == None or resourceType == []):
        resourceType = ValidResourceType
      else:
        if type(resourceType) is not type([]):
          resourceType = [resourceType]
      resourceType = ','.join(['"'+x.strip()+'"' for x in resourceType])

    #query construction
    #base
    req = "SELECT %s FROM %s WHERE" %(params, DBtable)
    #what "names"
    if 'SiteName' in getInfo:
      if siteName != [] and siteName != None and siteName is not None and siteName != '':
        req = req + " SiteName IN (%s) AND" %(siteName)
    if 'ServiceName' in getInfo:
      if serviceName != [] and serviceName != None and serviceName is not None and serviceName != '':
        req = req + " ServiceName IN (%s) AND" %(serviceName)
    if 'ResourceName' in getInfo:
      if resourceName != [] and resourceName != None and resourceName is not None and resourceName != '':
        req = req + " ResourceName IN (%s) AND" %(resourceName)
    if 'StorageElementName' in getInfo:
      if storageElementName != [] and storageElementName != None and storageElementName is not None and storageElementName != '':
        req = req + " StorageElementName IN (%s) AND" %(storageElementName)
    #status    
    req = req + " Status in (%s)" % (status)
    #types
    if 'SiteType' in getInfo:
      req = req + " AND SiteType in (%s)" % (siteType)
    if 'ServiceType' in getInfo:
      req = req + " AND ServiceType in (%s)" % (serviceType)
    if 'ResourceType' in getInfo:
      req = req + " AND ResourceType IN (%s)" % (resourceType)

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getMonitoredsList)+resQuery['Message']
    if not resQuery['Value']:
      return []
    list = []
    list = [ x for x in resQuery['Value']]
    return list


#############################################################################

  def getMonitoredsStatusWeb(self, granularity, selectDict, sortList, startItem, maxItems):
    """ 
    Get present sites status list, for the web.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
    and :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsHistory`
    
    Example of parameters:
    
    :params:
      :attr:`selectDict`: { 'SiteName':['XX', ...] , 'ExpandSiteHistory': ['XX', ...], 
      'Status': ['XX', ...]}
      and equivalents for the other monitoreds 
      
      :attr:`sortList` 
      
      :attr:`startItem` 
      
      :attr:`maxItems`
      
    :return: { 
      :attr:`ParameterNames`: ['SiteName', 'Tier', 'GridType', 'Country', 
      'Status', 'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'], 
      
      :attr:'Records': [[], [], ...], 
      
      :attr:'TotalRecords': X,
       
      :attr:'Extras': {}
      
      }
    """
        
    if granularity in ('Site', 'Sites'):
      paramNames = ['SiteName', 'Tier', 'GridType', 'Country',
                     'Status', 'DateEffective', 'FormerStatus', 'Reason']
      paramsList = ['SiteName', 'SiteType', 'Status', 'DateEffective', 
                    'FormerStatus', 'Reason']
    elif granularity in ('Service', 'Services'):
      paramNames = ['ServiceName', 'ServiceType', 'Site', 'Country', 'Status', 
                    'DateEffective', 'FormerStatus', 'Reason']
      paramsList = ['ServiceName', 'ServiceType', 'SiteName', 'Status', 
                    'DateEffective', 'FormerStatus', 'Reason']
    elif granularity in ('Resource', 'Resources'):
      paramNames = ['ResourceName', 'ServiceName', 'SiteName', 'ResourceType', 
                    'Country', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
      paramsList = ['ResourceName', 'ServiceName', 'SiteName', 'ResourceType', 
                    'Status', 'DateEffective', 'FormerStatus', 'Reason']
    elif granularity in ('StorageElement', 'StorageElements'):
      paramNames = ['StorageElementName', 'ResourceName', 'SiteName', 'Country', 
                    'Status', 'DateEffective', 'FormerStatus', 'Reason']
      paramsList = ['StorageElementName', 'ResourceName', 'SiteName', 'Status', 
                    'DateEffective', 'FormerStatus', 'Reason']
    else:
      raise InvalidRes, where(self, self.getMonitoredsStatusWeb)

    resultDict = {}
    records = []

    sites_select = []
    services_select = []
    resources_select = []
    storageElements_select = []
    status_select = []
    siteType_select = []
    serviceType_select = []
    resourceType_select = []
    expand_site_history = ''
    expand_service_history = ''
    expand_resource_history = ''
    expand_storageElements_history = ''

    #specify SiteName
    if selectDict.has_key('SiteName'):
      sites_select = selectDict['SiteName']
      if type(sites_select) is not list:
        sites_select = [sites_select]
      del selectDict['SiteName']
      
    #specify ServiceName
    if selectDict.has_key('ServiceName'):
      services_select = selectDict['ServiceName']
      if type(services_select) is not list:
        services_select = [services_select]
      del selectDict['ServiceName']
      
    #ResourceName
    if selectDict.has_key('ResourceName'):
      resources_select = selectDict['ResourceName']
      if type(resources_select) is not list:
        resources_select = [resources_select]
      del selectDict['ResourceName']

    #StorageElementName
    if selectDict.has_key('StorageElementName'):
      storageElements_select = selectDict['StorageElementName']
      if type(storageElements_select) is not list:
        storageElements_select = [storageElements_select]
      del selectDict['StorageElementName']

    #Status
    if selectDict.has_key('Status'):
      status_select = selectDict['Status']
      if type(status_select) is not list:
        status_select = [status_select]
      del selectDict['Status']
      
    #SiteType
    if selectDict.has_key('SiteType'):
      siteType_select = selectDict['SiteType']
      if type(siteType_select) is not list:
        siteType_select = [siteType_select]
      del selectDict['SiteType']
      
    #ServiceType
    if selectDict.has_key('ServiceType'):
      serviceType_select = selectDict['ServiceType']
      if type(serviceType_select) is not list:
        serviceType_select = [serviceType_select]
      del selectDict['ServiceType']
      
    #ResourceType
    if selectDict.has_key('ResourceType'):
      resourceType_select = selectDict['ResourceType']
      if type(resourceType_select) is not list:
        resourceType_select = [resourceType_select]
      del selectDict['ResourceType']
    
    #ExpandSiteHistory
    if selectDict.has_key('ExpandSiteHistory'):
      paramsList = ['SiteName', 'Status', 'Reason', 'DateEffective']
      sites_select = selectDict['ExpandSiteHistory']
      if type(sites_select) is not list:
        sites_select = [sites_select]
      sitesHistory = self.getMonitoredsHistory(granularity, paramsList = paramsList, 
                                               name = sites_select)
      # sitesHistory is a list of tuples
      for site in sitesHistory:
        record = []
        record.append(site[0]) #SiteName
        record.append(None) #Tier
        record.append(None) #GridType
        record.append(None) #Country
        record.append(site[1]) #Status
        record.append(site[3].isoformat(' ')) #DateEffective
        record.append(None) #FormerStatus
        record.append(site[2]) #Reason
        records.append(record)

    #ExpandServiceHistory
    elif selectDict.has_key('ExpandServiceHistory'):
      paramsList = ['ServiceName', 'Status', 'Reason', 'DateEffective']
      services_select = selectDict['ExpandServiceHistory']
      if type(services_select) is not list:
        services_select = [services_select]
      servicesHistory = self.getMonitoredsHistory(granularity, paramsList = paramsList, 
                                                  name = services_select)
      # servicesHistory is a list of tuples
      for service in servicesHistory:
        record = []
        record.append(service[0]) #ServiceName
        record.append(None) #ServiceType
        record.append(None) #Site
        record.append(None) #Country
        record.append(service[1]) #Status
        record.append(service[3].isoformat(' ')) #DateEffective
        record.append(None) #FormerStatus
        record.append(service[2]) #Reason
        records.append(record)

    #ExpandResourceHistory
    elif selectDict.has_key('ExpandResourceHistory'):
      paramsList = ['ResourceName', 'Status', 'Reason', 'DateEffective']
      resources_select = selectDict['ExpandResourceHistory']
      if type(resources_select) is not list:
        resources_select = [resources_select]
      resourcesHistory = self.getMonitoredsHistory(granularity, paramsList = paramsList,
                                                    name = resources_select)
      # resourcesHistory is a list of tuples
      for resource in resourcesHistory:
        record = []
        record.append(resource[0]) #ResourceName
        record.append(None) #ServiceName
        record.append(None) #SiteName
        record.append(None) #ResourceType
        record.append(None) #Country
        record.append(resource[1]) #Status
        record.append(resource[3].isoformat(' ')) #DateEffective
        record.append(None) #FormerStatus
        record.append(resource[2]) #Reason
        records.append(record)
    
    #ExpandStorageElementHistory
    elif selectDict.has_key('ExpandStorageElementHistory'):
      paramsList = ['StorageElementName', 'Status', 'Reason', 'DateEffective']
      storageElements_select = selectDict['ExpandStorageElementHistory']
      if type(storageElements_select) is not list:
        storageElements_select = [storageElements_select]
      storageElementsHistory = self.getMonitoredsHistory(granularity, paramsList = paramsList,
                                                          name = storageElements_select)
      # storageElementsHistory is a list of tuples
      for storageElement in storageElementsHistory:
        record = []
        record.append(storageElement[0]) #StorageElementName
        record.append(None) #ResourceName
        record.append(None) #SiteName
        record.append(None) #Country
        record.append(storageElement[1]) #Status
        record.append(storageElement[3].isoformat(' ')) #DateEffective
        record.append(None) #FormerStatus
        record.append(storageElement[2]) #Reason
        records.append(record)
    
    else:
      if granularity in ('Site', 'Sites'):
        sitesList = self.getMonitoredsList(granularity, paramsList = paramsList, 
                                           siteName = sites_select, status = status_select, 
                                           siteType = siteType_select)
        for site in sitesList:
          record = []
          record.append(site[0]) #SiteName
          record.append(site[1]) #Tier
          gridType = (site[0]).split('.').pop(0)
          record.append(gridType) #GridType
          country = (site[0]).split('.').pop()
          record.append(country) #Country
          record.append(site[2]) #Status
          record.append(site[3].isoformat(' ')) #DateEffective
          record.append(site[4]) #FormerStatus
          record.append(site[5]) #Reason
          records.append(record)

      elif granularity in ('Service', 'Services'):
        servicesList = self.getMonitoredsList(granularity, paramsList = paramsList, 
                                              serviceName = services_select, 
                                              siteName = sites_select, status = status_select, 
                                              serviceType = serviceType_select)
        for service in servicesList:
          record = []
          record.append(service[0]) #ServiceName
          record.append(service[1]) #ServiceType
          record.append(service[2]) #Site
          country = (service[0]).split('.').pop()
          record.append(country) #Country
          record.append(service[3]) #Status
          record.append(service[4].isoformat(' ')) #DateEffective
          record.append(service[5]) #FormerStatus
          record.append(service[6]) #Reason
          records.append(record)

      elif granularity in ('Resource', 'Resources'):
        resourcesList = self.getMonitoredsList(granularity, paramsList = paramsList, 
                                               resourceName = resources_select, 
                                               siteName = sites_select, 
                                               status = status_select, 
                                               resourceType = resourceType_select)
        for resource in resourcesList:
          record = []
          record.append(resource[0]) #ResourceName
          record.append(resource[1]) #ServiceName
          record.append(resource[2]) #SiteName
          record.append(resource[3]) #ResourceType
          country = (resource[2]).split('.').pop()
          record.append(country) #Country
          record.append(resource[4]) #Status
          record.append(resource[5].isoformat(' ')) #DateEffective
          record.append(resource[6]) #FormerStatus
          record.append(resource[7]) #Reason
          records.append(record)
      

      elif granularity in ('StorageElement', 'StorageElements'):
        storageElementsList = self.getMonitoredsList(granularity, paramsList = paramsList, 
                                                     storageElementName = storageElements_select, 
                                                     siteName = sites_select, 
                                                     status = status_select)
        for storageElement in storageElementsList:
          record = []
          record.append(storageElement[0]) #StorageElementName
          record.append(storageElement[1]) #ResourceName
          record.append(storageElement[2]) #SiteName
          country = (storageElement[2]).split('.').pop()
          record.append(country) #Country
          record.append(storageElement[3]) #Status
          record.append(storageElement[4].isoformat(' ')) #DateEffective
          record.append(storageElement[5]) #FormerStatus
          record.append(storageElement[6]) #Reason
          records.append(record)


    finalDict = {}
    finalDict['TotalRecords'] = len(records)
    finalDict['ParameterNames'] = paramNames

    # Return all the records if maxItems == 0 or the specified number otherwise
    if maxItems:
      finalDict['Records'] = records[startItem:startItem+maxItems]
    else:
      finalDict['Records'] = records

    finalDict['Extras'] = None

    return finalDict


#############################################################################

  def getMonitoredsHistory(self, granularity, paramsList = None, name = None):
    """ 
    Get history of sites/services/resources/storageElements in a list 
    (a site name can be specified)
        
    :params:
      :attr:`granularity`: a ValidRes
    
      :attr:`paramsList`: A list of parameters can be entered. If not, a custom list is used.
  
      :attr:`name`: list of strings. If not given, fetches the complete list 
    """
    
    if paramsList is not None:
      if type(paramsList) is not type([]):
        paramsList = [paramsList]
      params = ','.join([x.strip()+' ' for x in paramsList])

    if granularity in ('Site', 'Sites'):
      if (paramsList == None or paramsList == []):
        params = 'SiteName, Status, Reason, DateCreated, DateEffective, DateEnd, OperatorCode '
      DBtable = 'SitesHistory'
      DBname = 'SiteName'
      DBid = 'SitesHistoryID'
    elif granularity in ('Service', 'Services'):
      if (paramsList == None or paramsList == []):
        params = 'ServiceName, Status, Reason, DateCreated, DateEffective, DateEnd, OperatorCode '
      DBtable = 'ServicesHistory'
      DBname = 'ServiceName'
      DBid = 'ServicesHistoryID'
    elif granularity in ('Resource', 'Resources'):
      if (paramsList == None or paramsList == []):
        params = 'ResourceName, Status, Reason, DateCreated, DateEffective, DateEnd, OperatorCode '
      DBtable = 'ResourcesHistory'
      DBname = 'ResourceName'
      DBid = 'ResourcesHistoryID'
    elif granularity in ('StorageElement', 'StorageElements'):
      if (paramsList == None or paramsList == []):
        params = 'StorageElementName, Status, Reason, DateCreated, DateEffective, DateEnd, OperatorCode '
      DBtable = 'StorageElementsHistory'
      DBname = 'StorageElementName'
      DBid = 'StorageElementsHistoryID'
    else:
      raise InvalidRes, where(self, self.getMonitoredsHistory)
      

    if (name == None or name == []): 
      req = "SELECT %s FROM %s ORDER BY %s, %s" %(params, DBtable, DBname, DBid)
    else:
      if type(name) is not type([]):
        name = [name]
      name = ','.join(['"'+x.strip()+'"' for x in name])
      req = "SELECT %s FROM %s WHERE %s IN (%s) ORDER BY %s" % (params, DBtable, DBname, 
                                                                name, DBid)
    
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getMonitoredsHistory)+resQuery['Message']
    if not resQuery['Value']:
      return []
    list = []
    list = [ x for x in resQuery['Value']]
    return list
 
#############################################################################

  def setLastMonitoredCheckTime(self, granularity, name):
    """ 
    Set to utcnow() LastCheckTime of table Sites/Services/Resources/StorageElements
    
    :params:
      :attr:`granularity`: a ValidRes
    
      :attr:`name`: string
    """
    
    if granularity in ('Site', 'Sites'):
      DBtable = 'Sites'
      DBname = 'SiteName'
    elif granularity in ('Service', 'Services'):
      DBtable = 'Services'
      DBname = 'ServiceName'
    elif granularity in ('Resource', 'Resources'):
      DBtable = 'Resources'
      DBname = 'ResourceName'
    elif granularity in ('StorageElement', 'StorageElements'):
      DBtable = 'StorageElements'
      DBname = 'StorageElementName'
    else:
      raise InvalidRes, where(self, self.setLastMonitoredCheckTime)
    
    req = "UPDATE %s SET LastCheckTime = UTC_TIMESTAMP() WHERE " %(DBtable)
    req = req + "%s = '%s' AND DateEffective <= UTC_TIMESTAMP();" % (DBname, name)
    resUpdate = self.db._update(req)
    
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self.setLastMonitoredCheckTime) + resUpdate['Message']

#############################################################################

  def setMonitoredReason(self, granularity, name, reason, operatorCode):
    """
    Set new reason to name.
        
    :params:
      :attr:`granularity`: a ValidRes
    
      :attr:`name`: string, name or res

      :attr:`reason`: string, reason

      :attr:`operatorCode`: string, who's making this change 
      (RS_SVC if it's the service itslef)
    """
    
    if granularity in ('Site', 'Sites'):
      DBtable = 'Sites'
      DBname = 'SiteName'
    elif granularity in ('Service', 'Services'):
      DBtable = 'Services'
      DBname = 'ServiceName'
    elif granularity in ('Resource', 'Resources'):
      DBtable = 'Resources'
      DBname = 'ResourceName'
    elif granularity in ('StorageElement', 'StorageElements'):
      DBtable = 'StorageElements'
      DBname = 'StorageElementName'
    else:
      raise InvalidRes, where(self, self.setMonitoredReason)
    
    req = "UPDATE %s SET Reason = '%s', " %(DBtable, reason)
    req = req + "OperatorCode = '%s' WHERE %s = '%s';"  %(operatorCode, DBname, name)
    resUpdate = self.db._update(req)
    
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self.setMonitoredReason) + resUpdate['Message']

#############################################################################

#############################################################################
# Site functions
#############################################################################

#############################################################################

  def setSiteStatus(self, siteName, status, reason, operatorCode):
    """ 
    Set a Site status, effective from now, with no ending
        
    :params:
      :attr:`siteName`: string
  
      :attr:`status`: string. Possibilities: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
  
      :attr:`reason`: string
  
      :attr:`operatorCode`: string. For the service itself: `RS_SVC`
    """

    gLogger.info("Setting Site %s new status: %s" % (siteName, status))
    req = "SELECT SiteType FROM Sites WHERE SiteName = '%s' " %(siteName)
    req = req + "AND DateEffective < UTC_TIMESTAMP();"
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.setSiteStatus) + resQuery['Message']
    if not resQuery['Value']:
      return None

    siteType = resQuery['Value'][0][0]
  
    self.addOrModifySite(siteName, siteType, status, reason, 
                         datetime.utcnow().replace(microsecond = 0), operatorCode, 
                         datetime(9999, 12, 31, 23, 59, 59))
    
#############################################################################

  def addOrModifySite(self, siteName, siteType, status, reason, dateEffective, 
                      operatorCode, dateEnd):
    """ 
    Add or modify a site to the Sites table.
    
    :params:
      :attr:`siteName`: string - name of the site (DIRAC name)
    
      :attr:`siteType`: string - ValidSiteType: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`status`: string - ValidStatus: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateEffective`: datetime - date from which the site status is effective

      :attr:`operatorCode`: string - free

      :attr:`dateEnd`: datetime - date from which the site status ends to be effective
    """

    dateCreated = datetime.utcnow().replace(microsecond = 0)
    if dateEffective < dateCreated:
      dateEffective = dateCreated
    if dateEnd < dateEffective:
      raise NotAllowedDate, where(self, self.addOrModifySite)
    if status not in ValidStatus:
      raise InvalidStatus, where(self, self.addOrModifySite)


    #check if the site is already there
    query = "SELECT SiteName FROM Sites WHERE SiteName='%s'" % siteName
    resQuery = self.db._query(query)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.addOrModifySite) + resQuery['Message']

    if resQuery['Value']: 
      if dateEffective <= (dateCreated + timedelta(minutes=2)):
        #site modification, effective in less than 2 minutes
        self.setDateEnd('Site', siteName, dateEffective)
        self.transact2History('Site', siteName, dateEffective)
      else:
        self.setDateEnd('Site', siteName, dateEffective)
    else:
      if status in ('Active', 'Probing'):
        oldStatus = 'Banned'
      else:
        oldStatus = 'Active'
      self._addSiteHistoryRow(siteName, oldStatus, reason, dateCreated, dateEffective, 
                              datetime.utcnow().replace(microsecond = 0).isoformat(' '), 
                              operatorCode)

    #in any case add a row to present Sites table
    self._addSiteRow(siteName, siteType, status, reason, dateCreated, dateEffective, 
                     dateEnd, operatorCode)
#    siteRow = "Added %s --- %s " %(siteName, dateEffective)
#    return siteRow

#############################################################################

  def _addSiteRow(self, siteName, siteType, status, reason, dateCreated, dateEffective, 
                  dateEnd, operatorCode):
    """
    Add a new site row in Sites table

    :params:
      :attr:`siteName`: string - name of the site (DIRAC name)
    
      :attr:`siteType`: string - ValidSiteType: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`status`: string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateCreated`: datetime or string - date when which the site row is created

      :attr:`dateEffective`: datetime or string - date from which the site status is effective

      :attr:`dateEnd`: datetime or string - date from which the site status ends to be effective

      :attr:`operatorCode`: string - free
    """

    if not isinstance(dateCreated, basestring):
      dateCreated = dateCreated.isoformat(' ')
    if not isinstance(dateEffective, basestring):
      dateEffective = dateEffective.isoformat(' ')
    if not isinstance(dateEnd, basestring):
      dateEnd = dateEnd.isoformat(' ')
    if status not in ValidStatus:
      raise InvalidStatus, where(self, self._addSiteRow)
      

    req = "INSERT INTO Sites (SiteName, SiteType, Status, Reason, "
    req = req + "DateCreated, DateEffective, DateEnd, OperatorCode) "
    req = req + "VALUES ('%s', '%s', '%s', '%s', " % (siteName, siteType, status, reason) 
    req = req + "'%s', '%s', '%s', '%s');" %(dateCreated, dateEffective, dateEnd, operatorCode)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self._addSiteRow) + resUpdate['Message']

#############################################################################

  def _addSiteHistoryRow(self, siteName, status, reason, dateCreated, dateEffective, 
                         dateEnd, operatorCode):
    """ 
    Add an old site row in the SitesHistory table

    :params:
      :attr:`siteName`: string - name of the site (DIRAC name)
    
      :attr:`status`: string - ValidStatus: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateCreated`: datetime or string - date when which the site row is created

      :attr:`dateEffective`: datetime or string - date from which the site status is effective

      :attr:`dateEnd`: datetime or string - date from which the site status 
      ends to be effective

      :attr:`operatorCode`: string - free
    """

    if not isinstance(dateCreated, basestring):
      dateCreated = dateCreated.isoformat(' ')
    if not isinstance(dateEffective, basestring):
      dateEffective = dateEffective.isoformat(' ')
    if not isinstance(dateEnd, basestring):
      dateEnd = dateEnd.isoformat(' ')

    req = "INSERT INTO SitesHistory (SiteName, Status, Reason, DateCreated,"
    req = req + " DateEffective, DateEnd, OperatorCode) "
    req = req + "VALUES ('%s', '%s', '%s', '%s', " % (siteName, status, reason, dateCreated)
    req = req + "'%s', '%s', '%s');" % (dateEffective, dateEnd, operatorCode)
    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self._addSiteHistoryRow) + resUpdate['Message']


#############################################################################

  def removeSite(self, siteName):
    """ 
    Completely remove a site from the Sites and SitesHistory tables
    
    :params:
      :attr:`siteName`: string
    """
    
    self.removeResource(siteName = siteName)
    self.removeService(siteName = siteName)
    
    req = "DELETE from Sites WHERE SiteName = '%s';" %siteName
    resDel = self.db._update(req)
    if not resDel['OK']:
      raise RSSDBException, where(self, self.removeSite) + resDel['Message']
    
    req = "DELETE from SitesHistory WHERE SiteName = '%s';" %siteName
    resDel = self.db._update(req)
    if not resDel['OK']:
      raise RSSDBException, where(self, self.removeSite) + resDel['Message']

#############################################################################

#############################################################################
# Resource functions
#############################################################################

#############################################################################

  def setResourceStatus(self, resourceName, status, reason, operatorCode):
    """ 
    Set a Resource status, effective from now, with no ending
    
    :params:
      :attr:`resourceName`: string

      :attr:`status`: string. Possibilities: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string

      :attr:`operatorCode`: string. For the service itself: `RS_SVC`
    """

    gLogger.info("Setting Resource %s new status: %s" % (resourceName, status))
    req = "SELECT ResourceType, ServiceName, SiteName FROM Resources WHERE "
    req = req + "ResourceName = '%s' AND DateEffective < UTC_TIMESTAMP();" %(resourceName)
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.setResourceStatus) + resQuery['Message']
    if not resQuery['Value']:
      return None

    resourceType = resQuery['Value'][0][0]
    serviceName = resQuery['Value'][0][1]
    siteName = resQuery['Value'][0][2]

    self.addOrModifyResource(resourceName, resourceType, serviceName, siteName, status, 
                             reason, datetime.utcnow().replace(microsecond = 0), 
                             operatorCode, datetime(9999, 12, 31, 23, 59, 59))
    
#############################################################################

  def addOrModifyResource(self, resourceName, resourceType, serviceName, siteName, status, 
                          reason, dateEffective, operatorCode, dateEnd):
    """ 
    Add or modify a resource to the Resources table.
    
    :params:
      :attr:`resourceName`: string - name of the resource (DIRAC name)
    
      :attr:`resourceType`: string - ValidResourceType: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`status`: string - ValidStatus: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateEffective`: datetime - date from which the resource status is effective

      :attr:`operatorCode`: string - free

      :attr:`dateEnd`: datetime - date from which the resource status ends to be effective
    """

    dateCreated = datetime.utcnow().replace(microsecond = 0)
    if dateEffective < dateCreated:
      dateEffective = dateCreated
    if dateEnd < dateEffective:
      raise NotAllowedDate, where(self, self.addOrModifyResource)
    if status not in ValidStatus:
      raise InvalidStatus, where(self, self.addOrModifySite)

    #check if the resource is already there
    query = "SELECT ResourceName FROM Resources WHERE ResourceName='%s'" % (resourceName)
    resQuery = self.db._query(query)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.addOrModifyResource) + resQuery['Message']

    if resQuery['Value']: 
      #site modification, effective from now
      if dateEffective <= (dateCreated + timedelta(minutes=2)):
        self.setDateEnd('Resource', resourceName, dateEffective)
        self.transact2History('Resource', resourceName, serviceName, siteName, dateEffective)
      else:
        self.setDateEnd('Resource', resourceName, dateEffective)
    else:
      if status in ('Active', 'Probing'):
        oldStatus = 'Banned'
      else:
        oldStatus = 'Active'
      self._addResourcesHistoryRow(resourceName, serviceName, siteName, oldStatus, reason, 
                                   dateCreated, dateEffective, 
                                   datetime.utcnow().replace(microsecond = 0).isoformat(' '), 
                                   operatorCode)

    #in any case add a row to present Sites table
    self._addResourcesRow(resourceName, resourceType, serviceName, siteName, status, reason, 
                          dateCreated, dateEffective, dateEnd, operatorCode)
#    resourceRow = "Added %s --- %s --- %s " %(resourceName, siteName, dateEffective)
#    return resAddResourcesRow

#############################################################################

  def _addResourcesRow(self, resourceName, resourceType, serviceName, siteName, status, 
                       reason, dateCreated, dateEffective, dateEnd, operatorCode):
    """ 
    Add a new resource row in Resources table

    :params:
      :attr:`resourceName`: string - name of the resource (DIRAC name)
    
      :attr:`resourceType`: string - ValidResourceType: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`status`: string - ValidStatus: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateCreated`: datetime  or string - date when which the resource row is created

      :attr:`dateEffective`: datetime or string - date from which the resource status 
      is effective

      :attr:`dateEnd`: datetime  or string - date from which the resource status 
      ends to be effective

      :attr:`operatorCode`: string - free
    """

    if not isinstance(dateCreated, basestring):
      dateCreated = dateCreated.isoformat(' ')
    if not isinstance(dateEffective, basestring):
      dateEffective = dateEffective.isoformat(' ')
    if not isinstance(dateEnd, basestring):
      dateEnd = dateEnd.isoformat(' ')
    if status not in ValidStatus:
      raise InvalidStatus, where(self, self._addResourcesRow)

    
    req = "INSERT INTO Resources (ResourceName, ResourceType, ServiceName, SiteName, Status, "
    req = req + "Reason, DateCreated, DateEffective, DateEnd, OperatorCode) "
    req = req + "VALUES ('%s', '%s', '%s', " %(resourceName, resourceType, serviceName)
    req = req + "'%s', '%s', '%s', " %(siteName, status, reason)
    req = req + "'%s', '%s', '%s', '%s');" %(dateCreated, dateEffective, dateEnd, operatorCode)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self._addResourcesRow) + resUpdate['Message']
    

#############################################################################

  def _addResourcesHistoryRow(self, resourceName, serviceName, siteName, status, 
                              reason, dateCreated, dateEffective, dateEnd, operatorCode):
    """
    Add an old resource row in the ResourcesHistory table

    :params:
      :attr:`resourceName`: string - name of the resource (DIRAC name)
    
      :attr:`status`: string - ValidStatus: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateCreated`: datetime  or string - date when which the resource row is created

      :attr:`dateEffective`: datetime  or string - date from which the resource status 
      is effective

      :attr:`dateEnd`: datetime  or string - date from which the resource status ends 
      to be effective

      :attr:`operatorCode`: string - free
    """

    if not isinstance(dateCreated, basestring):
      dateCreated = dateCreated.isoformat(' ')
    if not isinstance(dateEffective, basestring):
      dateEffective = dateEffective.isoformat(' ')
    if not isinstance(dateEnd, basestring):
      dateEnd = dateEnd.isoformat(' ')


    req = "INSERT INTO ResourcesHistory (ResourceName, ServiceName, SiteName,"
    req = req + " Status, Reason, DateCreated," 
    req = req + " DateEffective, DateEnd, OperatorCode) "
    req = req + "VALUES ('%s', '%s', '%s', " % (resourceName, serviceName, siteName)
    req = req + "'%s', '%s', '%s', " %(status, reason, dateCreated)
    req = req + "'%s', '%s', '%s');" %(dateEffective, dateEnd, operatorCode)
    
    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self._addResourcesHistoryRow) + resUpdate['Message']

#############################################################################

  def addResourceType(self, resourceType, description=''):
    """ 
    Add a resource type (CE (different types also), SE, ...)
        
    :params:
      :attr:`serviceType`: string

      :attr:`description`: string, optional
    """

    req = "INSERT INTO ResourceTypes (ResourceType, Description)"
    req = req + "VALUES ('%s', '%s');" % (resourceType, description)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self.addResourceType) + resUpdate['Message']

#############################################################################

  def removeResource(self, resourceName = None, serviceName = None, siteName = None):
    """ 
    Completely remove a resource from the Resources and ResourcesHistory tables
    
    :params:
      :attr:`resourceName`: string
    """

    if serviceName == None and siteName == None:
      req = "DELETE from Resources WHERE ResourceName = '%s';" % (resourceName)
      resDel = self.db._update(req)
      if not resDel['OK']:
        raise RSSDBException, where(self, self.removeResource) + resDel['Message']
  
      req = "DELETE from ResourcesHistory WHERE ResourceName = '%s';" % (resourceName)
      resDel = self.db._update(req)
      if not resDel['OK']:
        raise RSSDBException, where(self, self.removeResource) + resDel['Message']

    else:
      if serviceName == None:
        req = "DELETE from Resources WHERE SiteName = '%s';" % (siteName)
        resDel = self.db._update(req)
        if not resDel['OK']:
          raise RSSDBException, where(self, self.removeResource) + resDel['Message']
    
        req = "DELETE from ResourcesHistory WHERE SiteName = '%s';" % (siteName)
        resDel = self.db._update(req)
        if not resDel['OK']:
          raise RSSDBException, where(self, self.removeResource) + resDel['Message']

      else:
        req = "DELETE from Resources WHERE ServiceName = '%s';" % (serviceName)
        resDel = self.db._update(req)
        if not resDel['OK']:
          raise RSSDBException, where(self, self.removeResource) + resDel['Message']
    
        req = "DELETE from ResourcesHistory WHERE ServiceName = '%s';" % (serviceName)
        resDel = self.db._update(req)
        if not resDel['OK']:
          raise RSSDBException, where(self, self.removeResource) + resDel['Message']

#############################################################################


#############################################################################
# Service functions
#############################################################################

#############################################################################
  
  def setServiceStatus(self, serviceName, status, reason, operatorCode):
    """ 
    Set a Service status, effective from now, with no ending
        
    :params:
      :attr:`serviceName`: string
      
      :attr:`status`: string. Possibilities: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string
      
      :attr:`operatorCode`: string. For the service itself: `RS_SVC`
    """

    gLogger.info("Setting Service %s new status: %s" % (serviceName, status))
    req = "SELECT ServiceType, SiteName FROM Services WHERE ServiceName = '%s' " %(serviceName)
    req = req + "AND DateEffective < UTC_TIMESTAMP();"
    
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.setServiceStatus) + resQuery['Message']
    if not resQuery['Value']:
      return None

    serviceType = resQuery['Value'][0][0]
    siteName = resQuery['Value'][0][1]
  
    self.addOrModifyService(serviceName, serviceType, siteName, status, reason, 
                            datetime.utcnow().replace(microsecond = 0), operatorCode, 
                            datetime(9999, 12, 31, 23, 59, 59))
    
#############################################################################

  def addOrModifyService(self, serviceName, serviceType, siteName, status, reason, 
                         dateEffective, operatorCode, dateEnd):
    """ 
    Add or modify a service to the Services table.
    
    :params:
      :attr:`serviceName`: string - name of the service (DIRAC name)
    
      :attr:`serviceType`: string - ValidServiceType: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`status`: string - ValidStatus: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateEffective`: datetime - date from which the service status is effective

      :attr:`operatorCode`: string - free

      :attr:`dateEnd`: datetime - date from which the service status ends to be effective
    """

    dateCreated = datetime.utcnow().replace(microsecond = 0)
    if dateEffective < dateCreated:
      dateEffective = dateCreated
    if dateEnd < dateEffective:
      raise NotAllowedDate, where(self, self.addOrModifyService)
    if status not in ValidStatus:
      raise InvalidStatus, where(self, self.addOrModifyService)

    #check if the service is already there
    query = "SELECT ServiceName FROM Services WHERE ServiceName='%s'" % serviceName
    resQuery = self.db._query(query)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.addOrModifyService) + resQuery['Message']

    if resQuery['Value']: 
      if dateEffective <= (dateCreated + timedelta(minutes=2)):
        #service modification, effective in less than 2 minutes
        self.setDateEnd('Service', serviceName, dateEffective)
        self.transact2History('Service', serviceName, siteName, dateEffective)
      else:
        self.setDateEnd('Service', serviceName, dateEffective)
    else:
      if status in ('Active', 'Probing'):
        oldStatus = 'Banned'
      else:
        oldStatus = 'Active'
      self._addServiceHistoryRow(serviceName, siteName, oldStatus, reason, dateCreated, 
                                 dateEffective, 
                                 datetime.utcnow().replace(microsecond = 0).isoformat(' '), 
                                 operatorCode)

    #in any case add a row to present Services table
    self._addServiceRow(serviceName, serviceType, siteName, status, reason, 
                        dateCreated, dateEffective, dateEnd, operatorCode)
#    serviceRow = "Added %s --- %s " %(serviceName, dateEffective)
#    return serviceRow

#############################################################################

  def _addServiceRow(self, serviceName, serviceType, siteName, status, reason, 
                     dateCreated, dateEffective, dateEnd, operatorCode):
    """
    Add a new service row in Services table

    :params:
      :attr:`serviceName`: string - name of the service (DIRAC name)
    
      :attr:`serviceType`: string - ValidServiceType: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`status`: string - ValidStatus: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateCreated`: datetime or string - 
      date when which the service row is created

      :attr:`dateEffective`: datetime or string - 
      date from which the service status is effective

      :attr:`dateEnd`: datetime or string - 
      date from which the service status ends to be effective

      :attr:`operatorCode`: string - free
    """
    
    if not isinstance(dateCreated, basestring):
      dateCreated = dateCreated.isoformat(' ')
    if not isinstance(dateEffective, basestring):
      dateEffective = dateEffective.isoformat(' ')
    if not isinstance(dateEnd, basestring):
      dateEnd = dateEnd.isoformat(' ')
    if status not in ValidStatus:
      raise InvalidStatus, where(self, self._addServiceRow)
      

    req = "INSERT INTO Services (ServiceName, ServiceType, SiteName, Status, Reason, "
    req = req + "DateCreated, DateEffective, DateEnd, OperatorCode) "
    req = req + "VALUES ('%s', '%s', '%s', " % (serviceName, serviceType, siteName)
    req = req + "'%s', '%s', '%s', '%s'" %(status, reason, dateCreated, dateEffective)
    req = req + ", '%s', '%s');" %(dateEnd, operatorCode)
    
    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self._addServiceRow) + resUpdate['Message']

#############################################################################

  def _addServiceHistoryRow(self, serviceName, siteName, status, reason, dateCreated, 
                            dateEffective, dateEnd, operatorCode):
    """
    Add an old service row in the ServicesHistory table

    :params:
      :attr:`serviceName`: string - name of the service (DIRAC name)
    
      :attr:`status`: string - ValidStatus: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateCreated`: datetime or string - 
      date when which the service row is created

      :attr:`dateEffective`: datetime or string - 
      date from which the service status is effective

      :attr:`dateEnd`: datetime or string - 
      date from which the service status ends to be effective

      :attr:`operatorCode`: string - free

    """

    if not isinstance(dateCreated, basestring):
      dateCreated = dateCreated.isoformat(' ')
    if not isinstance(dateEffective, basestring):
      dateEffective = dateEffective.isoformat(' ')
    if not isinstance(dateEnd, basestring):
      dateEnd = dateEnd.isoformat(' ')

    req = "INSERT INTO ServicesHistory (ServiceName, SiteName, Status, Reason, DateCreated,"
    req = req + " DateEffective, DateEnd, OperatorCode) "
    req = req + "VALUES ('%s', '%s', '%s', '%s', " % (serviceName, siteName, status, reason)
    req = req + "'%s', '%s', '%s', '%s');" %(dateCreated, dateEffective, dateEnd, operatorCode)
    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self._addServiceHistoryRow) + resUpdate['Message']


#############################################################################

  def removeService(self, serviceName = None, siteName = None):
    """ 
    Completely remove a service from the Services and ServicesHistory tables
        
    :params:
      :attr:`serviceName`: string
    """
    
    if siteName == None: 

      self.removeResource(serviceName = serviceName)
      
      req = "DELETE from Services WHERE ServiceName = '%s';" % (serviceName)
      resDel = self.db._update(req)
      if not resDel['OK']:
        raise RSSDBException, where(self, self.removeService) + resDel['Message']
  
      req = "DELETE from ServicesHistory WHERE ServiceName = '%s';" % (serviceName)
      resDel = self.db._update(req)
      if not resDel['OK']:
        raise RSSDBException, where(self, self.removeService) + resDel['Message']

    else: 

      self.removeResource(siteName = siteName)
      
      req = "DELETE from Services WHERE SiteName = '%s';" % (siteName)
      resDel = self.db._update(req)
      if not resDel['OK']:
        raise RSSDBException, where(self, self.removeService) + resDel['Message']
  
      req = "DELETE from ServicesHistory WHERE SiteName = '%s';" % (siteName)
      resDel = self.db._update(req)
      if not resDel['OK']:
        raise RSSDBException, where(self, self.removeService) + resDel['Message']


#############################################################################

  def setMonitoredToBeChecked(self, monitoreds, granularity, name):
    """ 
    Set LastCheckTime to 0 to monitored(s)
    
    :params:
      :attr:`monitored`: string, or a list of strings where each is a ValidRes:
      which granularity has to be set to be checked
    
      :attr:`granularity`: string, a ValidRes: from who this set comes 
      
      :attr:`name`: string, name of Site or Resource
    """
    
    if type(monitoreds) is not list:
      monitoreds = [monitoreds]
    
    for monitored in monitoreds:
    
      if monitored in ('Site', 'Sites'):
        
        siteName = self.getGeneralName(name, granularity, monitored)
        req = "UPDATE Sites SET LastCheckTime = '00000-00-00 00:00:00'" 
        req = req + " WHERE SiteName  = '%s';" %(siteName)
  
  
      
      elif monitored in ('Service', 'Services'):
      
        if granularity in ('Site', 'Sites'):
          serviceName = self.getMonitoredsList('Service', paramsList = ['ServiceName'], 
                                               siteName = name)
          if type(serviceName) is not list:
            serviceName = [serviceName]
          if serviceName == []:
            raise RSSDBException, where(self, self.setMonitoredToBeChecked) + "No services for site %s" %name
          else:  
            serviceName = [x[0] for x in serviceName]
            serviceName = ','.join(['"'+x.strip()+'"' for x in serviceName])
            req = "UPDATE Services SET LastCheckTime = '00000-00-00 00:00:00'"
            req = req + " WHERE ServiceName IN (%s);" %(serviceName)
        else:
          serviceName = self.getGeneralName(name, granularity, monitored)
          req = "UPDATE Services SET LastCheckTime = '00000-00-00 00:00:00'" 
          req = req + " WHERE ServiceName  = '%s';" %(serviceName)
      
  
      
      elif monitored in ('Resource', 'Resources'):
      
        if granularity in ('Site', 'Sites'):
          resourceName = self.getMonitoredsList('Resource', paramsList = ['ResourceName'], 
                                                siteName = name)
          if type(resourceName) is not list:
            resourceName = [resourceName]
          if resourceName == []:
            raise RSSDBException, where(self, self.setMonitoredToBeChecked) + "No resources for site %s" %name
          else:
            resourceName = [x[0] for x in resourceName]  
            resourceName = ','.join(['"'+x.strip()+'"' for x in resourceName])
            req = "UPDATE Resources SET LastCheckTime = '00000-00-00 00:00:00'"
            req = req + " WHERE ResourceName IN (%s);" %(resourceName)
    
        elif granularity in ('Service', 'Services'):
  
          resourceName = self.getMonitoredsList('Resource', paramsList = ['ResourceName'], 
                                                serviceName = name)
          if type(resourceName) is not list:
            resourceName = [resourceName]
          if resourceName == []:
            raise RSSDBException, where(self, self.setMonitoredToBeChecked) + "No resources for service %s" %name
          else:  
            resourceName = [x[0] for x in resourceName]
            resourceName = ','.join(['"'+x.strip()+'"' for x in resourceName])
            req = "UPDATE Resources SET LastCheckTime = '00000-00-00 00:00:00'"
            req = req + " WHERE ResourceName IN (%s);" %(resourceName)
    
          
        elif granularity in ('StorageElement', 'StorageElements'):
          resourceName = self.getGeneralName(name, granularity, monitored)
          req = "UPDATE Resources SET LastCheckTime = '00000-00-00 00:00:00'" 
          req = req + " WHERE ResourceName  = '%s';" %(resourceName)
      
  
  
      elif monitored in ('StorageElement', 'StorageElements'):
        
        if granularity in ('Site', 'Sites'):
          SEName = self.getMonitoredsList(monitored, paramsList = ['StorageElementName'],
                                          siteName = name)
          if type(SEName) is not list:
            SEName = [SEName]
          if SEName == []:
            pass
#            raise RSSDBException, where(self, self.setMonitoredToBeChecked) + "No storage elements for site %s" %name
          else:
            SEName = [x[0] for x in SEName]
            SEName = ','.join(['"'+x.strip()+'"' for x in SEName])
            req = "UPDATE StorageElements SET LastCheckTime = '00000-00-00 00:00:00'"
            req = req + " WHERE StorageElementName IN (%s);" %(SEName)
        
        elif granularity in ('Resource', 'Resources'):
          SEName = self.getMonitoredsList(monitored, paramsList = ['StorageElementName'],
                                          resourceName = name)
          if type(SEName) is not list:
            SEName = [SEName]
          if SEName == []:
            pass
#            raise RSSDBException, where(self, self.setMonitoredToBeChecked) + "No storage elements for resource %s" %name
          else:  
            SEName = [x[0] for x in SEName]
            SEName = ','.join(['"'+x.strip()+'"' for x in SEName])
            req = "UPDATE StorageElements SET LastCheckTime = '00000-00-00 00:00:00'"
            req = req + " WHERE StorageElementName IN (%s);" %(SEName)
    
        elif granularity in ('Service', 'Services'):
          SEName = self.getMonitoredsList(monitored, paramsList = ['StorageElementName'],
                                          siteName = name.split('@').pop())
          if type(SEName) is not list:
            SEName = [SEName]
          if SEName == []:
            pass
#            raise RSSDBException, where(self, self.setMonitoredToBeChecked) + "No storage elements for service %s" %name
          else:  
            SEName = [x[0] for x in SEName]
            SEName = ','.join(['"'+x.strip()+'"' for x in SEName])
            req = "UPDATE StorageElements SET LastCheckTime = '00000-00-00 00:00:00'"
            req = req + " WHERE StorageElementName IN (%s);" %(SEName)
    
         
      resUpdate = self.db._update(req)
      
      if not resUpdate['OK']:
        raise RSSDBException, where(self, self.setMonitoredToBeChecked) + resUpdate['Message']


#############################################################################

  def getResourceStats(self, granularity, name):
    """ 
    Returns simple statistics of active, probing and banned resources of a site or service;
        
    :params:
      :attr:`granularity`: string: site or service
      
      :attr:`name`: string - name of site or service
    
    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """
    
    res = {'Active':0, 'Probing':0, 'Banned':0, 'Total':0}
    
    if granularity in ('Site', 'Sites'): 
      req = "SELECT Status, COUNT(*)" 
      req = req + " FROM Resources WHERE SiteName = '%s' GROUP BY Status" %name
    elif granularity in ('Service', 'Services'): 
      req = "SELECT Status, COUNT(*)" 
      req = req + " FROM Resources WHERE ServiceName = '%s' GROUP BY Status" %name
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getResourceStats) + resQuery['Message']
    else:
      for x in resQuery['Value']:
        res[x[0]] = int(x[1])
        
    res['Total'] = sum(res.values())
    
    return res

#############################################################################

  def getStorageElementsStats(self, granularity, name):
    """ 
    Returns simple statistics of active, probing and banned resources of a site or resource;
        
    :params:
      :attr:`granularity`: string: site or resource
      
      :attr:`name`: string - name of site or resource
    
    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """
    
    res = {'Active':0, 'Probing':0, 'Banned':0, 'Total':0}
    
    if granularity in ('Site', 'Sites'): 
      req = "SELECT Status, COUNT(*)" 
      req = req + " FROM StorageElements WHERE SiteName = '%s' GROUP BY Status" %name
    elif granularity in ('Resource', 'Resources'): 
      req = "SELECT Status, COUNT(*)" 
      req = req + " FROM StorageElements WHERE ResourceName = '%s' GROUP BY Status" %name
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getStorageElementsStats) + resQuery['Message']
    else:
      for x in resQuery['Value']:
        res[x[0]] = int(x[1])
        
    res['Total'] = sum(res.values())
    
    return res

#############################################################################

#############################################################################
# StorageElement functions
#############################################################################

#############################################################################

  def setStorageElementStatus(self, storageElementName, status, reason, operatorCode):
    """ 
    Set a StorageElement status, effective from now, with no ending
        
    :params:
      :attr:`storageElementName`: string
  
      :attr:`status`: string. Possibilities: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
  
      :attr:`reason`: string
  
      :attr:`operatorCode`: string. For the service itself: `RS_SVC`
    """

    gLogger.info("Setting StorageElement %s new status: %s" % (storageElementName, status))
    req = "SELECT ResourceName, SiteName FROM StorageElements WHERE StorageElementName = " 
    req = req + "'%s' AND DateEffective < UTC_TIMESTAMP();" %(storageElementName)
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.setStorageElementStatus) + resQuery['Message']
    if not resQuery['Value']:
      return None

    resourceName = resQuery['Value'][0][0]
    siteName = resQuery['Value'][0][1]
  
    self.addOrModifyStorageElement(storageElementName, resourceName, siteName, status, 
                                   reason, datetime.utcnow().replace(microsecond = 0), 
                                   operatorCode, datetime(9999, 12, 31, 23, 59, 59))
    
#############################################################################

  def addOrModifyStorageElement(self, storageElementName, resourceName, siteName, 
                                status, reason, dateEffective, operatorCode, dateEnd):
    """ 
    Add or modify a storageElement to the StorageElements table.
    
    :params:
      :attr:`storageElementName`: string - name of the storageElement
    
      :attr:`resourceName`: string - name of the node
    
      :attr:`SiteName`: string - name of the site (DIRAC name)
    
      :attr:`status`: string - ValidStatus: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateEffective`: datetime - 
      date from which the storageElement status is effective

      :attr:`operatorCode`: string - free

      :attr:`dateEnd`: datetime - 
      date from which the storageElement status ends to be effective
    """

    dateCreated = datetime.utcnow().replace(microsecond = 0)
    if dateEffective < dateCreated:
      dateEffective = dateCreated
    if dateEnd < dateEffective:
      raise NotAllowedDate, where(self, self.addOrModifyStorageElement)
    if status not in ValidStatus:
      raise InvalidStatus, where(self, self.addOrModifyStorageElement)

    #check if the storageElement is already there
    query = "SELECT StorageElementName FROM StorageElements WHERE " 
    query = query + "StorageElementName='%s'" % storageElementName
    resQuery = self.db._query(query)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.addOrModifyStorageElement) + resQuery['Message']

    if resQuery['Value']: 
      if dateEffective <= (dateCreated + timedelta(minutes=2)):
        #storageElement modification, effective in less than 2 minutes
        self.setDateEnd('StorageElement', storageElementName, dateEffective)
        self.transact2History('StorageElement', storageElementName, resourceName, 
                              siteName, dateEffective)
      else:
        self.setDateEnd('StorageElement', storageElementName, dateEffective)
    else:
      if status in ('Active', 'Probing'):
        oldStatus = 'Banned'
      else:
        oldStatus = 'Active'
      self._addStorageElementHistoryRow(storageElementName, resourceName, siteName, 
                                        oldStatus, reason, dateCreated, dateEffective, 
                                        datetime.utcnow().replace(microsecond = 0).isoformat(' '), 
                                        operatorCode)

    #in any case add a row to present StorageElements table
    self._addStorageElementRow(storageElementName, resourceName, siteName, status, 
                               reason, dateCreated, dateEffective, dateEnd, operatorCode)
#    storageElementRow = "Added %s --- %s " %(storageElementName, dateEffective)
#    return storageElementRow

#############################################################################

  def _addStorageElementRow(self, storageElementName, resourceName, siteName, status, 
                            reason, dateCreated, dateEffective, dateEnd, operatorCode):
    """
    Add a new storageElement row in StorageElements table

    :params:
      :attr:`storageElementName`: string - name of the storageElement
    
      :attr:`resourceName`: string - name of the resource
  
      :attr:`siteName`: string - name of the site (DIRAC name)
      
      :attr:`status`: string - ValidStatus: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateCreated`: datetime or string - date when which the storageElement 
      row is created

      :attr:`dateEffective`: datetime or string - date from which the storageElement 
      status is effective

      :attr:`dateEnd`: datetime or string - date from which the storageElement status 
      ends to be effective

      :attr:`operatorCode`: string - free
    """

    if not isinstance(dateCreated, basestring):
      dateCreated = dateCreated.isoformat(' ')
    if not isinstance(dateEffective, basestring):
      dateEffective = dateEffective.isoformat(' ')
    if not isinstance(dateEnd, basestring):
      dateEnd = dateEnd.isoformat(' ')
    if status not in ValidStatus:
      raise InvalidStatus, where(self, self._addStorageElementRow)
      

    req = "INSERT INTO StorageElements (StorageElementName, ResourceName, SiteName, "
    req = req + "Status, Reason, DateCreated, DateEffective, DateEnd, OperatorCode) "
    req = req + "VALUES ('%s', '%s', '%s', " %(storageElementName, resourceName, siteName)
    req = req + "'%s', '%s', '%s', " %(status, reason, dateCreated, )
    req = req + "'%s', '%s', '%s');" %(dateEffective, dateEnd, operatorCode)
    
    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self._addStorageElementRow) + resUpdate['Message']

#############################################################################

  def _addStorageElementHistoryRow(self, storageElementName, resourceName, siteName, status, 
                                   reason, dateCreated, dateEffective, dateEnd, operatorCode):
    """ 
    Add an old storageElement row in the StorageElementsHistory table

    :params:
      :attr:`storageElementName`: string - name of the storageElement
    
      :attr:`storageElementName`: string - name of the resource
    
      :attr:`storageElementName`: string - name of the site (DIRAC name)
    
      :attr:`status`: string - ValidStatus: 
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateCreated`: datetime or string - 
      date when which the storageElement row is created

      :attr:`dateEffective`: datetime or string - 
      date from which the storageElement status is effective

      :attr:`dateEnd`: datetime or string - 
      date from which the storageElement status ends to be effective

      :attr:`operatorCode`: string - free
    """

    if not isinstance(dateCreated, basestring):
      dateCreated = dateCreated.isoformat(' ')
    if not isinstance(dateEffective, basestring):
      dateEffective = dateEffective.isoformat(' ')
    if not isinstance(dateEnd, basestring):
      dateEnd = dateEnd.isoformat(' ')

    req = "INSERT INTO StorageElementsHistory (StorageElementName, ResourceName, SiteName, "
    req = req + "Status, Reason, DateCreated, DateEffective, DateEnd, OperatorCode) "
    req = req + "VALUES ('%s', '%s', '%s'," % (storageElementName, resourceName, siteName)
    req = req + " '%s', '%s', '%s', '%s'," %(status, reason, dateCreated, dateEffective)
    req = req + " '%s', '%s');" %(dateEnd, operatorCode)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self._addStorageElementHistoryRow) + resUpdate['Message']


#############################################################################

  def removeStorageElement(self, storageElementName = None, resourceName = None, 
                           siteName = None):
    """ 
    Completely remove a storageElement from the StorageElements 
    and StorageElementsHistory tables
    
    :params:
      :attr:`storageElementName`: string

      :attr:`resourceName`: string

      :attr:`siteName`: string
    """
    
    if resourceName == None and siteName == None:
      req = "DELETE from StorageElements " 
      req = req + "WHERE StorageElementName = '%s';" % (storageElementName)
      resDel = self.db._update(req)
      if not resDel['OK']:
        raise RSSDBException, where(self, self.removeStorageElement) + resDel['Message']
  
      req = "DELETE from StorageElementsHistory" 
      req = req + " WHERE StorageElementName = '%s';" % (storageElementName)
      resDel = self.db._update(req)
      if not resDel['OK']:
        raise RSSDBException, where(self, self.removeStorageElement) + resDel['Message']

    else:
      if resourceName == None:
        req = "DELETE from StorageElements WHERE SiteName = '%s';" % (siteName)
        resDel = self.db._update(req)
        if not resDel['OK']:
          raise RSSDBException, where(self, self.removeStorageElement) + resDel['Message']
    
        req = "DELETE from StorageElementsHistory WHERE SiteName = '%s';" % (siteName)
        resDel = self.db._update(req)
        if not resDel['OK']:
          raise RSSDBException, where(self, self.removeStorageElement) + resDel['Message']

      else:
        req = "DELETE from StorageElements WHERE ResourceName = '%s';" % (resourceName)
        resDel = self.db._update(req)
        if not resDel['OK']:
          raise RSSDBException, where(self, self.removeStorageElement) + resDel['Message']
    
        req = "DELETE from StorageElementsHistory WHERE ResourceName = '%s';" % (resourceName)
        resDel = self.db._update(req)
        if not resDel['OK']:
          raise RSSDBException, where(self, self.removeStorageElement) + resDel['Message']

#############################################################################

#############################################################################
# GENERAL functions
#############################################################################

#############################################################################

  def removeRow(self, granularity, name, dateEffective):
    """ 
    Remove a row from one of the tables
    
    :params:
      :attr:`name`: string
      
      :attr:`dateEffective`: string or datetime
    """

    if not isinstance(dateEffective, basestring):
      dateEffective = dateEffective.isoformat(' ')

    if granularity in ('Site', 'Sites'):
      DBname = 'SiteName'
      DBtable = 'Sites'
    elif granularity in ('Service', 'Services'):
      DBname = 'ServiceName'
      DBtable = 'Services'
    elif granularity in ('Resource', 'Resources'):
      DBname = 'ResourceName'
      DBtable = 'Resources'
    elif granularity in ('StorageElement', 'StorageElements'):
      DBname = 'StorageElementName'
      DBtable = 'StorageElements'
    else:
      raise InvalidRes, where(self, self.removeRow)
    
    req = "DELETE from %s WHERE %s = '%s' AND " % (DBtable, DBname, name)
    req = req + "DateEffective = '%s';" %(dateEffective)
    resDel = self.db._update(req)
    if not resDel['OK']:
      raise RSSDBException, where(self, self.removeRow) + resDel['Message']

#############################################################################

  def getTypesList(self, granularity, type=None):
    """ 
    Get list of site, resource, service types with description
    
    :Params:
      :attr:`type`: string, the type.
    """
    
    if granularity in ('Site', 'Sites'):
      DBtype = 'SiteType'
      DBtable = 'SiteTypes'
    elif granularity in ('Service', 'Services'):
      DBtype = 'ServiceType'
      DBtable = 'ServiceTypes'
    elif granularity in ('Resource', 'Resources'):
      DBtype = 'ResourceType'
      DBtable = 'ResourceTypes'
    else:
      raise InvalidRes, where(self, self.getTypesList)

    if type == None:
      req = "SELECT %s FROM %s" %(DBtype, DBtable)
    else:
      req = "SELECT %s, Description FROM %s " %(DBtype, DBtable)
      req = req + "WHERE %s = '%s'" % (DBtype, type)

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getTypesList) + resQuery['Message']
    if not resQuery['Value']:
      return []
    typeList = []
    typeList = [ x[0] for x in resQuery['Value']]
    return typeList

#############################################################################

  def removeType(self, granularity, type):
    """ 
    Remove a type from the DB
    
    :params:
      :attr:`type`: string, a type (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`)
    """

    if granularity in ('Site', 'Sites'):
      DBtype = 'SiteType'
      DBtable = 'SiteTypes'
    elif granularity in ('Service', 'Services'):
      DBtype = 'ServiceType'
      DBtable = 'ServiceTypes'
    elif granularity in ('Resource', 'Resources'):
      DBtype = 'ResourceType'
      DBtable = 'ResourceTypes'
    else:
      raise InvalidRes, where(self, self.removeType)
      
    
    req = "DELETE from %s WHERE %s = '%s';" % (DBtable, DBtype, type)
    resDel = self.db._update(req)
    if not resDel['OK']:
      raise RSSDBException, where(self, self.removeType) + resDel['Message']

#############################################################################
  def getStatusList(self):
    """ 
    Get list of status with no descriptions.
    """

    req = "SELECT Status from Status"

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getStatusList) + resQuery['Message']
    if not resQuery['Value']:
      return []
    typeList = []
    typeList = [ x[0] for x in resQuery['Value']]
    return typeList

#############################################################################

  def getGeneralName(self, name, from_g, to_g):
    """ 
    Get name of res, of granularity `from_g`, to the name of res with granularity `to_g`
      
    For a StorageElement, get the Site name, or the Service name, or the Resource name.
    For a Resource, get the Site name, or the Service name.
    For a Service name, get the Site name
    
    :params:
      :attr:`name`: a string with a name
      
      :attr:`from_g`: a string with a valid granularity 
      (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`)
      
      :attr:`to_g`: a string with a valid granularity 
      (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`)
      
    :return:
      a string with the resulting name
    """

    if from_g in ('Service', 'Services'):
      DBtable = 'Services'
      DBnameW = 'ServiceName'
    elif from_g in ('Resource', 'Resources'):
      DBtable = 'Resources'
      DBnameW = 'ResourceName'
    elif from_g in ('StorageElement', 'StorageElements'):
      DBtable = 'StorageElements'
      DBnameW = 'StorageElementName'
    
    if to_g in ('Site', 'Sites'):
      DBname = 'SiteName'
    elif to_g in ('Service', 'Services'):
      DBname = 'ServiceName'
    elif to_g in ('Resource', 'Resources'):
      DBname = 'ResourceName'
      
    if from_g in ('StorageElement', 'StorageElements') and to_g in ('Service', 'Services'):
      req = "SELECT SiteName FROM %s WHERE %s = '%s'" %(DBtable, DBnameW, name.split('@').pop())
    else:
      req = "SELECT %s FROM %s WHERE %s = '%s'" %(DBname, DBtable, DBnameW, name)

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getGeneralName) + resQuery['Message']
    if not resQuery['Value']:
      return []
    newName = resQuery['Value'][0][0]
    if from_g in ('StorageElement', 'StorageElements') and to_g in ('Service', 'Services'):
      return 'Storage@'+newName
    else:
      return newName
    
  
#############################################################################

  def getEndings(self, table):
    """ get list of rows from table(s) that end to be effective
    """
    
    #getting primary key for table
    req = "SELECT k.column_name FROM information_schema.table_constraints t "
    req = req + "JOIN information_schema.key_column_usage k "
    req = req + "USING(constraint_name,table_schema,table_name) "
    req = req + "WHERE t.constraint_type='PRIMARY KEY' "
    req = req + "AND t.table_schema='ResourceStatusDB' AND t.table_name='%s';" %(table)
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getEndings)
    else:
      PKList = [ x[0] for x in resQuery['Value']]
      if len(PKList) == 1:
        req = "SELECT %s FROM %s " %(PKList[0], table)
        req = req + "WHERE TIMESTAMP(DateEnd) < UTC_TIMESTAMP();"
      elif len(PKList) == 2:
        req = "SELECT %s, %s FROM %s " %(PKList[0], PKList[1], table)
        req = req + "WHERE TIMESTAMP(DateEnd) < UTC_TIMESTAMP();"
      elif len(PKList) == 3:
        req = "SELECT %s, %s, %s FROM %s " %(PKList[0], PKList[1], PKList[2], table)
        req = req + "WHERE TIMESTAMP(DateEnd) < UTC_TIMESTAMP();"
      resQuery = self.db._query(req)
      if not resQuery['OK']:
        raise RSSDBException, where(self, self.getEndings) + resQuery['Message']
      else:
        list = []
        list = [ int(x[0]) for x in resQuery['Value']]
        return list


#############################################################################

  def getPeriods(self, granularity, name, status, hours = None, days = None):
    """ 
    Get list of periods of times when a ValidRes was in ValidStatus 
    (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`) 
    for a total of `hours` or `days`.
    :params:
      :attr:`granularity`: string - ValidRes
      
      :attr:`name`: string - name
      
      :attr:`status`: string - ValidStatus
      
      :attr:`hours`: integer
      
      :attr:`days`: integer
      
    :return:
      list of periods of time as tuples in string format
    """
    
    if days is not None:
      hours = 24*days
    
    hours = timedelta(hours = hours)
    
    if granularity in ('Site', 'Sites'):
      req = "SELECT DateEffective FROM Sites WHERE SiteName = '%s' AND DateEffective < UTC_TIMESTAMP() AND Status = '%s'" %(name, status)
    elif granularity in ('Service', 'Services'):
      req = "SELECT DateEffective FROM Services WHERE ServiceName = '%s' AND DateEffective < UTC_TIMESTAMP() AND Status = '%s'" %(name, status)
    elif granularity in ('Resource', 'Resources'):
      req = "SELECT DateEffective FROM Resources WHERE ResourceName = '%s' AND DateEffective < UTC_TIMESTAMP() AND Status = '%s'" %(name, status)
    elif granularity in ('StorageElement', 'StorageElements'):
      req = "SELECT DateEffective FROM StorageElements WHERE StorageElementName = '%s' AND DateEffective < UTC_TIMESTAMP() AND Status = '%s'" %(name, status)
    
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getPeriods) + resQuery['Message']
    else:
      if resQuery['Value'] == '':
        return None
      elif resQuery['Value'] == ():
        #actual status is not what was requested
        periods = []
        timeInStatus = timedelta(0)
      else:
        #actual status is what was requested
        effFrom = resQuery['Value'][0][0]
        timeInStatus = datetime.utcnow().replace(microsecond = 0) - effFrom
   
        if timeInStatus > hours:
          return [((datetime.utcnow().replace(microsecond = 0)-hours).isoformat(' '), datetime.utcnow().replace(microsecond = 0).isoformat(' '))] 
        
        periods = [(effFrom.isoformat(' '), datetime.utcnow().replace(microsecond = 0).isoformat(' '))]
   
      if granularity in ('Site', 'Sites'):
        req = "SELECT DateEffective, DateEnd FROM SitesHistory WHERE " 
        req = req + "SiteName = '%s' AND Status = '%s'" %(name, status)
      elif granularity in ('Resource', 'Resources'):
        req = "SELECT DateEffective, DateEnd FROM ResourcesHistory WHERE "
        req = req + "ResourceName = '%s' AND Status = '%s'" %(name, status)
      elif granularity in ('Service', 'Services'):
        req = "SELECT DateEffective, DateEnd FROM ServicesHistory WHERE " 
        req = req + "ServiceName = '%s' AND Status = '%s'" %(name, status)
      elif granularity in ('StorageElement', 'StorageElements'):
        req = "SELECT DateEffective, DateEnd FROM StorageElementsHistory "
        req = req + "WHERE StorageElementName = '%s' AND Status = '%s'" %(name, status)
      
      resQuery = self.db._query(req)
      if not resQuery['OK']:
        raise RSSDBException, where(self, self.getPeriods) + resQuery['Message']
      else:
        for x in range(len(resQuery['Value'])):
          i = len(resQuery['Value']) - x
          effFrom = resQuery['Value'][i-1][0]
          effTo = resQuery['Value'][i-1][1]
          oldTimeInStatus = timeInStatus
          timeInStatus = timeInStatus + (effTo - effFrom)
          if timeInStatus > hours:
            periods.append(((effTo - (hours - oldTimeInStatus)).isoformat(' '), 
                            effTo.isoformat(' ')))
            return periods
          else:
            periods.append((effFrom.isoformat(' '), effTo.isoformat(' ')))
      return periods

#############################################################################

  def getTablesWithHistory(self):
    """ 
    Get list of tables with associated an history table
    """

    tablesList=[]
    req = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " 
    req = req + "WHERE TABLE_SCHEMA = 'ResourceStatusDB' AND TABLE_NAME LIKE \"%History\"";
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getTablesWithHistory) + resQuery['Message']
    else:
      HistoryTablesList = [ x[0] for x in resQuery['Value']]
      for x in HistoryTablesList:
        tablesList.append(x[0:len(x)-7])
      return tablesList

#############################################################################

  def getServiceStats(self, siteName):
    """ 
    Returns simple statistics of active, probing and banned services of a site;
        
    :params:
      :attr:`siteName`: string - a site name
    
    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """
    
    res = {'Active':0, 'Probing':0, 'Banned':0, 'Total':0}
    
    req = "SELECT Status, COUNT(*) FROM Services WHERE SiteName = '%s' GROUP BY Status" %siteName
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getServiceStats) + resQuery['Message']
    else:
      for x in resQuery['Value']:
        res[x[0]] = int(x[1])
        
    res['Total'] = sum(res.values())
    
    return res

#############################################################################


  def transact2History(self, *args):
    """ 
    Transact a row from a Sites or Service or Resources table to history.
    Does not do a real transaction in terms of DB.
    
    :params:
      :attr: a tuple with info on what to transact
      
    Examples of possible way to call it:
    
    >>> trasact2History(('Site', 'LCG.CERN.ch', 
          datetime.utcnow().replace(microsecond = 0).isoformat(' ')) 
          - the date is the DateEffective parameter
        trasact2History(('Site', 523)) - the number if the SiteID
        trasact2History(('Service', 'Computing@LCG.CERN.ch', 'LCG.CERN.ch', 
          datetime.utcnow().replace(microsecond = 0).isoformat(' ')) 
          - the date is the DateEffective parameter
        trasact2History(('Service', 523)) - the number if the ServiceID
        trasact2History(('Resource', 'srm-lhcb.cern.ch', 'Computing@LCG.CERN.ch', 
          'LCG.CERN.ch', datetime.utcnow().replace(microsecond = 0).isoformat(' ')) 
          - the date is the DateEffective parameter
        trasact2History(('Resource', 523)) - the number if the ResourceID
        trasact2History(('StorageElement', 'CERN-RAW', 'srm-lhcb.cern.ch', 
          'LCG.CERN.ch', datetime.utcnow().replace(microsecond = 0).isoformat(' ')) 
          - the date is the DateEffective parameter
        trasact2History(('StorageElement', 523)) - the number if the StorageElementID
    """
    
    #get table (in args[0]) columns 
#    req = "select COLUMN_NAME from Sitesinformation_schema.columns where TABLE_NAME = '%s'" %(args[0])
#    resQuery = self.db._query(req)
#    if not resQuery['OK']:
#      return S_ERROR('Failed to query for table columns')
#    if not resQuery['Value']:
#      return S_OK('No columns')
#    
#    req = "SELECT %s, Description, DateCreated, "
#    req = req + "DateEffective, OperatorCode from Sites "
#    req = req + "WHERE (SiteName='%s' and DateEffective < '%s');" % (args[1], args[2])
    
    if args[0] in ('Site', 'Sites'):
      #get row to be put in history Sites table
      if len(args) == 3:
        req = "SELECT Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, OperatorCode from Sites "
        req = req + "WHERE (SiteName='%s' AND DateEffective < '%s');" % (args[1], args[2])
        resQuery = self.db._query(req)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.transact2History) + resQuery['Message']
        if not resQuery['Value']:
          return None
        oldStatus = resQuery['Value'][0][0]
        oldReason = resQuery['Value'][0][1]
        oldDateCreated = resQuery['Value'][0][2]
        oldDateEffective = resQuery['Value'][0][3]
        oldDateEnd = resQuery['Value'][0][4]
        oldOperatorCode = resQuery['Value'][0][5]

        #start "transaction" to history -- should be better to use a real transaction
        self._addSiteHistoryRow(args[1], oldStatus, oldReason, oldDateCreated, 
                                oldDateEffective, oldDateEnd, oldOperatorCode)
        self.removeRow(args[0], args[1], oldDateEffective)

      elif len(args) == 2:
        req = "SELECT SiteName, Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, OperatorCode from Sites "
        req = req + "WHERE (SiteID='%s');" % (args[1])
        resQuery = self.db._query(req)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.transact2History) + resQuery['Message']
        if not resQuery['Value']:
          return None
        siteName = resQuery['Value'][0][0]
        oldStatus = resQuery['Value'][0][1]
        oldReason = resQuery['Value'][0][2]
        oldDateCreated = resQuery['Value'][0][3]
        oldDateEffective = resQuery['Value'][0][4]
        oldDateEnd = resQuery['Value'][0][5]
        oldOperatorCode = resQuery['Value'][0][6]

        #start "transaction" to history -- should be better to use a real transaction
        self._addSiteHistoryRow(siteName, oldStatus, oldReason, oldDateCreated, 
                                oldDateEffective, oldDateEnd, oldOperatorCode)
        self.removeRow(args[0], siteName, oldDateEffective)


    if args[0] in ('Service', 'Services'):
      #get row to be put in history Services table
      if len(args) == 4:
        req = "SELECT Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, OperatorCode from Services "
        req = req + "WHERE (ServiceName='%s' AND DateEffective < '%s');" % (args[1], args[2])
        resQuery = self.db._query(req)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.transact2History) + resQuery['Message']
        if not resQuery['Value']:
          return None
        oldStatus = resQuery['Value'][0][0]
        oldReason = resQuery['Value'][0][1]
        oldDateCreated = resQuery['Value'][0][2]
        oldDateEffective = resQuery['Value'][0][3]
        oldDateEnd = resQuery['Value'][0][4]
        oldOperatorCode = resQuery['Value'][0][5]

        #start "transaction" to history -- should be better to use a real transaction
        self._addServiceHistoryRow(args[1], args[2], oldStatus, oldReason, 
                                   oldDateCreated, oldDateEffective, oldDateEnd, 
                                   oldOperatorCode)
        self.removeRow(args[0], args[1], oldDateEffective)

      elif len(args) == 2:
        req = "SELECT ServiceName, SiteName, Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, OperatorCode from Services "
        req = req + "WHERE (ServiceID='%s');" % (args[1])
        resQuery = self.db._query(req)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.transact2History) + resQuery['Message']
        if not resQuery['Value']:
          return None
        serviceName = resQuery['Value'][0][0]
        siteName = resQuery['Value'][0][1]
        oldStatus = resQuery['Value'][0][2]
        oldReason = resQuery['Value'][0][3]
        oldDateCreated = resQuery['Value'][0][4]
        oldDateEffective = resQuery['Value'][0][5]
        oldDateEnd = resQuery['Value'][0][6]
        oldOperatorCode = resQuery['Value'][0][7]

        #start "transaction" to history -- should be better to use a real transaction
        self._addServiceHistoryRow(serviceName, siteName, oldStatus, oldReason, 
                                   oldDateCreated, oldDateEffective, oldDateEnd, 
                                   oldOperatorCode)
        self.removeRow(args[0], serviceName, oldDateEffective)

        
    if args[0] in ('Resource', 'Resources'):
      if len(args) == 5:
        req = "SELECT Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, OperatorCode from Resources "
        req = req + "WHERE (ResourceName='%s' AND DateEffective < '%s' );" % (args[1], args[4])
        resQuery = self.db._query(req)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.transact2History) + resQuery['Message']
        if not resQuery['Value']:
          return None
        oldStatus = resQuery['Value'][0][0]
        oldReason = resQuery['Value'][0][1]
        oldDateCreated = resQuery['Value'][0][2]
        oldDateEffective = resQuery['Value'][0][3]
        oldDateEnd = resQuery['Value'][0][4]
        oldOperatorCode = resQuery['Value'][0][5]

        self._addResourcesHistoryRow(args[1], args[2], args[3], oldStatus, oldReason, 
                                     oldDateCreated, oldDateEffective, oldDateEnd, 
                                     oldOperatorCode)
        self.removeRow(args[0], args[1], oldDateEffective)
        
      elif len(args) == 2:
        req = "SELECT ResourceName, ServiceName, SiteName, Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, OperatorCode from Resources "
        req = req + "WHERE (ResourceID='%s');" % (args[1])
        resQuery = self.db._query(req)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.transact2History) + resQuery['Message']
        if not resQuery['Value']:
          return None
        resourceName = resQuery['Value'][0][0]
        serviceName = resQuery['Value'][0][1]
        siteName = resQuery['Value'][0][2]
        oldStatus = resQuery['Value'][0][3]
        oldReason = resQuery['Value'][0][4]
        oldDateCreated = resQuery['Value'][0][5]
        oldDateEffective = resQuery['Value'][0][6]
        oldDateEnd = resQuery['Value'][0][7]
        oldOperatorCode = resQuery['Value'][0][8]
        
        #start "transaction" to history -- should be better to use a real transaction
        self._addResourcesHistoryRow(resourceName, serviceName, siteName, oldStatus, 
                                     oldReason, oldDateCreated, oldDateEffective, 
                                     oldDateEnd, oldOperatorCode)
        self.removeRow(args[0], resourceName, oldDateEffective)

    if args[0] in ('StorageElement', 'StorageElements'):
      if len(args) == 5:
        req = "SELECT Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, OperatorCode from StorageElements "
        req = req + "WHERE (StorageElementName='%s' AND DateEffective < '%s' );" % (args[1], args[4])
        resQuery = self.db._query(req)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.transact2History) + resQuery['Message']
        if not resQuery['Value']:
          return None
        oldStatus = resQuery['Value'][0][0]
        oldReason = resQuery['Value'][0][1]
        oldDateCreated = resQuery['Value'][0][2]
        oldDateEffective = resQuery['Value'][0][3]
        oldDateEnd = resQuery['Value'][0][4]
        oldOperatorCode = resQuery['Value'][0][5]

        self._addStorageElementHistoryRow(args[1], args[2], args[3], oldStatus, oldReason, 
                                          oldDateCreated, oldDateEffective, oldDateEnd, 
                                          oldOperatorCode)
        self.removeRow(args[0], args[1], oldDateEffective)
        
      elif len(args) == 2:
        req = "SELECT StorageElementName, ResourceName, SiteName, Status, Reason, "
        req = req + "DateCreated, DateEffective, DateEnd, OperatorCode from StorageElements "
        req = req + "WHERE (StorageElementID='%s');" % (args[1])
        resQuery = self.db._query(req)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.transact2History) + resQuery['Message']
        if not resQuery['Value']:
          return None
        storageElementName = resQuery['Value'][0][0]
        resourceName = resQuery['Value'][0][1]
        siteName = resQuery['Value'][0][2]
        oldStatus = resQuery['Value'][0][3]
        oldReason = resQuery['Value'][0][4]
        oldDateCreated = resQuery['Value'][0][5]
        oldDateEffective = resQuery['Value'][0][6]
        oldDateEnd = resQuery['Value'][0][7]
        oldOperatorCode = resQuery['Value'][0][8]
        
        #start "transaction" to history -- should be better to use a real transaction
        self._addStorageElementHistoryRow(storageElementName, resourceName, siteName, 
                                          oldStatus, oldReason, oldDateCreated, 
                                          oldDateEffective, oldDateEnd, oldOperatorCode)
        self.removeRow(args[0], storageElementName, oldDateEffective)


#############################################################################

  def setDateEnd(self, granularity, name, dateEffective):
    """ 
    Set date end, for a Site or for a Resource
    
    :params:
      :attr:`granularity`: a ValidRes. 
      
      :attr:`name`: string, the name of the ValidRes
        
      :attr:`dateEffective`: a datetime
    """
    
    if granularity in ('Site', 'Sites'):
      DBtable = 'Sites'
      DBname = 'SiteName'
    elif granularity in ('Service', 'Services'):
      DBtable = 'Services'
      DBname = 'ServiceName'
    elif granularity in ('Resource', 'Resources'):
      DBtable = 'Resources'
      DBname = 'ResourceName'
    elif granularity in ('StorageElement', 'StorageElements'):
      DBtable = 'StorageElements'
      DBname = 'StorageElementName'
    else:
      raise InvalidRes, where(self, self.setDateEnd)
      
    query = "UPDATE %s SET DateEnd = '%s' " % (DBtable, dateEffective)
    query = query + "WHERE %s = '%s' AND DateEffective < '%s'" %(DBname, name, dateEffective)
    resUpdate = self.db._update(query)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self.setDateEnd) + resUpdate['Message']



#############################################################################

  #usata solo nell'handler
  def addStatus(self, status, description=''):
    """ 
    Add a status.
    
    :params:
      :attr:`status`: string - a new status
      
      :attr:`description`: string - optional description
    """

    req = "INSERT INTO Status (Status, Description)"
    req = req + "VALUES ('%s', '%s');" % (status, description)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self.addStatus) + resUpdate['Message']

#############################################################################

  #usata solo nell'handler
  def removeStatus(self, status):
    """ 
    Remove a status from the Status table.
    
    :params:
      :attr:`status`: string - status
    """

    req = "DELETE from Status WHERE Status = '%s';" % (status)
    resDel = self.db._update(req)
    if not resDel['OK']:
      raise RSSDBException, where(self, self.removeStatus) + resDel['Message']

#############################################################################

  def unique(self, table, ID):
    """ 
    Check if a ValidRes is unique.
    
    :params:
      :attr:`table`: string of the table name
      
      :attr:`ID`: integer
    """
    
    if table == 'Sites':
      DBname = 'SiteName'
      DBid = 'SiteID'
    elif table == 'Resources':
      DBname = 'ResourceName'
      DBid = 'ResourceID'
    elif table == 'Services':
      DBname = 'ServiceName'
      DBid = 'ServiceID'
    elif table == 'StorageElements':
      DBname = 'StorageElementName'
      DBid = 'StorageElementID'

    req = "SELECT COUNT(*) FROM %s WHERE %s = (SELECT %s " %(table, DBname, DBname)
    req = req + " FROM %s WHERE %s = '%d');" % (table, DBid, ID)

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.unique) + resQuery['Message']
    else:
      n = int(resQuery['Value'][0][0])
      if n == 1 :
        return True
      else:
        return False
      
#############################################################################

  def syncWithCS(self, a, b):
    """ 
    Syncronize DB content with CS content. Params are fake (just to be invoked by 
    meth:`DIRAC.gConfig.addListenerToNewVersionEvent`) during initialization of 
    :mod:`DIRAC.ResourceStatusSystem.Service.ResourceStatusHandler` 
    """ 
    
    gLogger.info("!!! Sync DB content with CS content !!!")
    
    from DIRAC.Core.Utilities.SiteCEMapping import getSiteCEMapping
    from DIRAC.Core.Utilities.SiteSEMapping import getSiteSEMapping
    from DIRAC import S_OK, S_ERROR
    import time
    import socket
    
    T0List = []
    T1List = []
    T2List = []
    
    CEList = []
    SerCompList = []
    SerStorList = []
    SEList = []
    SENodeList = []
    LFCNodeList = []
    seServiceSite = []
    
    sitesList = gConfig.getSections('Resources/Sites/LCG', True)
    if sitesList['OK']:
      sitesList = sitesList['Value']
      try:
        sitesList.remove('LCG.Dummy.ch')
      except ValueError:
        pass
    else:
      raise RSSException, where(self, self.syncWithCS) + sitesList['Message']
    
    for site in sitesList:
      tier = gConfig.getValue("Resources/Sites/LCG/%s/MoUTierLevel" %site)
      if tier == 0 or tier == '0':
        T0List.append(site)
      if tier == 1 or tier == '1':
        T1List.append(site)
      if tier == 2 or tier == '2' or tier == None or tier == 'None':
        T2List.append(site)
   
    siteCE = getSiteCEMapping('LCG')['Value']
    for i in siteCE.values():
      for ce in i:
        if ce is None:
          continue
#        try:
#          ce = socket.gethostbyname_ex(ce)[0]
#        except socket.gaierror:
#          pass
        CEList.append(ce)
      
    siteSE = getSiteSEMapping('LCG')['Value']
    for i in siteSE.values():
      for x in i:
        SEList.append(x)
        
    for SE in SEList:
      node = gConfig.getValue("/Resources/StorageElements/%s/AccessProtocol.1/Host" %SE)
      if node is None:
        continue
#      try:
#        node = socket.gethostbyname_ex(node)[0]
#      except socket.gaierror:
#        pass
      if node not in SENodeList:
        SENodeList.append(node)

    #create LFCNodeList
    for site in gConfig.getSections('Resources/FileCatalogs/LcgFileCatalogCombined', True)['Value']:
      for readable in ('ReadOnly', 'ReadWrite'):
        LFCNode = gConfig.getValue('Resources/FileCatalogs/LcgFileCatalogCombined/%s/%s' %(site, readable))
        if LFCNode is None:
          continue
#        try:
#          LFCNode = socket.gethostbyname_ex(LFCNode)[0]
#        except socket.gaierror:
#          pass
        if LFCNode is not None and LFCNode not in LFCNodeList:
          LFCNodeList.append(LFCNode)
      
      

    sitesIn = self.getMonitoredsList('Site', paramsList = ['SiteName'])
    sitesIn = [s[0] for s in sitesIn]
    servicesIn = self.getMonitoredsList('Service', paramsList = ['ServiceName'])
    servicesIn = [s[0] for s in servicesIn]
    resourcesIn = self.getMonitoredsList('Resource', paramsList = ['ResourceName'])
    resourcesIn = [s[0] for s in resourcesIn]
    storageElementsIn = self.getMonitoredsList('StorageElement',
                                               paramsList = ['StorageElementName'])
    storageElementsIn = [s[0] for s in storageElementsIn]

    #remove sites no more in the CS  - separate because of "race conditions"
    for site in sitesIn:
      if site not in T0List + T1List + T2List:
        self.removeResource(siteName = site)
        time.sleep(0.3)
    for site in sitesIn:
      if site not in T0List + T1List + T2List:
        self.removeService(siteName = site)
        time.sleep(0.3)
    for site in sitesIn:
      if site not in T0List + T1List + T2List:
        self.removeSite(site)
        time.sleep(0.3)
    
    #add new T0 sites
    for site in T0List:
      if site not in sitesIn:
        self.addOrModifySite(site, 'T0', 'Active', 'init', 
                             datetime.utcnow().replace(microsecond = 0), 'RS_SVC', 
                             datetime(9999, 12, 31, 23, 59, 59))
        sitesIn.append(site)

    #add new T1 sites
    for site in T1List:
      if site not in sitesIn:
        self.addOrModifySite(site, 'T1', 'Active', 'init', 
                             datetime.utcnow().replace(microsecond = 0), 'RS_SVC', 
                             datetime(9999, 12, 31, 23, 59, 59))
        sitesIn.append(site)
    
    #add new T2 sites
    for site in T2List:
      if site not in sitesIn:
        self.addOrModifySite(site, 'T2', 'Active', 'init', 
                             datetime.utcnow().replace(microsecond = 0), 'RS_SVC', 
                             datetime(9999, 12, 31, 23, 59, 59))
        sitesIn.append(site)
        
    #create SerCompList 
    for site in siteCE.keys():
      for ce in siteCE[site]:
        service = 'Computing@' + site
        if service not in SerCompList:
          SerCompList.append(service)

    #create SerStorList 
    for site in siteSE.keys():
      for se in siteSE[site]:
        service = 'Storage@' + site
        if service not in SerStorList:
          SerStorList.append(service)
          
    #remove Services no more in the CS  - separate because of "race conditions"
    for ser in servicesIn:
      if ser not in SerCompList + SerStorList:
        self.removeResource(serviceName = ser)
    for ser in servicesIn:
      if ser not in SerCompList + SerStorList:
        self.removeService(ser)

#    #add new Computing services
#    for ser in SerCompList:
#      if ser not in servicesIn:
#        self.addOrModifyService(ser, 'Computing', 'Active', 'init', 
#                                datetime.utcnow().replace(microsecond = 0), 'RS_SVC', 
#                                datetime(9999, 12, 31, 23, 59, 59))

    #remove CEs or SEs or LFCs nodes no more in the CS
    for res in resourcesIn:
      if res not in CEList + SENodeList + LFCNodeList:
        self.removeResource(res)
        
    #remove SEs no more in the CS
    for res in storageElementsIn:
      if res not in SEList:
        self.removeStorageElement(res)
        
    #add new comp services and CEs - separate because of "race conditions"         
    for site in siteCE.keys():
      if site == 'LCG.Dummy.ch':
        continue
      for ce in siteCE[site]:
        service = 'Computing@' + site
        if service not in servicesIn:
          self.addOrModifyService(service, 'Computing', site, 'Active', 'init', 
                                  datetime.utcnow().replace(microsecond = 0), 'RS_SVC', 
                                  datetime(9999, 12, 31, 23, 59, 59))
          servicesIn.append(service)
    for site in siteCE.keys():
      if site == 'LCG.Dummy.ch':
        continue
      for ce in siteCE[site]:
        if ce is None:
          continue
        service = 'Computing@' + site
#        try:
#          ce = socket.gethostbyname_ex(ce)[0]
#        except socket.gaierror:
#          pass
        if ce not in resourcesIn:
          self.addOrModifyResource(ce, 'CE', service, site, 'Active', 'init', 
                                   datetime.utcnow().replace(microsecond = 0), 'RS_SVC', 
                                   datetime(9999, 12, 31, 23, 59, 59))
          resourcesIn.append(ce)
      
    #add new storage services and SEs nodes - separate because of "race conditions"
    for site in siteSE.keys():
      if site == 'LCG.Dummy.ch':
        continue
      for se in siteSE[site]:
        service = 'Storage@' + site
        if service not in servicesIn:
          self.addOrModifyService(service, 'Storage', site, 'Active', 'init', 
                                  datetime.utcnow().replace(microsecond = 0), 'RS_SVC', 
                                  datetime(9999, 12, 31, 23, 59, 59))
          servicesIn.append(service)
    for site in siteSE.keys():
      if site == 'LCG.Dummy.ch':
        continue
      for se in siteSE[site]:
        service = 'Storage@' + site
        se = gConfig.getValue("/Resources/StorageElements/%s/AccessProtocol.1/Host" %se)
        if se is None:
          continue
#        try:
#          se = socket.gethostbyname_ex(se)[0]
#        except socket.gaierror:
#          pass
        if se not in resourcesIn and se is not None:
          sss = se+service+site
          if sss not in seServiceSite:
            self.addOrModifyResource(se, 'SE', service, site, 'Active', 'init', 
                                     datetime.utcnow().replace(microsecond = 0), 'RS_SVC', 
                                     datetime(9999, 12, 31, 23, 59, 59))
            seServiceSite.append(sss)


    #add new storage services and LFCs - separate because of "race conditions"         
    for site in gConfig.getSections('Resources/FileCatalogs/LcgFileCatalogCombined', True)['Value']:
      service = 'Storage@'+site
      if service not in servicesIn:
        self.addOrModifyService(service, 'Storage', site, 'Active', 'init', 
                                datetime.utcnow().replace(microsecond = 0), 'RS_SVC', 
                                datetime(9999, 12, 31, 23, 59, 59))
        servicesIn.append(service)
      
      for readable in ('ReadOnly', 'ReadWrite'):
        LFCNode = gConfig.getValue('Resources/FileCatalogs/LcgFileCatalogCombined/%s/%s' %(site, readable))
        if LFCNode is None:
          continue
#        try:
#          LFCNode = socket.gethostbyname_ex(LFCNode)[0]
#        except socket.gaierror:
#          pass
        if LFCNode is not None and LFCNode not in resourcesIn:
          #Otherwise I can't monitor SAM!
          if site == 'LCG.NIKHEF.nl':
            site = 'LCG.SARA.nl'
          self.addOrModifyResource(LFCNode, 'LFC', service, site, 'Active', 'init', 
                                   datetime.utcnow().replace(microsecond = 0), 'RS_SVC', 
                                   datetime(9999, 12, 31, 23, 59, 59))
          resourcesIn.append(LFCNode)
    
    
    seRes = []

    #add StorageElements
    for site in siteSE.keys():
      if site == 'LCG.Dummy.ch':
        continue
      for storageElement in siteSE[site]:
        if storageElement not in storageElementsIn:
          res = gConfig.getValue("/Resources/StorageElements/%s/AccessProtocol.1/Host" %storageElement)
          if res is not None:
            sr = storageElement+res
            if sr not in seRes:
              self.addOrModifyStorageElement(storageElement, res, site, 'Active', 'init', 
                                             datetime.utcnow().replace(microsecond = 0), 
                                             'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
              seRes.append(sr)
            
    
    #sincrony of assignee group for alarms        
    #I just take all lhcb_prod users
#    from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
#    nc = NotificationClient()
#    nc.setAssigneeGroup('RSS_alarms', gConfig.getValue("Security/Groups/lhcb_prod/Users").replace(',', '').split())
#            
    return S_OK()
          
      
#############################################################################

  def getStuffToCheck(self, granularity, checkFrequency = None, maxN = None):
    """ 
    Get Sites, Services, or Resources to be checked.
    
    :params:
      :attr:`granularity`: a ValidRes
      
      :attr:`checkFrequecy': dictonary. Frequency of active sites/resources checking in minutes.
              See :mod:`DIRAC.ResourceStatusSystem.Policy.Configurations`
      
      :attr:`maxN`: integer - maximum number of lines in output
    """
    
    if granularity in ('Service', 'Services'):
      req = "SELECT ServiceName, Status, FormerStatus, ServiceType FROM"
      req = req + " PresentServices WHERE LastCheckTime = '0000-00-00 00:00:00'"
      if maxN != None:
        req = req + " LIMIT %d" %maxN
      
      resQuery = self.db._query(req)
      if not resQuery['OK']:
        raise RSSDBException, where(self, self.getStuffToCheck) + resQuery['Message']
      if not resQuery['Value']:
        return []
      stuffList = []
      stuffList = [ x for x in resQuery['Value']]
  
      return stuffList
    
    else:

      T0activeCheckFrequecy = checkFrequency['T0_ACTIVE_CHECK_FREQUENCY']
      T0probingCheckFrequecy = checkFrequency['T0_PROBING_CHECK_FREQUENCY']
      T0bannedCheckFrequecy = checkFrequency['T0_BANNED_CHECK_FREQUENCY']
      T1activeCheckFrequecy = checkFrequency['T1_ACTIVE_CHECK_FREQUENCY']
      T1probingCheckFrequecy = checkFrequency['T1_PROBING_CHECK_FREQUENCY']
      T1bannedCheckFrequecy = checkFrequency['T1_BANNED_CHECK_FREQUENCY']
      T2activeCheckFrequecy = checkFrequency['T2_ACTIVE_CHECK_FREQUENCY']
      T2probingCheckFrequecy = checkFrequency['T2_PROBING_CHECK_FREQUENCY']
      T2bannedCheckFrequecy = checkFrequency['T2_BANNED_CHECK_FREQUENCY']
  
      T0dateToCheckFromActive = (datetime.utcnow().replace(microsecond = 0)-timedelta(minutes=T0activeCheckFrequecy)).isoformat(' ')
      T0dateToCheckFromProbing = (datetime.utcnow().replace(microsecond = 0)-timedelta(minutes=T0probingCheckFrequecy)).isoformat(' ')
      T0dateToCheckFromBanned = (datetime.utcnow().replace(microsecond = 0)-timedelta(minutes=T0bannedCheckFrequecy)).isoformat(' ')
      T1dateToCheckFromActive = (datetime.utcnow().replace(microsecond = 0)-timedelta(minutes=T1activeCheckFrequecy)).isoformat(' ')
      T1dateToCheckFromProbing = (datetime.utcnow().replace(microsecond = 0)-timedelta(minutes=T1probingCheckFrequecy)).isoformat(' ')
      T1dateToCheckFromBanned = (datetime.utcnow().replace(microsecond = 0)-timedelta(minutes=T1bannedCheckFrequecy)).isoformat(' ')
      T2dateToCheckFromActive = (datetime.utcnow().replace(microsecond = 0)-timedelta(minutes=T2activeCheckFrequecy)).isoformat(' ')
      T2dateToCheckFromProbing = (datetime.utcnow().replace(microsecond = 0)-timedelta(minutes=T2probingCheckFrequecy)).isoformat(' ')
      T2dateToCheckFromBanned = (datetime.utcnow().replace(microsecond = 0)-timedelta(minutes=T2bannedCheckFrequecy)).isoformat(' ')
  
      if granularity in ('Site', 'Sites'):
        req = "SELECT SiteName, Status, FormerStatus, SiteType FROM PresentSites WHERE"
      elif granularity in ('Resource', 'Resources'):
        req = "SELECT ResourceName, Status, FormerStatus, SiteType, ResourceType FROM PresentResources WHERE"
      elif granularity in ('StorageElement', 'StorageElements'):
        req = "SELECT StorageElementName, Status, FormerStatus, SiteType FROM PresentStorageElements WHERE"
      else:
        raise InvalidRes, where(self, self.getStuffToCheck)
      req = req + " (Status = 'Active' AND SiteType = 'T0' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC') OR" %( T0dateToCheckFromActive )
      req = req + " (Status = 'Probing' AND SiteType = 'T0' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC') OR" %( T0dateToCheckFromProbing )
      req = req + " (Status = 'Banned' AND SiteType = 'T0' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC') OR" %( T0dateToCheckFromBanned )
      req = req + " (Status = 'Active' AND SiteType = 'T1' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC') OR" %( T1dateToCheckFromActive )
      req = req + " (Status = 'Probing' AND SiteType = 'T1' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC') OR" %( T1dateToCheckFromProbing )
      req = req + " (Status = 'Banned' AND SiteType = 'T1' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC') OR" %( T1dateToCheckFromBanned )
      req = req + " (Status = 'Active' AND SiteType = 'T2' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC') OR" %( T2dateToCheckFromActive )
      req = req + " (Status = 'Probing' AND SiteType = 'T2' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC') OR" %( T2dateToCheckFromProbing )
      req = req + " (Status = 'Banned' AND SiteType = 'T2' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC')" %( T2dateToCheckFromBanned )
      req = req + " ORDER BY LastCheckTime"
      if maxN != None:
        req = req + " LIMIT %d" %maxN
      
      resQuery = self.db._query(req)
      if not resQuery['OK']:
        raise RSSDBException, where(self, self.getStuffToCheck) + resQuery['Message']
      if not resQuery['Value']:
        return []
      stuffList = []
      stuffList = [ x for x in resQuery['Value']]
  
      return stuffList

#############################################################################

  def rankRes(self, granularity, days, startingDate = None):
    """
    Construct the rank of a ValidRes, based on the time it's been Active, Probing 
    (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`)
    
    :params:
      :attr:`granularity`: string, a ValidRes
      
      :attr:`days`: integer, amount of days in the past to look at
      
      :attr:`startingDate`: datetime or string - optional date to start from  
    """

    if granularity not in ValidRes:
      raise InvalidRes

    if startingDate is not None:
      if isinstance(startingDate, basestring):
        startingDate = datetime.strptime(startingDate, '%Y-%m-%d %H:%M:%S')
    else:
      startingDate = datetime.utcnow().replace(microsecond = 0)
    
    dateToCheckFrom = startingDate - timedelta(days = days)
    
    if granularity in ('Site', 'Sites'):
      resList = self.getMonitoredsList(granularity, paramsList = ['SiteName'])
    if granularity in ('Service', 'Services'):
      resList = self.getMonitoredsList(granularity, paramsList = ['ServiceName'])
    if granularity in ('Resource', 'Resources'):
      resList = self.getMonitoredsList(granularity, paramsList = ['ResourceName'])
    if granularity in ('StorageElement', 'StorageElements'):
      resList = self.getMonitoredsList(granularity, paramsList = ['StorageElementName'])
    
    rankList = []
    activeRankList = []
    probingRankList = []
    
    for res in resList:

      periodsActive = self.getPeriods(granularity, res[0], 'Active', None, days)
      periodsActive = [ [ datetime.strptime(period[0], '%Y-%m-%d %H:%M:%S'), 
                         datetime.strptime(period[1], '%Y-%m-%d %H:%M:%S') ] for period in periodsActive ]
      
      for p in periodsActive:
        if p[1] < dateToCheckFrom:
          periodsActive.remove(p)
        elif p[0] < dateToCheckFrom:
          p[0] = dateToCheckFrom
      
      activePeriodsLength = [ x[1]-x[0] for x in periodsActive ]
      activePeriodsLength = [self.__convertTime(x) for x in activePeriodsLength]
      activeRankList.append((res, sum(activePeriodsLength)))

      
      
      periodsProbing = self.getPeriods(granularity, res[0], 'Probing', None, days)
      periodsProbing = [ [ datetime.strptime(period[0], '%Y-%m-%d %H:%M:%S'), 
                          datetime.strptime(period[1], '%Y-%m-%d %H:%M:%S') ] for period in periodsProbing ]

      for p in periodsProbing:
        if p[1] < dateToCheckFrom:
          periodsProbing.remove(p)
        elif p[0] < dateToCheckFrom:
          p[0] = dateToCheckFrom
      
      probingPeriodsLength = [ x[1]-x[0] for x in periodsProbing ]
      probingPeriodsLength = [self.__convertTime(x) for x in probingPeriodsLength]
      probingRankList.append((res, sum(probingPeriodsLength)))
    
      rankList.append( ( res[0], sum(activePeriodsLength) + sum(probingPeriodsLength)/2 ) )
      
    activeRankList = sorted(activeRankList, key=lambda x:(x[1], x[0]))
    probingRankList = sorted(probingRankList, key=lambda x:(x[1], x[0]))
    rankList =sorted(rankList, key=lambda x:(x[1], x[0]))

    rank = {'WeightedRank':rankList, 'ActivesRank':activeRankList, 
            'ProbingsRank':probingRankList}

    return rank

#############################################################################

  def __convertTime(self, t):
    
    sec = 0
    
    try:
      tms = t.milliseconds
      sec = sec + tms/1000
    except AttributeError:
      pass
    try:
      ts = t.seconds
      sec = sec + ts
    except AttributeError:
      pass
    try:
      tm = t.minutes
      sec = sec + tm * 60
    except AttributeError:
      pass
    try:
      th = t.hours
      sec = sec + th * 3600
    except AttributeError:
      pass
    try:
      td = t.days
      sec = sec + td * 86400
    except AttributeError:
      pass
    try:
      tw = t.weeks
      sec = sec + tw * 604800
    except AttributeError:
      pass
    
    return sec
    
#############################################################################    
