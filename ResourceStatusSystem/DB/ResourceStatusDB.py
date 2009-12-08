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
  
  The simplest way to instantiate an object of type :class:`ResourceStatusDB` is simply by calling 

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

  Or, if you want to work with a local DB, provided it's MySQL:

   >>> rsDB = ResourceStatusDB(DBin = ['UserName', 'Password'])

  """

# SI PUO' FARE MEGLIO!!!
  def __init__(self, *args, **kwargs):
#    if kwargs['systemInstance']:
#      systemInstance = kwargs['systemInstance'] 
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
# Site functions
#############################################################################

#############################################################################

  def getSitesList(self, paramsList = None, siteName = None, status = None, siteType = None):
    """ 
    Get Present Sites list. 
    
    :params:
      :attr:`paramsList`: a list of parameters can be entered. If not, a custom list is used. 
      
      :attr:`siteName`: a string or a list representing the site name
      
      :attr:`status`: a string or a list representing the status
      
      :attr:`siteType`: a string or a list representing the site type (T0, T1, T2)
      
    :return:
      list of siteName paramsList's values
    """
    
    #query construction
        
    #paramsList
    if (paramsList == None or paramsList == []):
      params = 'SiteName, Status, FormerStatus, DateEffective, LastCheckTime '
    else:
      if type(paramsList) is not list:
        paramsList = [paramsList]
      params = ','.join([x.strip()+' ' for x in paramsList])

    #siteName
    if (siteName == None or siteName == []): 
      r = "SELECT SiteName FROM PresentSites"
      resQuery = self.db._query(r)
      if not resQuery['OK']:
        raise RSSDBException, where(self, self.getSitesList)+resQuery['Message']
      if not resQuery['Value']:
        siteName = []
      siteName = [ x[0] for x in resQuery['Value']]
      siteName = ','.join(['"'+x.strip()+'"' for x in siteName])
    else:
      if type(siteName) is not list:
        siteName = [siteName]
      siteName = ','.join(['"'+x.strip()+'"' for x in siteName])
    
    #status
    if (status == None or status == []):
      status = ValidStatus
    else:
      if type(status) is not list:
        status = [status]
    status = ','.join(['"'+x.strip()+'"' for x in status])

    #siteType
    if (siteType == None or siteType == []):
      siteType = ValidSiteType
    else:
      if type(siteType) is not list:
        siteType = [siteType]
    siteType = ','.join(['"'+x.strip()+'"' for x in siteType])

    #query
    req = "SELECT %s FROM PresentSites WHERE" %(params)
    if siteName != [] and siteName != None and siteName is not None and siteName != '':
      req = req + " SiteName IN (%s) AND" %(siteName)
    req = req + " Status in (%s) AND" % (status)
    req = req + " SiteType in (%s)" % (siteType)
    
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getSitesList)+resQuery['Message']
    if not resQuery['Value']:
      return []
    siteList = []
    siteList = [ x for x in resQuery['Value']]
    return siteList

#############################################################################

  def getSitesStatusWeb(self, selectDict, sortList, startItem, maxItems):
    """ 
    Get present sites status list, for the web.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getSitesList`
    and :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getSitesHistory`
    
    :params:
      :attr:`selectDict`: { 'SiteName':['XX', ...] , 'ExpandSiteHistory': ['XX', ...], 'Status': ['XX', ...]} 
      
      :attr:`sortList` 
      
      :attr:`startItem` 
      
      :attr:`maxItems`
      
    :return: 
      {
     
      :attr:`ParameterNames`: ['SiteName', 'Tier', 'GridType', 'Country', 'Status', 'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'], 
      
      :attr:`Records`: [[], [], ...], 
      
      :attr:`TotalRecords`: X,
       
      :attr:`Extras`: {}
      
      }
      
    """
        
    paramNames = ['SiteName', 'Tier', 'GridType', 'Country', 'Status', 'DateEffective', 'FormerStatus', 'Reason']

    resultDict = {}
    records = []

    paramsList = []
    sites_select = []
    status_select = []
    siteType_select = []
    expand_site_history = ''
    
    #get everything
    if selectDict.keys() == []:
      paramsList = ['SiteName', 'SiteType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
      
    #specify SiteName
    if selectDict.has_key('SiteName'):
      paramsList = ['SiteName', 'SiteType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
      sites_select = selectDict['SiteName']
      if type(sites_select) is not list:
        sites_select = [sites_select]
      del selectDict['SiteName']
      
    #Status
    if selectDict.has_key('Status'):
      paramsList = ['SiteName', 'SiteType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
      status_select = selectDict['Status']
      if type(status_select) is not list:
        status_select = [status_select]
      del selectDict['Status']
      
    #SiteType
    if selectDict.has_key('SiteType'):
      paramsList = ['SiteName', 'SiteType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
      siteType_select = selectDict['SiteType']
      if type(siteType_select) is not list:
        siteType_select = [siteType_select]
      del selectDict['SiteType']
      
    #ExpandSiteHistory
    if selectDict.has_key('ExpandSiteHistory'):
      paramsList = ['SiteName', 'Status', 'Reason', 'DateEffective']
      sites_select = selectDict['ExpandSiteHistory']
      if type(sites_select) is not list:
        sites_select = [sites_select]
      #calls getSitesHistory
      sitesHistory = self.getSitesHistory(paramsList = paramsList, siteName = sites_select)
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
    
    else:
      #makes the right call to getSitesList
      sitesList = self.getSitesList(paramsList = paramsList, siteName = sites_select, status = status_select, siteType = siteType_select)
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

  def getSitesHistory(self, paramsList = None, siteName = None):
    """ 
    Get list of sites history (a site name can be specified)
        
    :params:
      :attr:`paramsList`: A list of parameters can be entered. If not, a custom list is used.
  
      :attr:`siteName`: list of strings. If not given, fetches the complete list 
    """
    
        
    if (paramsList == None or paramsList == []):
      params = 'SiteName, Status, Reason, DateCreated, DateEffective, DateEnd, OperatorCode '
    else:
      if type(paramsList) is not list:
        paramsList = [paramsList]
      params = ','.join([x.strip()+' ' for x in paramsList])

    if (siteName == None or siteName == []): 
      req = "SELECT %s FROM SitesHistory ORDER BY SiteName, SitesHistoryID" %(params)
    else:
      if type(siteName) is not list:
        siteName = [siteName]
      siteName = ','.join(['"'+x.strip()+'"' for x in siteName])
      req = "SELECT %s FROM SitesHistory WHERE SiteName IN (%s) ORDER BY SitesHistoryID" % (params, siteName)
    
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getSitesList)+resQuery['Message']
    if not resQuery['Value']:
      return []
    siteList = []
    siteList = [ x for x in resQuery['Value']]
    return siteList
 
#############################################################################

  def setSiteStatus(self, siteName, status, reason, operatorCode):
    """ 
    Set a Site status, effective from now, with no ending
        
    :params:
      :attr:`siteName`: string
  
      :attr:`status`: string. Possibilities: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
  
      :attr:`reason`: string
  
      :attr:`operatorCode`: string. For the service itself: `RS_SVC`
    """

    gLogger.info("Setting Site %s new status: %s" % (siteName, status))
    req = "SELECT SiteType FROM Sites WHERE SiteName = '%s' AND DateEffective < UTC_TIMESTAMP();" %(siteName)
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.setSiteStatus) + resQuery['Message']
    if not resQuery['Value']:
      return None

    siteType = resQuery['Value'][0][0]
  
    self.addOrModifySite(siteName, siteType, status, reason, datetime.utcnow(), operatorCode, datetime(9999, 12, 31, 23, 59, 59))
    
#############################################################################

  def addOrModifySite(self, siteName, siteType, status, reason, dateEffective, operatorCode, dateEnd):
    """ 
    Add or modify a site to the Sites table.
    
    :params:
      :attr:`siteName`: string - name of the site (DIRAC name)
    
      :attr:`siteType`: string - ValidSiteType: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`status`: string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateEffective`: datetime - date from which the site status is effective

      :attr:`operatorCode`: string - free

      :attr:`dateEnd`: datetime - date from which the site status ends to be effective
    """

    dateCreated = datetime.utcnow()
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
      self._addSiteHistoryRow(siteName, oldStatus, reason, dateCreated, dateEffective, datetime.utcnow().isoformat(' '), operatorCode)

    #in any case add a row to present Sites table
    self._addSiteRow(siteName, siteType, status, reason, dateCreated, dateEffective, dateEnd, operatorCode)
#    siteRow = "Added %s --- %s " %(siteName, dateEffective)
#    return siteRow

#############################################################################

  def _addSiteRow(self, siteName, siteType, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    """
    Add a new site row in Sites table

    :params:
      :attr:`siteName`: string - name of the site (DIRAC name)
    
      :attr:`siteType`: string - ValidSiteType: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`status`: string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateCreated`: datetime - date when which the site row is created

      :attr:`dateEffective`: datetime - date from which the site status is effective

      :attr:`dateEnd`: datetime - date from which the site status ends to be effective

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
    req = req + "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" % (siteName, siteType, status, reason, dateCreated, dateEffective, dateEnd, operatorCode)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self._addSiteRow) + resUpdate['Message']

#############################################################################

  def _addSiteHistoryRow(self, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    """ 
    Add an old site row in the SitesHistory table

    :params:
      :attr:`siteName`: string - name of the site (DIRAC name)
    
      :attr:`status`: string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateCreated`: datetime - date when which the site row is created

      :attr:`dateEffective`: datetime - date from which the site status is effective

      :attr:`dateEnd`: datetime - date from which the site status ends to be effective

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
    req = req + "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s');" % (siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode)

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

  def removeSiteRow(self, siteName, dateEffective):
    """ 
    Remove a site row from the Sites table
    
    :params:
      :attr:`siteName`: string
      
      :attr:`dateEffective`: string or datetime
    """
    #if type(dateEffective) not in types.StringTypes:
    if not isinstance(dateEffective, basestring):
      dateEffective = dateEffective.isoformat(' ')

    req = "DELETE from Sites WHERE SiteName = '%s' AND DateEffective = '%s';" % (siteName, dateEffective)
    resDel = self.db._update(req)
    if not resDel['OK']:
      raise RSSDBException, where(self, self.removeSiteRow) + resDel['Message'] 

#############################################################################


  def addSiteType(self, siteType, description=''):
    """ 
    Add a site type (T0, T1, T2, ...)
    
    :params:
      :attr:`serviceType`: string

      :attr:`description`: string, optional
    """

    req = "INSERT INTO SiteTypes (SiteType, Description)"
    req = req + "VALUES ('%s', '%s');" % (siteType, description)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self.addSiteType) + resUpdate['Message']


#############################################################################

  def setLastSiteCheckTime(self, siteName):
    """ 
    Set to utcnow() LastCheckTime of table Resources
    
    :params:
      :attr:`siteName`: string
    """
    
    req = "UPDATE Sites SET LastCheckTime = UTC_TIMESTAMP() WHERE "
    req = req + "SiteName = '%s' AND DateEffective <= UTC_TIMESTAMP();" % (siteName)
    resUpdate = self.db._update(req)
    
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self.setLastSiteCheckTime) + resUpdate['Message']

#############################################################################

  def setSiteReason(self, siteName, reason, operatorCode):
    """
    Set new reason to resourceName
        
    :params:
      :attr:`siteName`: string, service name

      :attr:`reason`: string, reason

      :attr:`operatorCode`: string, who's making this change (RS_SVC if it's the service itslef)
    """
    
    req = "UPDATE Sites SET Reason = '%s', OperatorCode = '%s' WHERE SiteName = '%s';" %(reason, operatorCode, siteName)
    resUpdate = self.db._update(req)
    
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self.setSiteReason) + resUpdate['Message']

#############################################################################

  def removeSiteType(self, siteType):
    """ 
    Remove a site type from the SiteTypes table
    
    :params:
      :attr:`siteType`: string, a site type
    """

    req = "DELETE from SiteTypes WHERE SiteType = '%s';" % (siteType)
    resDel = self.db._update(req)
    if not resDel['OK']:
      raise RSSDBException, where(self, self.removeSiteType) + resDel['Message']

#############################################################################

#############################################################################
# Resource functions
#############################################################################

#############################################################################

  def getResourcesList(self, paramsList = None, resourceName = None, siteName = None, status = None, resourceType = None):
    """ 
    Get Present Resources list. 
    
    :Params:
      :attr:`paramsList`: a list of parameters can be entered. If not, a custom list is used. 
      
      :attr:`resourceName`: a string or a list representing the resource name
      
      :attr:`status`: a string or a list representing the status
      
      :attr:`resourceType`: a string or a list representing the resource type
      
    :return:
      list of resourceName paramsList's values
    """
    
    #paramsList    
    if (paramsList == None or paramsList == []):
      params = 'ResourceName, Status, FormerStatus, DateEffective, LastCheckTime '
    else:
      if type(paramsList) is not list:
        paramsList = [paramsList]
      params = ','.join([x.strip()+' ' for x in paramsList])

    #siteName
    if (siteName == None or siteName == []):
      r = "SELECT SiteName FROM PresentSites"
      resQuery = self.db._query(r)
      if not resQuery['OK']:
        raise RSSDBException, where(self, self.getSitesList)+resQuery['Message']
      if not resQuery['Value']:
        siteName = []
      siteName = [ x[0] for x in resQuery['Value']]
      siteName = ','.join(['"'+x.strip()+'"' for x in siteName])
    else:
      if type(siteName) is not list:
        siteName = [siteName]
      siteName = ','.join(['"'+x.strip()+'"' for x in siteName])

    #status
    if (status == None or status == []):
      status = ValidStatus
    else:
      if type(status) is not list:
        status = [status]
    status = ','.join(['"'+x.strip()+'"' for x in status])

    #resourceName
    if (resourceName == None or resourceName == []): 
      r = "SELECT ResourceName FROM PresentResources"
      resQuery = self.db._query(r)
      if not resQuery['OK']:
        raise RSSDBException, where(self, self.getResourcesList)+resQuery['Message']
      if not resQuery['Value']:
        resourceName = []
      resourceName = [ x[0] for x in resQuery['Value']]
      resourceName = ','.join(['"'+x.strip()+'"' for x in resourceName])
    else:
      if type(resourceName) is not list:
        resourceName = [resourceName]
      resourceName = ','.join(['"'+x.strip()+'"' for x in resourceName])
      
    #resourceType
    if (resourceType == None or resourceType == []):
      resourceType = ValidResourceType
    else:
      if type(resourceType) is not list:
        resourceType = [resourceType]
    resourceType = ','.join(['"'+x.strip()+'"' for x in resourceType])

    req = "SELECT %s FROM PresentResources WHERE" %(params) 
    if resourceName != [] and resourceName != None and resourceName is not None and resourceName != '':
      req = req + " ResourceName IN (%s) AND" %(resourceName)
    if siteName != [] and siteName != None and siteName is not None and siteName != '':
      req = req + " SiteName IN (%s) AND" %siteName
    req = req + " Status IN (%s) AND" %status
    req = req + " ResourceType IN (%s)" %resourceType
    
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getResourcesList)+resQuery['Message']
    if not resQuery['Value']:
      return []
    resourceList = []
    resourceList = [ x for x in resQuery['Value']]
    return resourceList

#############################################################################

  def getResourcesStatusWeb(self, selectDict, sortList, startItem, maxItems):
    """ get present resources status list, for the web
    
        :params:
          :attr:`selectDict`: {'ResourceName':['XX', ...], 'ExpandResourceHistory': ['XX', ...], 'Status':['XX', ...]}
          
          :attr:`sortList`: [] (now only empty)
          
          :attr:`startItem`: integer
          
          :attr:`maxItems`: integer
    
        :return: 
          { 
      
          :attr:`ParameterNames`: ['ResourceName', 'ServiceName', 'SiteName', 'ResourceType', 'Country', 'Status', 'DateEffective', 'FormerStatus', 'Reason'], 
          
          :attr:`Records`: [[], [], ...],
          
          :attr:`TotalRecords`: X, 
          
          :attr:`Extras`: {} 
          
          }
    """
    
    paramNames = ['ResourceName', 'ServiceName', 'SiteName', 'ResourceType', 'Country', 'Status', 'DateEffective', 'FormerStatus', 'Reason']

    resultDict = {}
    records = []

    paramsList = []
    resources_select = []
    sites_select = []
    status_select = []
    resourceType_select = []
    expand_resource_history = ''
    
    # get everything
    if selectDict.keys() == []:
      paramsList = ['ResourceName', 'ServiceName', 'SiteName', 'ResourceType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
    
    #ResourceName
    if selectDict.has_key('ResourceName'):
      paramsList = ['ResourceName', 'ServiceName', 'SiteName', 'ResourceType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']      
      resources_select = selectDict['ResourceName']
      if type(resources_select) is not list:
        resources_select = [resources_select]
      del selectDict['ResourceName']
      
    #SiteName
    if selectDict.has_key('SiteName'):
      paramsList = ['ResourceName', 'ServiceName', 'SiteName', 'ResourceType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']      
      sites_select = selectDict['SiteName']
      if type(sites_select) is not list:
        sites_select = [sites_select]
      del selectDict['SiteName']
      
    #Status
    if selectDict.has_key('Status'):
      paramsList = ['ResourceName', 'ServiceName', 'SiteName', 'ResourceType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
      status_select = selectDict['Status']
      if type(status_select) is not list:
        status_select = [status_select]
      del selectDict['Status']

    #ResourceType
    if selectDict.has_key('ResourceType'):
      paramsList = ['ResourceName', 'ServiceName', 'SiteName', 'ResourceType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']      
      resourceType_select = selectDict['ResourceType']
      if type(resourceType_select) is not list:
        resourceType_select = [resourceType_select]
      del selectDict['ResourceType']
    
    #ResourceHistory
    if selectDict.has_key('ExpandResourceHistory'):
      paramsList = ['ResourceName', 'Status', 'Reason', 'DateEffective']
      resources_select = selectDict['ExpandResourceHistory']
      if type(resources_select) is not list:
        resources_select = [resources_select]
      #calls getSitesHistory
      resourcesHistory = self.getResourcesHistory(paramsList = paramsList, resourceName = resources_select)
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
    
    else:
      #makes the right call to getResourcesList
      resourcesList = self.getResourcesList(paramsList = paramsList, resourceName = resources_select, siteName = sites_select, status = status_select, resourceType = resourceType_select)
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

  def getResourcesHistory(self, paramsList = None, resourceName = None):
    """ 
    Get list of Resources history (a Resource name can be specified)
        
    :params:
      :attr:`paramsList`: A list of parameters can be entered. If not, a custom list is used.

      :attr:`resourceName`: list of strings. If not given, fetches the complete list 
    """
    
        
    if (paramsList == None or paramsList == []):
      params = 'ResourceName, Status, Reason, DateCreated, DateEffective, DateEnd, OperatorCode '
    else:
      if type(paramsList) is not list:
        paramsList = [paramsList]
      params = ','.join([x.strip()+' ' for x in paramsList])

    if (resourceName == None or resourceName == []): 
      req = "SELECT %s FROM ResourcesHistory ORDER BY ResourceName, ResourcesHistoryID" %(params)
    else:
      if type(resourceName) is not list:
        resourceName = [resourceName]
      resourceName = ','.join(['"'+x.strip()+'"' for x in resourceName])
      req = "SELECT %s FROM ResourcesHistory WHERE ResourceName IN (%s) ORDER BY ResourcesHistoryID" % (params, resourceName)
    
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getResourcesHistory)+resQuery['Message']
    if not resQuery['Value']:
      return []
    ResourceList = []
    ResourceList = [ x for x in resQuery['Value']]
    return ResourceList
 
#############################################################################

  def setResourceStatus(self, resourceName, status, reason, operatorCode):
    """ 
    Set a Resource status, effective from now, with no ending
    
    :params:
      :attr:`resourceName`: string

      :attr:`status`: string. Possibilities: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string

      :attr:`operatorCode`: string. For the service itself: `RS_SVC`
    """

    gLogger.info("Setting Resource %s new status: %s" % (resourceName, status))
    req = "SELECT ResourceType, ServiceName, SiteName FROM Resources WHERE ResourceName = '%s' AND DateEffective < UTC_TIMESTAMP();" %(resourceName)
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.setResourceStatus) + resQuery['Message']
    if not resQuery['Value']:
      return None

    resourceType = resQuery['Value'][0][0]
    serviceName = resQuery['Value'][0][1]
    siteName = resQuery['Value'][0][2]

    self.addOrModifyResource(resourceName, resourceType, serviceName, siteName, status, reason, datetime.utcnow(), operatorCode, datetime(9999, 12, 31, 23, 59, 59))
    
#############################################################################

  def addOrModifyResource(self, resourceName, resourceType, serviceName, siteName, status, reason, dateEffective, operatorCode, dateEnd):
    """ 
    Add or modify a resource to the Resources table.
    
    :params:
      :attr:`resourceName`: string - name of the resource (DIRAC name)
    
      :attr:`resourceType`: string - ValidResourceType: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`status`: string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateEffective`: datetime - date from which the resource status is effective

      :attr:`operatorCode`: string - free

      :attr:`dateEnd`: datetime - date from which the resource status ends to be effective
    """

    dateCreated = datetime.utcnow()
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
      self._addResourcesHistoryRow(resourceName, serviceName, siteName, oldStatus, reason, dateCreated, dateEffective, datetime.utcnow().isoformat(' '),  operatorCode)

    #in any case add a row to present Sites table
    self._addResourcesRow(resourceName, resourceType, serviceName, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode)
#    resourceRow = "Added %s --- %s --- %s " %(resourceName, siteName, dateEffective)
#    return resAddResourcesRow

#############################################################################

  def _addResourcesRow(self, resourceName, resourceType, serviceName, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    """ 
    Add a new resource row in Resources table

    :params:
      :attr:`resourceName`: string - name of the resource (DIRAC name)
    
      :attr:`resourceType`: string - ValidResourceType: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`status`: string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateCreated`: datetime - date when which the resource row is created

      :attr:`dateEffective`: datetime - date from which the resource status is effective

      :attr:`dateEnd`: datetime - date from which the resource status ends to be effective

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
    req = req + "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" % (resourceName, resourceType, serviceName, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self._addResourcesRow) + resUpdate['Message']
    

#############################################################################

  def _addResourcesHistoryRow(self, resourceName, serviceName, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    """
    Add an old resource row in the ResourcesHistory table

    :params:
      :attr:`resourceName`: string - name of the resource (DIRAC name)
    
      :attr:`status`: string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateCreated`: datetime - date when which the resource row is created

      :attr:`dateEffective`: datetime - date from which the resource status is effective

      :attr:`dateEnd`: datetime - date from which the resource status ends to be effective

      :attr:`operatorCode`: string - free
    """

    if not isinstance(dateCreated, basestring):
      dateCreated = dateCreated.isoformat(' ')
    if not isinstance(dateEffective, basestring):
      dateEffective = dateEffective.isoformat(' ')
    if not isinstance(dateEnd, basestring):
      dateEnd = dateEnd.isoformat(' ')


    req = "INSERT INTO ResourcesHistory (ResourceName, ServiceName, SiteName, Status, Reason, DateCreated,"
    req = req + " DateEffective, DateEnd, OperatorCode) "
    req = req + "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" % (resourceName, serviceName, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self._addResourcesHistoryRow) + resUpdate['Message']

#############################################################################

  def setLastResourceCheckTime(self, resourceName):
    """ 
    Set to utcnow() LastCheckTime of table Resources
    
        
    :params:
      :attr:`resourceName`: string
    """
    
    req = "UPDATE Resources SET LastCheckTime = UTC_TIMESTAMP() WHERE "
    req = req + "ResourceName = '%s' AND DateEffective <= UTC_TIMESTAMP();" % (resourceName)
    resUpdate = self.db._update(req)
    
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self.setLastResourceCheckTime) + resUpdate['Message']

#############################################################################

  def setResourceReason(self, resourceName, reason, operatorCode):
    """ 
    Set new reason to resourceName
        
    :params:
      :attr:`resourceName`: string, service name

      :attr:`reason`: string, reason

      :attr:`operatorCode`: string, who's making this change (RS_SVC if it's the service itslef)
    """
    
    req = "UPDATE Resources SET Reason = '%s', OperatorCode = '%s' WHERE ResourceName = '%s';" % (reason, operatorCode, resourceName)
    resUpdate = self.db._update(req)
    
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self.setResourceReason) + resUpdate['Message']

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

  def removeResourceRow(self, resourceName, siteName, dateEffective):
    """ 
    Remove a Resource Status from the Resources table
    
    :params:
      :attr:`resourceName`: string
      
      :attr:`siteName`: string
      
      :attr:`dateEffective`: string or datetime
    """

    if not isinstance(dateEffective, basestring):
      dateEffective = dateEffective.isoformat(' ')

    req = "DELETE from Resources WHERE ResourceName = '%s' AND SiteName = '%s' AND DateEffective = '%s';" % (resourceName, siteName, dateEffective)
    resDel = self.db._update(req)
    if not resDel['OK']:
      raise RSSDBException, where(self, self.removeResourceRow) + resDel['Message']

#############################################################################

  def removeResourceType(self, resourceType):
    """ 
    Remove a Resource Type
    
    :params:
      :attr:`resourceType`: string, a service type (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`)
    """

    req = "DELETE from ResourceTypes WHERE ResourceType = '%s';" % (resourceType)
    resDel = self.db._update(req)
    if not resDel['OK']:
      raise RSSDBException, where(self, self.removeResourceType) + resDel['Message']

#############################################################################


#############################################################################
# Service functions
#############################################################################

#############################################################################
  
  
  def getServicesList(self, paramsList = None, serviceName = None, siteName = None, status = None, serviceType = None):
    """ 
    Get Present Services list. 
    
    :params:
      :attr:`paramsList`: a list of parameters can be entered. If not, a custom list is used. 
      
      :attr:`serviceName`: a string or a list representing the service name
      
      :attr:`siteName`: a string or a list representing the site name
      
      :attr:`status`: a string or a list representing the status
      
      :attr:`serviceType`: a string or a list representing the service type (T0, T1, T2)
      
    :return:
      list of serviceName paramsList's values
    """
    
    #query construction
        
    #paramsList
    if (paramsList == None or paramsList == []):
      params = 'ServiceName, Status, FormerStatus, DateEffective, LastCheckTime '
    else:
      if type(paramsList) is not list:
        paramsList = [paramsList]
      params = ','.join([x.strip()+' ' for x in paramsList])

    #serviceName
    if (serviceName == None or serviceName == []): 
      r = "SELECT ServiceName FROM PresentServices"
      resQuery = self.db._query(r)
      if not resQuery['OK']:
        raise RSSDBException, where(self, self.getServicesList)+resQuery['Message']
      if not resQuery['Value']:
        serviceName = []
      serviceName = [ x[0] for x in resQuery['Value']]
      serviceName = ','.join(['"'+x.strip()+'"' for x in serviceName])
    else:
      if type(serviceName) is not list:
        serviceName = [serviceName]
      serviceName = ','.join(['"'+x.strip()+'"' for x in serviceName])
    
    #siteName
    if (siteName == None or siteName == []):
      r = "SELECT SiteName FROM PresentSites"
      resQuery = self.db._query(r)
      if not resQuery['OK']:
        raise RSSDBException, where(self, self.getSitesList)+resQuery['Message']
      if not resQuery['Value']:
        siteName = []
      siteName = [ x[0] for x in resQuery['Value']]
      siteName = ','.join(['"'+x.strip()+'"' for x in siteName])
    else:
      if type(siteName) is not list:
        siteName = [siteName]
      siteName = ','.join(['"'+x.strip()+'"' for x in siteName])

   
    #status
    if (status == None or status == []):
      status = ValidStatus
    else:
      if type(status) is not list:
        status = [status]
    status = ','.join(['"'+x.strip()+'"' for x in status])

    #serviceType
    if (serviceType == None or serviceType == []):
      serviceType = ValidServiceType
    else:
      if type(serviceType) is not list:
        serviceType = [serviceType]
    serviceType = ','.join(['"'+x.strip()+'"' for x in serviceType])

    #query
    req = "SELECT %s FROM PresentServices WHERE" %(params)
    if serviceName != [] and serviceName != None and serviceName is not None and serviceName != '':
      req = req + " ServiceName IN (%s) AND" %(serviceName)
    req = req + " SiteName in (%s) AND" % (siteName)
    req = req + " Status in (%s) AND" % (status)
    req = req + " ServiceType in (%s)" % (serviceType)
    
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getServicesList)+resQuery['Message']
    if not resQuery['Value']:
      return []
    serviceList = []
    serviceList = [ x for x in resQuery['Value']]
    return serviceList

#############################################################################

  def getServicesStatusWeb(self, selectDict, sortList, startItem, maxItems):
    """ 
    Get present services status list, for the web.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getServicesList`
    and :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getServicesHistory`
    
    :params:
      :attr:`selectDict`: { 'ServiceName':['XX', ...] , 'ExpandServiceHistory': ['XX', ...], 'Status': ['XX', ...]} 
      
      :attr:`sortList` 
      
      :attr:`startItem` 
      
      :attr:`maxItems`
      
    :return: { 
      :attr:`ParameterNames`: ['ServiceName', 'ServiceType', 'Site', 'Country', 'Status', 'DateEffective', 'FormerStatus', 'Reason'], 
      
      :attr:`Records`: [[], [], ...], 
      
      :attr:`TotalRecords`: X,
       
      :attr:`Extras`: {}
      
      }
    """
        
    paramNames = ['ServiceName', 'ServiceType', 'Site', 'Country', 'Status', 'DateEffective', 'FormerStatus', 'Reason']

    resultDict = {}
    records = []

    paramsList = []
    services_select = []
    sites_select = []
    status_select = []
    serviceType_select = []
    expand_service_history = ''
    
    #get everything
    if selectDict.keys() == []:
      paramsList = ['ServiceName', 'ServiceType', 'SiteName', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
      
    #specify ServiceName
    if selectDict.has_key('ServiceName'):
      paramsList = ['ServiceName', 'ServiceType', 'SiteName', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
      services_select = selectDict['ServiceName']
      if type(services_select) is not list:
        services_select = [services_select]
      del selectDict['ServiceName']
      
    #specify SiteName
    if selectDict.has_key('SiteName'):
      paramsList = ['ServiceName', 'ServiceType', 'SiteName', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
      sites_select = selectDict['SiteName']
      if type(sites_select) is not list:
        sites_select = [sites_select]
      del selectDict['SiteName']
      
    #Status
    if selectDict.has_key('Status'):
      paramsList = ['ServiceName', 'ServiceType', 'SiteName', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
      status_select = selectDict['Status']
      if type(status_select) is not list:
        status_select = [status_select]
      del selectDict['Status']
      
    #ServiceType
    if selectDict.has_key('ServiceType'):
      paramsList = ['ServiceName', 'ServiceType', 'SiteName', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
      serviceType_select = selectDict['ServiceType']
      if type(serviceType_select) is not list:
        serviceType_select = [serviceType_select]
      del selectDict['ServiceType']
      
    #ExpandServiceHistory
    if selectDict.has_key('ExpandServiceHistory'):
      paramsList = ['ServiceName', 'Status', 'Reason', 'DateEffective']
      services_select = selectDict['ExpandServiceHistory']
      if type(services_select) is not list:
        services_select = [services_select]
      #calls getServicesHistory
      servicesHistory = self.getServicesHistory(paramsList = paramsList, serviceName = services_select)
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
    
    else:
      #makes the right call to getServicesList
      servicesList = self.getServicesList(paramsList = paramsList, serviceName = services_select, siteName = sites_select, status = status_select, serviceType = serviceType_select)
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

  def getServicesHistory(self, paramsList = None, serviceName = None):
    """ 
    Get list of services history (a service name can be specified)
        
    :params:
      :attr:`paramsList`: A list of parameters can be entered. If not, a custom list is used.
      
      :attr:`serviceName`: list of strings. If not given, fetches the complete list 
    """
    
        
    if (paramsList == None or paramsList == []):
      params = 'ServiceName, Status, Reason, DateCreated, DateEffective, DateEnd, OperatorCode '
    else:
      if type(paramsList) is not list:
        paramsList = [paramsList]
      params = ','.join([x.strip()+' ' for x in paramsList])

    if (serviceName == None or serviceName == []): 
      req = "SELECT %s FROM ServicesHistory ORDER BY ServiceName, ServicesHistoryID" %(params)
    else:
      if type(serviceName) is not list:
        serviceName = [serviceName]
      serviceName = ','.join(['"'+x.strip()+'"' for x in serviceName])
      req = "SELECT %s FROM ServicesHistory WHERE ServiceName IN (%s) ORDER BY ServicesHistoryID" % (params, serviceName)
    
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getServicesHistory)+resQuery['Message']
    if not resQuery['Value']:
      return []
    serviceList = []
    serviceList = [ x for x in resQuery['Value']]
    return serviceList
 
#############################################################################

  def setServiceStatus(self, serviceName, status, reason, operatorCode):
    """ 
    Set a Service status, effective from now, with no ending
        
    :params:
      :attr:`serviceName`: string
      
      :attr:`status`: string. Possibilities: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string
      
      :attr:`operatorCode`: string. For the service itself: `RS_SVC`
    """

    gLogger.info("Setting Service %s new status: %s" % (serviceName, status))
    req = "SELECT ServiceType, SiteName FROM Services WHERE ServiceName = '%s' AND DateEffective < UTC_TIMESTAMP();" %(serviceName)
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.setServiceStatus) + resQuery['Message']
    if not resQuery['Value']:
      return None

    serviceType = resQuery['Value'][0][0]
    siteName = resQuery['Value'][0][1]
  
    self.addOrModifyService(serviceName, serviceType, siteName, status, reason, datetime.utcnow(), operatorCode, datetime(9999, 12, 31, 23, 59, 59))
    
#############################################################################

  def addOrModifyService(self, serviceName, serviceType, siteName, status, reason, dateEffective, operatorCode, dateEnd):
    """ 
    Add or modify a service to the Services table.
    
    :params:
      :attr:`serviceName`: string - name of the service (DIRAC name)
    
      :attr:`serviceType`: string - ValidServiceType: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`status`: string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateEffective`: datetime - date from which the service status is effective

      :attr:`operatorCode`: string - free

      :attr:`dateEnd`: datetime - date from which the service status ends to be effective
    """

    dateCreated = datetime.utcnow()
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
      self._addServiceHistoryRow(serviceName, siteName, oldStatus, reason, dateCreated, dateEffective, datetime.utcnow().isoformat(' '), operatorCode)

    #in any case add a row to present Services table
    self._addServiceRow(serviceName, serviceType, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode)
#    serviceRow = "Added %s --- %s " %(serviceName, dateEffective)
#    return serviceRow

#############################################################################

  def _addServiceRow(self, serviceName, serviceType, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    """
    Add a new service row in Services table

    :params:
      :attr:`serviceName`: string - name of the service (DIRAC name)
    
      :attr:`serviceType`: string - ValidServiceType: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`status`: string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateCreated`: datetime - date when which the service row is created

      :attr:`dateEffective`: datetime - date from which the service status is effective

      :attr:`dateEnd`: datetime - date from which the service status ends to be effective

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
    req = req + "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" % (serviceName, serviceType, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self._addServiceRow) + resUpdate['Message']

#############################################################################

  def _addServiceHistoryRow(self, serviceName, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    """
    Add an old service row in the ServicesHistory table

    :params:
      :attr:`serviceName`: string - name of the service (DIRAC name)
    
      :attr:`status`: string - ValidStatus: see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`
      
      :attr:`reason`: string - free
      
      :attr:`dateCreated`: datetime - date when which the service row is created

      :attr:`dateEffective`: datetime - date from which the service status is effective

      :attr:`dateEnd`: datetime - date from which the service status ends to be effective

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
    req = req + "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" % (serviceName, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode)

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

  def removeServiceRow(self, serviceName, dateEffective):
    """ 
    Remove a service row from the Services table
    
    :params:
      :attr:`serviceName`: string
      
      :attr:`dateEffective`: string or datetime
    """
    #if type(dateEffective) not in types.StringTypes:
    if not isinstance(dateEffective, basestring):
      dateEffective = dateEffective.isoformat(' ')

    req = "DELETE from Services WHERE ServiceName = '%s' AND DateEffective = '%s';" % (serviceName, dateEffective)
    resDel = self.db._update(req)
    if not resDel['OK']:
      raise RSSDBException, where(self, self.removeServiceRow) + resDel['Message']

#############################################################################


  def addServiceType(self, serviceType, description=''):
    """ 
    Add a service type (Computing, Storage, ...)
    
    :params:
      :attr:`serviceType`: string
      
      :attr:`description`: string, optional
    """

    req = "INSERT INTO ServiceTypes (ServiceType, Description)"
    req = req + "VALUES ('%s', '%s');" % (serviceType, description)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self.addServiceType) + resUpdate['Message']


#############################################################################

  def setLastServiceCheckTime(self, serviceName):
    """ 
    Set to utcnow() LastCheckTime of table Resources
    
    :params:
      :attr:`serviceName`: string
    """
    
    req = "UPDATE Services SET LastCheckTime = UTC_TIMESTAMP() WHERE "
    req = req + "ServiceName = '%s' AND DateEffective <= UTC_TIMESTAMP();" % (serviceName)
    resUpdate = self.db._update(req)
    
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self.setLastServiceCheckTime) + resUpdate['Message']

#############################################################################

  def setServiceReason(self, serviceName, reason, operatorCode):
    """ 
    Set new reason to serviceName
    
    :params:
      :attr:`serviceName`: string, service name
      
      :attr:`reason`: string, reason
      
      :attr:`operatorCode`: string, who's making this change (RS_SVC if it's the service itslef)
    """
    
    req = "UPDATE Services SET Reason = '%s', OperatorCode = '%s' WHERE ServiceName = '%s';" %(reason, operatorCode, serviceName)
    resUpdate = self.db._update(req)
    
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self.setServiceReason) + resUpdate['Message']

#############################################################################

  def setServiceToBeChecked(self, granularity, name):
    """ 
    Set new reason to serviceName
    
    :params:
      :attr:`granularity`: string, 'Site' or 'Resource'
      
      :attr:`name`: string, name of Site or Service
    """
    
    if granularity in ('Site', 'Sites'):
      serviceName = self.getServicesList(paramsList = ['ServiceName'], siteName = name)
      if type(serviceName) is not list:
        serviceName = [serviceName]
      serviceName = ','.join(['"'+x.strip()+'"' for x in serviceName[0]])
      req = "UPDATE Services SET LastCheckTime = '00000-00-00 00:00:00' WHERE ServiceName IN (%s);" %(serviceName)
    elif granularity in ('Resource', 'Resources'):
      serviceName = self.getGeneralName(name, 'Resource', 'Service')
      req = "UPDATE Services SET LastCheckTime = '00000-00-00 00:00:00' WHERE ServiceName  = '%s';" %(serviceName)
    
    resUpdate = self.db._update(req)
    
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self.setServiceToBeChecked) + resUpdate['Message']


#############################################################################

  def removeServiceType(self, serviceType):
    """ 
    Remove a service type from the ServiceTypes table
    
    :params:
      :attr:`serviceType`: string, a service type (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`)
    """

    req = "DELETE from ServiceTypes WHERE ServiceType = '%s';" % (serviceType)
    resDel = self.db._update(req)
    if not resDel['OK']:
      raise RSSDBException, where(self, self.removeServiceType) + resDel['Message']

#############################################################################

  def getResourceStats(self, granularity, name):
    """ 
    Returns simple statistics of active, probing and banned resources of a site or service;
        
    :params:
      :attr:`siteName`: string - a site name
    
    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """
    
    res = {'Active':0, 'Probing':0, 'Banned':0, 'Total':0}
    
    if granularity == 'Site': 
      req = "SELECT Status, COUNT(*) FROM Resources WHERE SiteName = '%s' GROUP BY Status" %name
    elif granularity == 'Service': 
      req = "SELECT Status, COUNT(*) FROM Resources WHERE ServiceName = '%s' GROUP BY Status" %name
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getResourceStats) + resQuery['Message']
    else:
      for x in resQuery['Value']:
        res[x[0]] = int(x[1])
        
    res['Total'] = sum(res.values())
    
    return res


#############################################################################

#############################################################################
# GENERAL functions
#############################################################################

#############################################################################

  def getSiteTypeList(self, siteType=None):
    """ 
    Get list of site types
    
    :Params:
      :attr:`siteType`: string, site type.
    """

    if siteType == None:
      req = "SELECT SiteType FROM SiteTypes"
    else:
      req = "SELECT SiteType, Description FROM SiteTypes "
      req = req + "WHERE SiteType = '%s'" % (siteType)

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getSiteTypeList) + resQuery['Message']
    if not resQuery['Value']:
      return []
    typeList = []
    typeList = [ x[0] for x in resQuery['Value']]
    return typeList

#############################################################################

  def getResourceTypeList(self, resourceType=None):
    """ 
    Get list of resource types.
    
    :Params:
      :attr:`resourceType`: a single resource type
    """

    if resourceType == None:
      req = "SELECT ResourceType FROM ResourceTypes"
    else:
      req= "SELECT ResourceType FROM ResourceTypes "
      req = req + "WHERE ResourceType = '%s'" % (resourceType)

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getResourceTypeList) + resQuery['Message']
    if not resQuery['Value']:
      return []
    typeList = []
    typeList = [ x[0] for x in resQuery['Value']]
    return typeList

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

  def getServiceTypeList(self, serviceType=None):
    """ 
    Get list of service types
    
    :Params:
      :attr:`serviceType`: string, service type.
    """

    if serviceType == None:
      req = "SELECT ServiceType FROM ServiceTypes"
    else:
      req = "SELECT ServiceType, Description FROM ServiceTypes "
      req = req + "WHERE ServiceType = '%s'" % (serviceType)

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getServiceTypeList) + resQuery['Message']
    if not resQuery['Value']:
      return []
    typeList = []
    typeList = [ x[0] for x in resQuery['Value']]
    return typeList

#############################################################################


  def getGeneralName(self, name, from_g, to_g):
    """ 
    Get name of res, of granularity `from_g`, to the name of res with granularity `to_g`
      
    For a Resource, get the Site name, or the Service name.
    For a Service name, get the Site name
    
    :params:
      :attr:`resource`: a string with a name
      
      :attr:`from_g`: a string with a valid granularity (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`)
      
      :attr:`to_g`: a string with a valid granularity (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`)
      
    :return:
      a string with the resulting name
    """

    if from_g in ('Resource', 'Resources'):
      if to_g in ('Site', 'Sites'):
        req = "SELECT SiteName FROM Resources WHERE ResourceName = '%s';" %(name)
      elif to_g in ('Service', 'Services'):
        req = "SELECT SiteName, ResourceType FROM Resources WHERE ResourceName = '%s';" %(name)
        resQuery = self.db._query(req)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.getGeneralName) + resQuery['Message']
        if not resQuery['Value']:
          return []
        siteName = resQuery['Value'][0][0]
        if resQuery['Value'][0][1] == 'CE':
          serviceType = 'Computing'
        if resQuery['Value'][0][1] == 'SE':
          serviceType = 'Storage'
        req = "SELECT ServiceName FROM Services WHERE SiteName = '%s' AND ServiceType = '%s';" %(siteName, serviceType)
    elif from_g in ('Service', 'Services'):
      if to_g in ('Site', 'Sites'):
        req = "SELECT SiteName FROM Services WHERE ServiceName = '%s';" %(name)

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getGeneralName) + resQuery['Message']
    if not resQuery['Value']:
      return []
    newName = resQuery['Value'][0][0]
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
        req = "SELECT %s FROM %s WHERE TIMESTAMP(DateEnd) < UTC_TIMESTAMP();" %(PKList[0], table)
      elif len(PKList) == 2:
        req = "SELECT %s, %s FROM %s WHERE TIMESTAMP(DateEnd) < UTC_TIMESTAMP();" %(PKList[0], PKList[1], table)
      elif len(PKList) == 3:
        req = "SELECT %s, %s, %s FROM %s WHERE TIMESTAMP(DateEnd) < UTC_TIMESTAMP();" %(PKList[0], PKList[1], PKList[2], table)
      resQuery = self.db._query(req)
      if not resQuery['OK']:
        raise RSSDBException, where(self, self.getEndings) + resQuery['Message']
      else:
        list = []
        list = [ int(x[0]) for x in resQuery['Value']]
        return list


#############################################################################

  def getPeriods(self, granularity, name, status, hours):
    """ 
    Get list of periods of times when a ValidRes was in ValidStatus 
    (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`) for a total of `hours`
    
    :params:
      :attr:`granularity`: string - ValidRes
      
      :attr:`name`: string - name
      
      :attr:`status`: string - ValidStatus
      
      :attr:`hours`: integer
    """
    
    hours = timedelta(hours = hours)
    
    if granularity == 'Site':
      req = "SELECT DateEffective FROM Sites WHERE SiteName = '%s' AND DateEffective < UTC_TIMESTAMP() AND Status = '%s';" %(name, status)
    elif granularity == 'Resource':
      req = "SELECT DateEffective FROM Resources WHERE ResourceName = '%s' AND DateEffective < UTC_TIMESTAMP() AND Status = '%s';" %(name, status)
    elif granularity == 'Service':
      req = "SELECT DateEffective FROM Services WHERE ServiceName = '%s' AND DateEffective < UTC_TIMESTAMP() AND Status = '%s';" %(name, status)
    
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getPeriods) + resQuery['Message']
    else:
      if resQuery['Value'] == '':
        return None
      effFrom = resQuery['Value'][0][0]
      timeInStatus = datetime.utcnow() - effFrom
      if timeInStatus > hours:
        return [((datetime.utcnow()-hours).isoformat(' '), datetime.utcnow().isoformat(' '))] 
      else:
        periods = [(effFrom.isoformat(' '), datetime.utcnow().isoformat(' '))]
        if granularity == 'Site':
          req = "SELECT DateEffective, DateEnd FROM SitesHistory WHERE SiteName = '%s' AND Status = '%s';" %(name, status)
        elif granularity == 'Resource':
          req = "SELECT DateEffective, DateEnd FROM ResourcesHistory WHERE ResourceName = '%s' AND Status = '%s';" %(name, status)
        elif granularity == 'Service':
          req = "SELECT DateEffective, DateEnd FROM ServicesHistory WHERE ServiceName = '%s' AND Status = '%s';" %(name, status)
        
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
              periods.append(((effTo - (hours - oldTimeInStatus)).isoformat(' '), effTo.isoformat(' ')))
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
    req = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ResourceStatusDB' AND TABLE_NAME LIKE \"%History\"";
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
    
    >>> trasact2History(('Site', 'LCG.CERN.ch', datetime.utcnow().isoformat(' ')) - the date is the DateEffective parameter
        trasact2History(('Site', 523)) - the number if the SiteID
        trasact2History(('Service', 'Computing@LCG.CERN.ch', datetime.utcnow().isoformat(' ')) - the date is the DateEffective parameter
        trasact2History(('Service', 523)) - the number if the ServiceID
        trasact2History(('Resource', 'srm-lhcb.cern.ch', datetime.utcnow().isoformat(' ')) - the date is the DateEffective parameter
        trasact2History(('Resource', 523)) - the number if the ResourceID
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
        self._addSiteHistoryRow(args[1], oldStatus, oldReason, oldDateCreated, oldDateEffective, oldDateEnd, oldOperatorCode)
        self.removeSiteRow(args[1], oldDateEffective)

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
        self._addSiteHistoryRow(siteName, oldStatus, oldReason, oldDateCreated, oldDateEffective, oldDateEnd, oldOperatorCode)
        self.removeSiteRow(siteName, oldDateEffective)


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
        self._addServiceHistoryRow(args[1], args[2], oldStatus, oldReason, oldDateCreated, oldDateEffective, oldDateEnd, oldOperatorCode)
        self.removeServiceRow(args[1], oldDateEffective)

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
        self._addServiceHistoryRow(serviceName, siteName, oldStatus, oldReason, oldDateCreated, oldDateEffective, oldDateEnd, oldOperatorCode)
        self.removeServiceRow(serviceName, oldDateEffective)

        
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

        self._addResourcesHistoryRow(args[1], args[2], args[3], oldStatus, oldReason, oldDateCreated, oldDateEffective, oldDateEnd, oldOperatorCode)
        self.removeResourceRow(args[1], args[3], oldDateEffective)
        
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
        self._addResourcesHistoryRow(resourceName, serviceName, siteName, oldStatus, oldReason, oldDateCreated, oldDateEffective, oldDateEnd, oldOperatorCode)
        self.removeResourceRow(resourceName, siteName, oldDateEffective)


#############################################################################

  def setDateEnd(self, *args):
    """ 
    Set date end, for a Site or for a Resource
    
    :params:
      :attr:`args`: a tuple. 
      
        - args[0] is a ValidRes (see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`)
        
        - args[1] is the name of the ValidRes
        
        - args[2] is its dateEffective
    """
    
    siteOrRes = args[0].capitalize()
    
    if siteOrRes not in ValidRes:
      raise InvalidRes, where(self, self.setDateEnd)
    
    if siteOrRes == 'Site':
      query = "UPDATE Sites SET DateEnd = '%s' WHERE SiteName = '%s' AND DateEffective < '%s'" % (args[2], args[1], args[2])
      resUpdate = self.db._update(query)
      if not resUpdate['OK']:
        raise RSSDBException, where(self, self.setDateEnd) + resUpdate['Message']

    elif siteOrRes == 'Resource':
      query = "UPDATE Resources SET DateEnd = '%s' WHERE ResourceName = '%s' AND DateEffective < '%s'" % (args[2], args[1], args[2])
      resUpdate = self.db._update(query)
      if not resUpdate['OK']:
        raise RSSDBException, where(self, self.setDateEnd) + resUpdate['Message']

    elif siteOrRes == 'Service':
      query = "UPDATE Services SET DateEnd = '%s' WHERE ServiceName = '%s' AND DateEffective < '%s'" % (args[2], args[1], args[2])
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
      req = "SELECT COUNT(*) FROM Sites WHERE SiteName = (SELECT SiteName FROM "
      req = req + "Sites WHERE SiteID = '%d');" % (ID)
      
    elif table == 'Resources':
      req = "SELECT COUNT(*) FROM Resources WHERE ResourceName = (SELECT ResourceName "
      req = req + " FROM Resources WHERE ResourceID = '%d');" % (ID)

    elif table == 'Services':
      req = "SELECT COUNT(*) FROM Services WHERE ServiceName = (SELECT ServiceName "
      req = req + " FROM Services WHERE ServiceID = '%d');" % (ID)

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
    Syncronize DB content with CS content. Params are fake.
    """ 
    
    from DIRAC.Core.Utilities.SiteCEMapping import getSiteCEMapping
    from DIRAC.Core.Utilities.SiteSEMapping import getSiteSEMapping
    from DIRAC import S_OK, S_ERROR
    import time
    
    T0List = []
    T1List = []
    T2List = []
    
    CEList = []
    SerCompList = []
    SerStorList = []
    SEList = []
    SENodeList = []
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
      CEList = CEList+i
      
    siteSE = getSiteSEMapping('LCG')['Value']
    for i in siteSE.values():
      for x in i:
        SEList.append(x)
        
    for SE in SEList:
      node = gConfig.getValue("/Resources/StorageElements/%s/AccessProtocol.1/Host" %SE)
      if node not in SENodeList:
        SENodeList.append(node)

    sitesIn = self.getSitesList(paramsList = ['SiteName'])
    sitesIn = [s[0] for s in sitesIn]
    servicesIn = self.getServicesList(paramsList = ['ServiceName'])
    servicesIn = [s[0] for s in servicesIn]
    resourcesIn = self.getResourcesList(paramsList = ['ResourceName'])
    resourcesIn = [s[0] for s in resourcesIn]

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
        self.addOrModifySite(site, 'T0', 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
        sitesIn.append(site)

    #add new T1 sites
    for site in T1List:
      if site not in sitesIn:
        self.addOrModifySite(site, 'T1', 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
        sitesIn.append(site)
    
    #add new T2 sites
    for site in T2List:
      if site not in sitesIn:
        self.addOrModifySite(site, 'T2', 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
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
#        self.addOrModifyService(ser, 'Computing', 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))

    #remove CEs or SEs no more in the CS
   
    for res in resourcesIn:
      if res not in CEList + SENodeList:
        self.removeResource(res)
        
    #add new comp services and CEs - separate because of "race conditions"         
    for site in siteCE.keys():
      if site == 'LCG.Dummy.ch':
        continue
      for ce in siteCE[site]:
        service = 'Computing@' + site
        if service not in servicesIn:
          self.addOrModifyService(service, 'Computing', site, 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
          servicesIn.append(service)
    for site in siteCE.keys():
      if site == 'LCG.Dummy.ch':
        continue
      for ce in siteCE[site]:
        service = 'Computing@' + site
        if ce not in resourcesIn:
          self.addOrModifyResource(ce, 'CE', service, site, 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
          resourcesIn.append(ce)
      
    #add new storage services and SEs - separate because of "race conditions"
    for site in siteSE.keys():
      if site == 'LCG.Dummy.ch':
        continue
      for se in siteSE[site]:
        service = 'Storage@' + site
        if service not in servicesIn:
          self.addOrModifyService(service, 'Storage', site, 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
          servicesIn.append(service)
    for site in siteSE.keys():
      if site == 'LCG.Dummy.ch':
        continue
      for se in siteSE[site]:
        service = 'Storage@' + site
        se = gConfig.getValue("/Resources/StorageElements/%s/AccessProtocol.1/Host" %se)
        if se not in resourcesIn and se is not None:
          sss = se+service+site
          if sss not in seServiceSite:
            self.addOrModifyResource(se, 'SE', service, site, 'Active', 'init', datetime.utcnow(), 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59))
            seServiceSite.append(sss)
            
    return S_OK()
          
      
#############################################################################

  def getStuffToCheck(self, granularity, checkFrequency = None, maxN = None):
    """ 
    Get Sites, Services, or Resources to be checked.
    
    :params:
      :attr:`granularity`: a ValidRes
      
      :attr:`checkFrequecy`: dictonary. Frequency of active sites/resources checking in minutes.
              See :mod:`DIRAC.ResourceStatusSystem.Policy.Configurations`
      
      :attr:`maxN`: integer - maximum number of lines in output
      
    """
    
    if granularity in ('Service', 'Services'):
      req = "SELECT ServiceName, Status, FormerStatus, Reason FROM PresentServices WHERE"
      req = req + " LastCheckTime = '0000-00-00 00:00:00'"
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
  
      T0dateToCheckFromActive = (datetime.utcnow()-timedelta(minutes=T0activeCheckFrequecy)).isoformat(' ')
      T0dateToCheckFromProbing = (datetime.utcnow()-timedelta(minutes=T0probingCheckFrequecy)).isoformat(' ')
      T0dateToCheckFromBanned = (datetime.utcnow()-timedelta(minutes=T0bannedCheckFrequecy)).isoformat(' ')
      T1dateToCheckFromActive = (datetime.utcnow()-timedelta(minutes=T1activeCheckFrequecy)).isoformat(' ')
      T1dateToCheckFromProbing = (datetime.utcnow()-timedelta(minutes=T1probingCheckFrequecy)).isoformat(' ')
      T1dateToCheckFromBanned = (datetime.utcnow()-timedelta(minutes=T1bannedCheckFrequecy)).isoformat(' ')
      T2dateToCheckFromActive = (datetime.utcnow()-timedelta(minutes=T2activeCheckFrequecy)).isoformat(' ')
      T2dateToCheckFromProbing = (datetime.utcnow()-timedelta(minutes=T2probingCheckFrequecy)).isoformat(' ')
      T2dateToCheckFromBanned = (datetime.utcnow()-timedelta(minutes=T2bannedCheckFrequecy)).isoformat(' ')
  
      if granularity in ('Site', 'Sites'):
        req = "SELECT SiteName, Status, FormerStatus, Reason FROM PresentSites WHERE"
      elif granularity in ('Resource', 'Resources'):
        req = "SELECT ResourceName, Status, FormerStatus, Reason FROM PresentResources WHERE"
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
