""" ResourcesStatusDB module.
"""

from types import *
from datetime import datetime, timedelta
from DIRAC import gLogger, gConfig
from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *

#############################################################################

class RSSDBException(RSSException):
  """ RSS DB Exception
  """
  pass

#############################################################################

class NotAllowedDate(RSSException):
  pass

#############################################################################

class ResourceStatusDB:
  """ The ResourcesStatusDB class is a front-end to the Resource Status Database.
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
    req = "SELECT %s FROM PresentSites " %(params)
    req = req + "WHERE SiteName IN (%s) " %(siteName)
    req = req + "AND Status in (%s)" % (status)
    req = req + "AND SiteType in (%s)" % (siteType)
    
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
      
    :return: { 
      :attr:`ParameterNames`: ['SiteName', 'Tier', 'GridType', 'Country', 'Status', 'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'], 
      
      :attr:'Records': [[], [], ...], 
      
      :attr:'TotalRecords': X,
       
      :attr:'Extras': {}
      
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

    req = "SELECT SiteType, Description FROM Sites WHERE SiteName = '%s' AND DateEffective < UTC_TIMESTAMP();" %(siteName)
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.setSiteStatus)
    if not resQuery['Value']:
      return None

    siteType = resQuery['Value'][0][0]
    description = resQuery['Value'][0][1]
  
    self.addOrModifySite(siteName, siteType, description, status, reason, datetime.utcnow(), operatorCode, datetime(9999, 12, 31, 23, 59, 59))
    
#############################################################################

  def addOrModifySite(self, siteName, siteType, description, status, reason, dateEffective, operatorCode, dateEnd):
    """ Add or modify a site to the Sites table.
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
      raise RSSDBException, where(self, self.addOrModifySite)

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
    self._addSiteRow(siteName, siteType, description, status, reason, dateCreated, dateEffective, dateEnd, operatorCode)
#    siteRow = "Added %s --- %s " %(siteName, dateEffective)
#    return siteRow

#############################################################################

  def _addSiteRow(self, siteName, siteType, description, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    #add a new site row in Sites table

    if not isinstance(dateCreated, basestring):
      dateCreated = dateCreated.isoformat(' ')
    if not isinstance(dateEffective, basestring):
      dateEffective = dateEffective.isoformat(' ')
    if not isinstance(dateEnd, basestring):
      dateEnd = dateEnd.isoformat(' ')
    if status not in ValidStatus:
      raise InvalidStatus, where(self, self.addOrModifySite)
      

    req = "INSERT INTO Sites (SiteName, SiteType, Description, Status, Reason, "
    req = req + "DateCreated, DateEffective, DateEnd, OperatorCode) "
    req = req + "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" % (siteName, siteType, description, status, reason, dateCreated, dateEffective, dateEnd, operatorCode)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self._addSiteRow)

#############################################################################

  def _addSiteHistoryRow(self, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    """ add an old site row in the history
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
      raise RSSDBException, where(self, self._addSiteHistoryRow)


#############################################################################

  def removeSite(self, siteName):
    """ 
    Completely remove a site from the Sites table
    
    :params:
      :attr:`siteName`: string
    """
    req = "DELETE from Sites WHERE SiteName = '%s';" % (siteName)
    resDel = self.db._update(req)
    if not resDel['OK']:
      raise RSSDBException, where(self, self.removeSite)

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
      raise RSSDBException, where(self, self.removeSiteRow)

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
      raise RSSDBException, where(self, self.addSiteType)


#############################################################################

  def getSitesToCheck(self, activeCheckFrequecy, probingCheckFrequecy, bannedCheckFrequecy):
    """ get resources to be checked
    """
    dateToCheckFromActive = (datetime.utcnow()-timedelta(minutes=activeCheckFrequecy)).isoformat(' ')
    dateToCheckFromProbing = (datetime.utcnow()-timedelta(minutes=probingCheckFrequecy)).isoformat(' ')
    dateToCheckFromBanned = (datetime.utcnow()-timedelta(minutes=bannedCheckFrequecy)).isoformat(' ')

    req = "SELECT SiteName, Status, FormerStatus, Reason FROM PresentSites WHERE"
    req = req + " (Status = 'Active' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC') OR" %( dateToCheckFromActive )
    req = req + " (Status = 'Probing' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC') OR" %( dateToCheckFromProbing )
    req = req + " (Status = 'Banned' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC');" %( dateToCheckFromBanned )

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getSitesToCheck)
    if not resQuery['Value']:
      return []
    sitesList = []
    sitesList = [ x for x in resQuery['Value']]
    return sitesList

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
      raise RSSDBException, where(self, self.setLastSiteCheckTime)

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
      raise RSSDBException, where(self, self.setSiteReason)

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
      raise RSSDBException, where(self, self.removeSiteType)

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

    req = "SELECT %s FROM PresentResources " %(params) 
    req = req + "WHERE ResourceName IN (%s) " %(resourceName)
    req = req + "AND SiteName IN (%s)" %siteName
    req = req + "AND Status IN (%s)" %status
    req = req + "AND ResourceType IN (%s)" %resourceType
    
    
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
        
          :attr:`ParameterNames`: ['ResourceName', 'SiteName', 'ServiceExposed', 'Country', 'Status', 'DateEffective', 'FormerStatus', 'Reason'], 
          
          :attr:'Records': [[], [], ...],
          
          :attr:'TotalRecords': X, 
          
          :attr:'Extras': {} 
          
          }
    """
    
    paramNames = ['ResourceName', 'SiteName', 'ResourceType', 'Country', 'Status', 'DateEffective', 'FormerStatus', 'Reason']

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
      paramsList = ['ResourceName', 'SiteName', 'ResourceType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
    
    #ResourceName
    if selectDict.has_key('ResourceName'):
      paramsList = ['ResourceName', 'SiteName', 'ResourceType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']      
      resources_select = selectDict['ResourceName']
      if type(resources_select) is not list:
        resources_select = [resources_select]
      del selectDict['ResourceName']
      
    #SiteName
    if selectDict.has_key('SiteName'):
      paramsList = ['ResourceName', 'SiteName', 'ResourceType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']      
      sites_select = selectDict['SiteName']
      if type(sites_select) is not list:
        sites_select = [sites_select]
      del selectDict['SiteName']
      
    #Status
    if selectDict.has_key('Status'):
      paramsList = ['ResourceName', 'SiteName', 'ResourceType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
      status_select = selectDict['Status']
      if type(status_select) is not list:
        status_select = [status_select]
      del selectDict['Status']

    #ResourceType
    if selectDict.has_key('ResourceType'):
      paramsList = ['ResourceName', 'SiteName', 'ResourceType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']      
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
        record.append(resource[1]) #SiteName
        record.append(resource[2]) #ResourceType
        country = (resource[1]).split('.').pop()
        record.append(country) #Country
        record.append(resource[3]) #Status
        record.append(resource[4].isoformat(' ')) #DateEffective
        record.append(resource[5]) #FormerStatus
        record.append(resource[6]) #Reason
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
      raise RSSDBException, where(self, self.getResourcesList)+resQuery['Message']
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

    req = "SELECT ResourceType, SiteName FROM Resources WHERE ResourceName = '%s' AND DateEffective < UTC_TIMESTAMP();" %(resourceName)
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.setResourceStatus)
    if not resQuery['Value']:
      return None

    resourceType = resQuery['Value'][0][0]
    siteName = resQuery['Value'][0][1]

    self.addOrModifyResource(resourceName, resourceType, siteName, status, reason, datetime.utcnow(), operatorCode, datetime(9999, 12, 31, 23, 59, 59))
    
#############################################################################

  def addOrModifyResource(self, resourceName, resourceType, siteName, status, reason, dateEffective, operatorCode, dateEnd):
    """ Add or modify a resource to the Resources table. 
        If the dateEffective is not given, the resource status row is effective from now
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
      raise RSSDBException, where(self, self.addOrModifyResource)

    if resQuery['Value']: 
      #site modification, effective from now
      if dateEffective <= (dateCreated + timedelta(minutes=2)):
        self.setDateEnd('Resource', resourceName, dateEffective)
        self.transact2History('Resource', resourceName, siteName, dateEffective)
      else:
        self.setDateEnd('Resource', resourceName, dateEffective)
    else:
      if status in ('Active', 'Probing'):
        oldStatus = 'Banned'
      else:
        oldStatus = 'Active'
      self._addResourcesHistoryRow(resourceName, siteName, oldStatus, reason, dateCreated, dateEffective, datetime.utcnow().isoformat(' '),  operatorCode)

    #in any case add a row to present Sites table
    self._addResourcesRow(resourceName, resourceType, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode)
#    resourceRow = "Added %s --- %s --- %s " %(resourceName, siteName, dateEffective)
#    return resAddResourcesRow

#############################################################################

  def _addResourcesRow(self, resourceName, resourceType, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    """ add a new resource row in Resources table
    """

    if not isinstance(dateCreated, basestring):
      dateCreated = dateCreated.isoformat(' ')
    if not isinstance(dateEffective, basestring):
      dateEffective = dateEffective.isoformat(' ')
    if not isinstance(dateEnd, basestring):
      dateEnd = dateEnd.isoformat(' ')
    if status not in ValidStatus:
      raise InvalidStatus, where(self, self.addOrModifySite)

    
    req = "INSERT INTO Resources (ResourceName, ResourceType, SiteName, Status, "
    req = req + "Reason, DateCreated, DateEffective, DateEnd, OperatorCode) "
    req = req + "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" % (resourceName, resourceType, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self._addResourcesRow)
    

#############################################################################

  def _addResourcesHistoryRow(self, resourceName, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    """ add an old resource row in the history
    """

    if not isinstance(dateCreated, basestring):
      dateCreated = dateCreated.isoformat(' ')
    if not isinstance(dateEffective, basestring):
      dateEffective = dateEffective.isoformat(' ')
    if not isinstance(dateEnd, basestring):
      dateEnd = dateEnd.isoformat(' ')


    req = "INSERT INTO ResourcesHistory (ResourceName, SiteName, Status, Reason, DateCreated,"
    req = req + " DateEffective, DateEnd, OperatorCode) "
    req = req + "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" % (resourceName, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self._addResourcesHistoryRow)



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
      raise RSSDBException, where(self, self.setLastSiteCheckTime)

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
      raise RSSDBException, where(self, self.setResourceReason)

#############################################################################

  def addResourceType(self, resourceType, description=''):
    """ 
    Add a resource type (CE, SE, ...)
        
    :params:
      :attr:`serviceType`: string
      :attr:`description`: string, optional
    """

    req = "INSERT INTO ResourceTypes (ResourceType, Description)"
    req = req + "VALUES ('%s', '%s');" % (resourceType, description)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self.addResourceType)

#############################################################################

  #usata solo nell'handler
  def removeResource(self, resourceName):
    """ 
    Completely remove a resource from the Resources table
    
    :params:
      :attr:`resourceName`: string
    """

    req = "DELETE from Resources WHERE ResourceName = '%s';" % (resourceName)
    resDel = self.db._update(req)
    if not resDel['OK']:
      raise RSSDBException, where(self, self.removeResource)

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
      raise RSSDBException, where(self, self.removeResourceRow)

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
      raise RSSDBException, where(self, self.removeResourceType)

#############################################################################

  def getResourcesToCheck(self, activeCheckFrequecy, probingCheckFrequecy, bannedCheckFrequecy):
    """ get resources to be checked
    """
    dateToCheckFromActive = (datetime.utcnow()-timedelta(minutes=activeCheckFrequecy)).isoformat(' ')
    dateToCheckFromProbing = (datetime.utcnow()-timedelta(minutes=probingCheckFrequecy)).isoformat(' ')
    dateToCheckFromBanned = (datetime.utcnow()-timedelta(minutes=bannedCheckFrequecy)).isoformat(' ')

    req = "SELECT ResourceName, Status, FormerStatus, Reason FROM PresentResources WHERE"
    req = req + " (Status = 'Active' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC') OR" %( dateToCheckFromActive )
    req = req + " (Status = 'Probing' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC') OR" %( dateToCheckFromProbing )
    req = req + " (Status = 'Banned' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC');" %( dateToCheckFromBanned )

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getResourcesToCheck)
    if not resQuery['Value']:
      return []
    resourcesList = []
    resourcesList = [ x for x in resQuery['Value']]
    return resourcesList

#############################################################################

#############################################################################
# Service functions
#############################################################################

#############################################################################
  def getServicesList(self, paramsList = None, serviceName = None, status = None, serviceType = None):
    """ 
    Get Present Services list. 
    
    :params:
      :attr:`paramsList`: a list of parameters can be entered. If not, a custom list is used. 
      
      :attr:`serviceName`: a string or a list representing the service name
      
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
    req = "SELECT %s FROM PresentServices " %(params)
    req = req + "WHERE ServiceName IN (%s) " %(serviceName)
    req = req + "AND Status in (%s)" % (status)
    req = req + "AND ServiceType in (%s)" % (serviceType)
    
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
      :attr:`ParameterNames`: ['ServiceName', 'Tier', 'GridType', 'Country', 'Status', 'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'], 
      
      :attr:'Records': [[], [], ...], 
      
      :attr:'TotalRecords': X,
       
      :attr:'Extras': {}
      
      }
    """
        
    paramNames = ['ServiceName', 'Tier', 'GridType', 'Country', 'Status', 'DateEffective', 'FormerStatus', 'Reason']

    resultDict = {}
    records = []

    paramsList = []
    services_select = []
    status_select = []
    serviceType_select = []
    expand_service_history = ''
    
    #get everything
    if selectDict.keys() == []:
      paramsList = ['ServiceName', 'ServiceType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
      
    #specify ServiceName
    if selectDict.has_key('ServiceName'):
      paramsList = ['ServiceName', 'ServiceType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
      services_select = selectDict['ServiceName']
      if type(services_select) is not list:
        services_select = [services_select]
      del selectDict['ServiceName']
      
    #Status
    if selectDict.has_key('Status'):
      paramsList = ['ServiceName', 'ServiceType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
      status_select = selectDict['Status']
      if type(status_select) is not list:
        status_select = [status_select]
      del selectDict['Status']
      
    #ServiceType
    if selectDict.has_key('ServiceType'):
      paramsList = ['ServiceName', 'ServiceType', 'Status', 'DateEffective', 'FormerStatus', 'Reason']
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
        record.append(None) #Tier
        record.append(None) #GridType
        record.append(None) #Country
        record.append(service[1]) #Status
        record.append(service[3].isoformat(' ')) #DateEffective
        record.append(None) #FormerStatus
        record.append(service[2]) #Reason
        records.append(record)
    
    else:
      #makes the right call to getServicesList
      servicesList = self.getServicesList(paramsList = paramsList, serviceName = services_select, status = status_select, serviceType = serviceType_select)
      for service in servicesList:
        record = []
        record.append(service[0]) #ServiceName
        record.append(service[1]) #Tier
        gridType = (service[0]).split('.').pop(0)
        record.append(gridType) #GridType
        country = (service[0]).split('.').pop()
        record.append(country) #Country
        record.append(service[2]) #Status
        record.append(service[3].isoformat(' ')) #DateEffective
        record.append(service[4]) #FormerStatus
        record.append(service[5]) #Reason
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
      raise RSSDBException, where(self, self.getServicesList)+resQuery['Message']
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

    req = "SELECT ServiceType, SiteName FROM Services WHERE ServiceName = '%s' AND DateEffective < UTC_TIMESTAMP();" %(serviceName)
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.setServiceStatus)
    if not resQuery['Value']:
      return None

    serviceType = resQuery['Value'][0][0]
    siteName = resQuery['Value'][0][1]
  
    self.addOrModifyService(serviceName, serviceType, siteName, status, reason, datetime.utcnow(), operatorCode, datetime(9999, 12, 31, 23, 59, 59))
    
#############################################################################

  def addOrModifyService(self, serviceName, serviceType, siteName, status, reason, dateEffective, operatorCode, dateEnd):
    """ Add or modify a service to the Services table.
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
      raise RSSDBException, where(self, self.addOrModifyService)

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
    #add a new service row in Services table

    if not isinstance(dateCreated, basestring):
      dateCreated = dateCreated.isoformat(' ')
    if not isinstance(dateEffective, basestring):
      dateEffective = dateEffective.isoformat(' ')
    if not isinstance(dateEnd, basestring):
      dateEnd = dateEnd.isoformat(' ')
    if status not in ValidStatus:
      raise InvalidStatus, where(self, self.addOrModifyService)
      

    req = "INSERT INTO Services (ServiceName, ServiceType, SiteName, Status, Reason, "
    req = req + "DateCreated, DateEffective, DateEnd, OperatorCode) "
    req = req + "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" % (serviceName, serviceType, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self._addServiceRow)

#############################################################################

  def _addServiceHistoryRow(self, serviceName, siteName, status, reason, dateCreated, dateEffective, dateEnd, operatorCode):
    """ add an old service row in the history
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
      raise RSSDBException, where(self, self._addServiceHistoryRow)


#############################################################################

  def removeService(self, serviceName):
    """ 
    Completely remove a service from the Services table
        
    :params:
      :attr:`serviceName`: string
    """
    
    req = "DELETE from Services WHERE ServiceName = '%s';" % (serviceName)
    resDel = self.db._update(req)
    if not resDel['OK']:
      raise RSSDBException, where(self, self.removeService)

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
      raise RSSDBException, where(self, self.removeServiceRow)

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
      raise RSSDBException, where(self, self.addServiceType)


#############################################################################

  def getServicesToCheck(self, activeCheckFrequecy, probingCheckFrequecy, bannedCheckFrequecy):
    """ 
    Get resources to be checked
    
    :params:
      :attr: `activeCheckFrequecy`, `probingCheckFrequecy`, `bannedCheckFrequecy`: \
        minutes, as integers, parameters taken from :mod:`DIRAC.ResourceStatusSystem.Policy.Configurations`
    """
    dateToCheckFromActive = (datetime.utcnow()-timedelta(minutes=activeCheckFrequecy)).isoformat(' ')
    dateToCheckFromProbing = (datetime.utcnow()-timedelta(minutes=probingCheckFrequecy)).isoformat(' ')
    dateToCheckFromBanned = (datetime.utcnow()-timedelta(minutes=bannedCheckFrequecy)).isoformat(' ')

    req = "SELECT ServiceName, Status, FormerStatus, Reason FROM PresentServices WHERE"
    req = req + " (Status = 'Active' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC') OR" %( dateToCheckFromActive )
    req = req + " (Status = 'Probing' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC') OR" %( dateToCheckFromProbing )
    req = req + " (Status = 'Banned' AND LastCheckTime < '%s' AND OperatorCode = 'RS_SVC');" %( dateToCheckFromBanned )

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getServicesToCheck)
    if not resQuery['Value']:
      return []
    servicesList = []
    servicesList = [ x for x in resQuery['Value']]
    return servicesList

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
      raise RSSDBException, where(self, self.setLastServiceCheckTime)

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
      raise RSSDBException, where(self, self.setServiceReason)

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
      raise RSSDBException, where(self, self.removeServiceType)

#############################################################################

  def getResourceStats(self, granularity, name):
    """ 
    Returns simple statistics of active, probing and banned resources of a site;
        
    :params:
      :attr:`siteName`: string - a site name
    
    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """
    
    if granularity == 'Site': 
      req = "SELECT Status, COUNT(*) WHERE SiteName = '%s'" %name
    elif granularity == 'Service': 
      req = "SELECT Status, COUNT(*) WHERE Service = '%s'" %name
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getResourceStats)
    else:
      #TODO
      pass
#      resourceStats = [ x[0] for x in resQuery['Value']]
#      for x in HistoryTablesList:
#        tablesList.append(x[0:len(x)-7])
#      return tablesList


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
      raise RSSDBException, where(self, self.getSiteTypeList)
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
      raise RSSDBException, where(self, self.getResourceTypeList)
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
      raise RSSDBException, where(self, self.getStatusList)
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
      raise RSSDBException, where(self, self.getServiceTypeList)
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

    if from_g == 'Resource':
      if to_g == 'Site':
        req = "SELECT SiteName FROM Resources WHERE ResourceName = '%s';" %(name)
      elif to_g == 'Service':
        req = "SELECT SiteName, ResourceType FROM Resources WHERE ResourceName = '%s';" %(name)
        resQuery = self.db._query(req)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.getGeneralName)
        if not resQuery['Value']:
          return []
        siteName = resQuery['Value'][0][0]
        if resQuery['Value'][0][1] == 'CE':
          serviceType = 'Computing'
        if resQuery['Value'][0][1] == 'SE':
          serviceType = 'Storage'
        req = "SELECT ServiceName FROM Services WHERE SiteName = '%s' AND ServiceType = '%s';" %(siteName, serviceType)
    if from_g == 'Service':
      if to_g == 'Site':
        req = "SELECT SiteName FROM Services WHERE ServiceName = '%s';" %(name)

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getGeneralName)
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
        raise RSSDBException, where(self, self.getEndings)
      else:
        list = []
        list = [ int(x[0]) for x in resQuery['Value']]
        return list


#############################################################################

  def getPeriods(self, granularity, name, status, hours):
    """ get list of periods of times when a site or res was in status
        for a total of hours
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
      raise RSSDBException, where(self, self.getEndings)
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
          raise RSSDBException, where(self, self.getEndings)
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
    """ get list of tables with associated an history table
    """

    tablesList=[]
    req = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ResourceStatusDB' AND TABLE_NAME LIKE \"%History\"";
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getTablesWithHistory)
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
    
    req = "SELECT Status, COUNT(*) WHERE SiteName = '%s'" %siteName
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getServiceStats)
    else:
      #TODO
      pass
#      serviceStats = [ x[0] for x in resQuery['Value']]
#      for x in HistoryTablesList:
#        tablesList.append(x[0:len(x)-7])
#      return tablesList


#############################################################################


  def transact2History(self, *args):
    """ transact a row from a Sites or Resources table to history 
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
          raise RSSDBException, where(self, self.transact2History)
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
          raise RSSDBException, where(self, self.transact2History)
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

    if args[0] in ('Resource', 'Resources'):
      if len(args) == 4:
        req = "SELECT Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, OperatorCode from Resources "
        req = req + "WHERE (ResourceName='%s' AND DateEffective < '%s' );" % (args[1], args[3])
        resQuery = self.db._query(req)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.transact2History)
        if not resQuery['Value']:
          return None
        oldStatus = resQuery['Value'][0][0]
        oldReason = resQuery['Value'][0][1]
        oldDateCreated = resQuery['Value'][0][2]
        oldDateEffective = resQuery['Value'][0][3]
        oldDateEnd = resQuery['Value'][0][4]
        oldOperatorCode = resQuery['Value'][0][5]

        self._addResourcesHistoryRow(args[1], args[2], oldStatus, oldReason, oldDateCreated, oldDateEffective, oldDateEnd, oldOperatorCode)
        self.removeResourceRow(args[1], args[2], oldDateEffective)
        
      elif len(args) == 2:
        req = "SELECT ResourceName, SiteName, Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, OperatorCode from Resources "
        req = req + "WHERE (ResourceID='%s');" % (args[1])
        resQuery = self.db._query(req)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.transact2History)
        if not resQuery['Value']:
          return None
        resourceName = resQuery['Value'][0][0]
        siteName = resQuery['Value'][0][1]
        oldStatus = resQuery['Value'][0][2]
        oldReason = resQuery['Value'][0][3]
        oldDateCreated = resQuery['Value'][0][4]
        oldDateEffective = resQuery['Value'][0][5]
        oldDateEnd = resQuery['Value'][0][6]
        oldOperatorCode = resQuery['Value'][0][7]

        #start "transaction" to history -- should be better to use a real transaction
        self._addResourcesHistoryRow(resourceName, siteName, oldStatus, oldReason, oldDateCreated, oldDateEffective, oldDateEnd, oldOperatorCode)
        self.removeResourceRow(resourceName, siteName, oldDateEffective)


    if args[0] in ('Service', 'Services'):
      #get row to be put in history Services table
      if len(args) == 4:
        req = "SELECT Status, Reason, DateCreated, "
        req = req + "DateEffective, DateEnd, OperatorCode from Services "
        req = req + "WHERE (ServiceName='%s' AND DateEffective < '%s');" % (args[1], args[2])
        resQuery = self.db._query(req)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.transact2History)
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
          raise RSSDBException, where(self, self.transact2History)
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

        

#############################################################################

  def setDateEnd(self, *args):
    """ set date end, for a Site or for a Resource 
    """
    
    siteOrRes = args[0].capitalize()
    
    if siteOrRes not in ValidRes:
      raise InvalidRes, where(self, self.setDateEnd)
    
    if siteOrRes == 'Site':
      query = "UPDATE Sites SET DateEnd = '%s' WHERE SiteName = '%s' AND DateEffective < '%s'" % (args[2], args[1], args[2])
      resUpdate = self.db._update(query)
      if not resUpdate['OK']:
        raise RSSDBException, where(self, self.setDateEnd)

    elif siteOrRes == 'Resource':
      query = "UPDATE Resources SET DateEnd = '%s' WHERE ResourceName = '%s' AND DateEffective < '%s'" % (args[2], args[1], args[2])
      resUpdate = self.db._update(query)
      if not resUpdate['OK']:
        raise RSSDBException, where(self, self.setDateEnd)

    elif siteOrRes == 'Service':
      query = "UPDATE Services SET DateEnd = '%s' WHERE ServiceName = '%s' AND DateEffective < '%s'" % (args[2], args[1], args[2])
      resUpdate = self.db._update(query)
      if not resUpdate['OK']:
        raise RSSDBException, where(self, self.setDateEnd)


#############################################################################


  #usata solo nell'handler
  def addStatus(self, status, description=''):
    """ Add a status
    """

    req = "INSERT INTO Status (Status, Description)"
    req = req + "VALUES ('%s', '%s');" % (status, description)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSDBException, where(self, self.addStatus)

#############################################################################

  #usata solo nell'handler
  def removeStatus(self, status):
    """ remove a status from the Status table
    """

    req = "DELETE from Status WHERE Status = '%s';" % (status)
    resDel = self.db._update(req)
    if not resDel['OK']:
      raise RSSDBException, where(self, self.removeStatus)

#############################################################################

  def unique(self, table, ID):
    """ check if the site or resource corresponding to the ID is unique in the table 
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
      raise RSSDBException, where(self, self.unique)
    else:
      n = int(resQuery['Value'][0][0])
      if n == 1 :
        return True
      else:
        return False
      
#############################################################################
