"""
ResourceStatusClient class is a client for requesting info from the ResourceStatusService.
"""
# it crashes epydoc
# __docformat__ = "restructuredtext en"

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import InvalidRes, RSSException
from DIRAC.ResourceStatusSystem.Utilities.Utils import where
from DIRAC.ResourceStatusSystem.PolicySystem.Configurations import ValidRes, \
    ValidStatus, ValidSiteType, ValidServiceType, ValidResourceType, PolicyTypes

class ResourceStatusClient:

#############################################################################

  def __init__(self, serviceIn = None, timeout = None):
    """ Constructor of the ResourceStatusClient class
    """
    if serviceIn == None:
      self.rsS = RPCClient("ResourceStatus/ResourceStatus", timeout = timeout)
    else:
      self.rsS = serviceIn

#############################################################################

  def getServiceStats(self, granularity, name):
    """
    Returns simple statistics of active, probing and banned services of a site;

    :Parameters:
      `granularity`
        string, has to be 'Site'

      `name`
        string - a service name

    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """

    if granularity not in ('Site', 'Sites'):
      raise InvalidRes, where(self, self.getServiceStats)

    res = self.rsS.getServiceStats(name)
    if not res['OK']:
      raise RSSException, where(self, self.getServiceStats) + " " + res['Message']

    return res['Value']

#############################################################################

  def getResourceStats(self, granularity, name):
    """
    Returns simple statistics of active, probing and banned resources of a site or a service;

    :Parameters:
      `granularity`
        string, should be in ('Site', 'Service')

      `name`
        string, name of site or service

    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """

    res = self.rsS.getResourceStats(granularity, name)
    if not res['OK']:
      raise RSSException, where(self, self.getResourceStats) + " " + res['Message']

    return res['Value']

#############################################################################

  def getStorageElementsStats(self, granularity, name):
    """
    Returns simple statistics of active, probing and banned storageElements of a site or a resource;

    :Parameters:
      `granularity`
        string, should be in ['Site', 'Resource']

      `name`
        string, name of site or resource

    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """

    res = self.rsS.getStorageElementsStats(granularity, name)
    if not res['OK']:
      raise RSSException, where(self, self.getStorageElementsStats) + " " + res['Message']

    return res['Value']

#############################################################################


  def getPeriods(self, granularity, name, status, hours):
    """
    Returns a list of periods of time where name was in status

    :returns:
      {
        'Periods':[list of periods]
      }
    """
    #da migliorare!

    if granularity not in ValidRes:
      raise InvalidRes, where(self, self.getPeriods)

    res = self.rsS.getPeriods(granularity, name, status, hours)
    if not res['OK']:
      raise RSSException, where(self, self.getPeriods) + " " + res['Message']

    return {'Periods':res['Value']}

#############################################################################

  def getGeneralName(self, granularity, name, toGranularity):
    """
    Returns simple statistics of active, probing and banned storageElements of a site or a resource;

    :Parameters:
      `granularity`
        string, should be a ValidRes

      `name`
        string, name of site or resource

      `toGranularity`
        string, should be a ValidRes

    :returns:
      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
    """

    res = self.rsS.getGeneralName(granularity, name, toGranularity)
    if not res['OK']:
      raise RSSException, where(self, self.getGeneralName) + " " + res['Message']

    return res['Value']


#############################################################################

  def getMonitoredStatus(self, granularity, names):
    """
    Returns RSS status of names (could be a string or a list of strings)

    :Parameters:
      `granularity`
        string, should be a ValidRes

      `names`
        string or dict, name(s) of the ValidRes

    :returns:
      'Active'|'Probing'|'Banned'|None
    """

    if not isinstance(names, list):
      names = [names]

    statusList = []

    for name in names:
      if granularity in ('Site', 'Sites'):
        res = self.rsS.getSitesStatusWeb({'SiteName':name}, [], 0, 1)
      elif granularity in ('Service', 'Services'):
        res = self.rsS.getServicesStatusWeb({'ServiceName':name}, [], 0, 1)
      elif granularity in ('Resource', 'Resources'):
        res = self.rsS.getResourcesStatusWeb({'ResourceName':name}, [], 0, 1)
      elif granularity in ('StorageElement', 'StorageElements'):
        res = self.rsS.getStorageElementsStatusWeb({'StorageElementName':name}, [], 0, 1)
      else:
        raise InvalidRes, where(self, self.getMonitoredStatus)

      if not res['OK']:
        raise RSSException, where(self, self.getMonitoredStatus) + " " + res['Message']
      else:
        try:
          if granularity in ('Resource', 'Resources'):
            statusList.append(res['Value']['Records'][0][5])
          else:
            statusList.append(res['Value']['Records'][0][4])
        except IndexError:
          return None

    return statusList

#############################################################################

#  def getCachedAccountingResult(self, name, plotType, plotName):
#    """
#    Returns a cached accounting plot
#
#    :Parameters:
#      `name`
#        string, should be the name of the res
#
#      `plotType`
#        string, plot type
#
#      `plotName`
#        string, should be the plot name
#
#    :returns:
#      a plot
#    """
#
#    res = self.rsS.getCachedAccountingResult(name, plotType, plotName)
#    if not res['OK']:
#      raise RSSException, where(self, self.getCachedAccountingResult) + " " + res['Message']
#
#    return res['Value']


#############################################################################

#  def getCachedResult(self, name, commandName, value, opt_ID = 'NULL'):
#    """
#    Returns a cached result;
#
#    :Parameters:
#      `name`
#        string, name of site or resource
#
#      `commandName`
#        string
#
#      `value`
#        string
#
#      `opt_ID`
#        optional string
#
#    :returns:
#      (result, )
#    """
#
#    res = self.rsS.getCachedResult(name, commandName, value, opt_ID)
#    if not res['OK']:
#      raise RSSException, where(self, self.getCachedResult) + " " + res['Message']
#
#    return res['Value']


#############################################################################

#  def getCachedIDs(self, name, commandName):
#    """
#    Returns a cached result;
#
#    :Parameters:
#      `name`
#        string, name of site or resource
#
#      `commandName`
#        string
#
#    :returns: (e.g.)
#      [78805473L, 78805473L, 78805473L, 78805473L]
#    """
#
#    res = self.rsS.getCachedIDs(name, commandName)
#    if not res['OK']:
#      raise RSSException, where(self, self.getCachedIDs) + " " + res['Message']
#
#    ID_list = [x for x in res['Value']]
#
#    return ID_list

#############################################################################

  def getGridSiteName(self, granularity, name):
    """
    Returns the grid site name (what is in GOC BD)

    :Parameters:
      `granularity`
        string, should be a ValidRes

      `name`
        string, name of site or resource
    """

    res = self.rsS.getGridSiteName(granularity, name)
    if not res['OK']:
      raise RSSException, where(self, self.getGridSiteName) + " " + res['Message']

    return res['Value']

#############################################################################


#
#  def addOrModifySite(self, siteName, siteType, description, status, reason, dateEffective, tokenOwner, dateEnd):
#    try:
#      server = RPCClient('ResourceStatus/ResourceStatus', useCertificates=self.useCerts, timeout=120)
#      result = server.addOrModifySite(siteName, siteType, description, status, reason, dateEffective, tokenOwner, dateEnd)
#      return result
#    except Exception, x:
#      errorStr = "ResourceStatusClient.addOrModifySite failed"
#      gLogger.exception(errorStr,lException=x)
#      return S_ERROR(errorStr+": "+str(x))
#
#  def addOrModifyResource(self, resourceName, resourceType, siteName, status, reason, dateEffective, tokenOwner, dateEnd):
#    try:
#      server = RPCClient('ResourceStatus/ResourceStatus', useCertificates=self.useCerts, timeout=120)
#      result = server.addOrModifyResource(resourceName, resourceType, siteName, status, reason, dateEffective, tokenOwner, dateEnd)
#      return result
#    except Exception, x:
#      errorStr = "ResourceStatusClient.addOrModifyResource failed"
#      gLogger.exception(errorStr,lException=x)
#      return S_ERROR(errorStr+": "+str(x))
