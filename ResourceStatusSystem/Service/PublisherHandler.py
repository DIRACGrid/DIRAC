""" PublisherHandler

This service has been built to provide the RSS web views with all the information
they need. NO OTHER COMPONENT THAN Web controllers should make use of it.

"""

__RCSID__ = '$Id$'

#  pylint: disable=no-self-use

import six
import types
from datetime import datetime, timedelta

# DIRAC
from DIRAC import gLogger, S_OK, gConfig, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.SiteSEMapping import getSEHosts, getStorageElementsHosts
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities import CSHelpers, Utils
ResourceManagementClient = getattr(
    Utils.voimport('DIRAC.ResourceStatusSystem.Client.ResourceManagementClient'),
    'ResourceManagementClient')


# RSS Clients
rsClient = None
rmClient = None


def initializePublisherHandler(_serviceInfo):
  """
  Handler initialization in the usual horrible way.
  """

  global rsClient
  rsClient = ResourceStatusClient()

  global rmClient
  rmClient = ResourceManagementClient()

  return S_OK()


class PublisherHandler(RequestHandler):
  """
  RPCServer used to deliver data to the web portal.

  """

  def __init__(self, *args, **kwargs):
    """
    Constructor
    """
    super(PublisherHandler, self).__init__(*args, **kwargs)

  # ResourceStatusClient .......................................................

  types_getSites = []

  def export_getSites(self):
    """
    Returns list of all sites considered by RSS

    :return: S_OK( [ sites ] ) | S_ERROR
    """

    gLogger.info('getSites')
    return getSites()

  types_getSitesResources = [(basestring, list, types.NoneType)]

  def export_getSitesResources(self, siteNames):
    """
    Returns dictionary with SEs and CEs for the given site(s). If siteNames is
    None, all sites are taken into account.

    :return: S_OK( { site1 : { ces : [ ces ], 'ses' : [ ses  ] },... } ) | S_ERROR
    """

    gLogger.info('getSitesResources')

    if siteNames is None:
      siteNames = getSites()
      if not siteNames['OK']:
        return siteNames
      siteNames = siteNames['Value']

    if isinstance(siteNames, six.string_types):
      siteNames = [siteNames]

    sitesRes = {}

    for siteName in siteNames:

      res = {}
      res['ces'] = CSHelpers.getSiteComputingElements(siteName)
      # Convert StorageElements to host names
      res = DMSHelpers().getSiteSEMapping()
      if not res['OK']:
        return res
      ses = res['Value'][1].get(siteName, [])
      sesHosts = getStorageElementsHosts(ses)
      if not sesHosts['OK']:
        return sesHosts
      # Remove duplicates
      res['ses'] = list(set(sesHosts['Value']))

      sitesRes[siteName] = res

    return S_OK(sitesRes)

  types_getElementStatuses = [basestring, (basestring, list, types.NoneType), (basestring, list, types.NoneType),
                              (basestring, list, types.NoneType), (basestring, list, types.NoneType),
                              (basestring, list, types.NoneType)]

  def export_getElementStatuses(self, element, name, elementType, statusType, status, tokenOwner):
    """
    Returns element statuses from the ResourceStatusDB
    """

    gLogger.info('getElementStatuses')
    return rsClient.selectStatusElement(element, 'Status', name=name, elementType=elementType,
                                        statusType=statusType, status=status,
                                        tokenOwner=tokenOwner)

  types_getElementHistory = [basestring, (basestring, list, types.NoneType), (basestring, list, types.NoneType),
                             (basestring, list, types.NoneType)]

  def export_getElementHistory(self, element, name, elementType, statusType):
    """
    Returns element history from ResourceStatusDB
    """

    gLogger.info('getElementHistory')
    columns = ['Status', 'DateEffective', 'Reason']
    return rsClient.selectStatusElement(element, 'History', name=name, elementType=elementType,
                                        statusType=statusType,
                                        meta={'columns': columns})

  types_getElementPolicies = [basestring, (basestring, list, types.NoneType), (basestring, list, types.NoneType)]

  def export_getElementPolicies(self, element, name, statusType):
    """
    Returns policies for a given element
    """

    gLogger.info('getElementPolicies')
    columns = ['Status', 'PolicyName', 'DateEffective', 'LastCheckTime', 'Reason']
    return rmClient.selectPolicyResult(element=element, name=name,
                                       statusType=statusType,
                                       meta={'columns': columns})

  types_getNodeStatuses = []

  def export_getNodeStatuses(self):
    return rsClient.selectStatusElement('Node', 'Status')

  types_getTree = [basestring, basestring]

  def export_getTree(self, elementType, elementName):
    """
    Given an element type and name,
    finds its parent site and returns all descendants of that site.
    """

    gLogger.info('getTree')

    site = self.getSite(elementType, elementName)
    if not site:
      return S_ERROR('No site')

    siteStatus = rsClient.selectStatusElement('Site', 'Status', name=site,
                                              meta={'columns': ['StatusType', 'Status']})
    if not siteStatus['OK']:
      return siteStatus

    tree = {site: {'statusTypes': dict(siteStatus['Value'])}}

    ces = CSHelpers.getSiteComputingElements(site)
    cesStatus = rsClient.selectStatusElement('Resource', 'Status', name=ces,
                                             meta={'columns': ['Name', 'StatusType', 'Status']})
    if not cesStatus['OK']:
      return cesStatus

    res = DMSHelpers().getSiteSEMapping()
    if not res['OK']:
      return res
    ses = res['Value'][1].get(site, [])

    sesStatus = rsClient.selectStatusElement('Resource', 'Status', name=list(ses),
                                             meta={'columns': ['Name', 'StatusType', 'Status']})
    return sesStatus

    def feedTree(elementsList):

      elements = {}
      for elementTuple in elementsList['Value']:
        name, statusType, status = elementTuple

        if name not in elements:
          elements[name] = {}
        elements[name][statusType] = status

      return elements

    tree[site]['ces'] = feedTree(cesStatus)
    tree[site]['ses'] = feedTree(sesStatus)

    return S_OK(tree)

  types_setToken = [basestring] * 7

  def export_setToken(self, element, name, statusType, token, elementType, username, lastCheckTime):

    lastCheckTime = datetime.strptime(lastCheckTime, '%Y-%m-%d %H:%M:%S')

    credentials = self.getRemoteCredentials()
    gLogger.info(credentials)

    elementInDB = rsClient.selectStatusElement(element, 'Status', name=name,
                                               statusType=statusType,
                                               elementType=elementType,
                                               lastCheckTime=lastCheckTime)
    if not elementInDB['OK']:
      return elementInDB
    elif not elementInDB['Value']:
      return S_ERROR('Your selection has been modified. Please refresh.')

    if token == 'Acquire':
      tokenOwner = username
      tokenExpiration = datetime.utcnow() + timedelta(days=1)
    elif token == 'Release':
      tokenOwner = 'rs_svc'
      tokenExpiration = datetime.max
    else:
      return S_ERROR('%s is unknown token action' % token)

    reason = 'Token %sd by %s ( web )' % (token, username)

    newStatus = rsClient.addOrModifyStatusElement(element, 'Status', name=name,
                                                  statusType=statusType,
                                                  elementType=elementType,
                                                  reason=reason,
                                                  tokenOwner=tokenOwner,
                                                  tokenExpiration=tokenExpiration)
    if not newStatus['OK']:
      return newStatus

    return S_OK(reason)

  def getSite(self, elementType, elementName):
    """
    Given an element name, return its site
    """

    if elementType == 'StorageElement':
      elementType = 'SE'

    domainNames = gConfig.getSections('Resources/Sites')
    if not domainNames['OK']:
      return domainNames
    domainNames = domainNames['Value']

    for domainName in domainNames:

      sites = gConfig.getSections('Resources/Sites/%s' % domainName)
      if not sites['OK']:
        continue

      for site in sites['Value']:

        elements = gConfig.getValue('Resources/Sites/%s/%s/%s' % (domainName, site, elementType), '')
        if elementName in elements:
          return site

    return ''

  # ResourceManagementClient ...................................................

  types_getDowntimes = [basestring, basestring, basestring]

  def export_getDowntimes(self, element, elementType, name):

    if elementType == 'StorageElement':
      res = getSEHosts(name)
      if not res['OK']:
        return res
      names = res['Value']
    else:
      names = name

    return rmClient.selectDowntimeCache(element=element, name=names,
                                        meta={'columns': ['StartDate', 'EndDate',
                                                          'Link', 'Description',
                                                          'Severity']})

  types_getCachedDowntimes = [(basestring, types.NoneType, list),
                              (basestring, types.NoneType, list),
                              (basestring, types.NoneType, list),
                              (basestring, types.NoneType, list)]

  def export_getCachedDowntimes(self, element, elementType, name, severity):

    if elementType == 'StorageElement':
      res = getSEHosts(name)
      if not res['OK']:
        return res
      names = res['Value']
    else:
      names = name

    columns = ['Element', 'Name', 'StartDate', 'EndDate', 'Severity', 'Description', 'Link']

    res = rmClient.selectDowntimeCache(element=element, name=names, severity=severity,
                                       meta={'columns': columns})
    if not res['OK']:
      return res

    result = S_OK(res['Value'])
    result['Columns'] = columns

    return result

  types_setStatus = [basestring] * 7

  def export_setStatus(self, element, name, statusType, status, elementType, username, lastCheckTime):

    lastCheckTime = datetime.strptime(lastCheckTime, '%Y-%m-%d %H:%M:%S')

    credentials = self.getRemoteCredentials()
    gLogger.info(credentials)

    elementInDB = rsClient.selectStatusElement(element, 'Status', name=name,
                                               statusType=statusType,
                                               # status = status,
                                               elementType=elementType,
                                               lastCheckTime=lastCheckTime)
    if not elementInDB['OK']:
      return elementInDB
    elif not elementInDB['Value']:
      return S_ERROR('Your selection has been modified. Please refresh.')

    reason = 'Status %s forced by %s ( web )' % (status, username)
    tokenExpiration = datetime.utcnow() + timedelta(days=1)

    newStatus = rsClient.addOrModifyStatusElement(element, 'Status', name=name,
                                                  statusType=statusType,
                                                  status=status,
                                                  elementType=elementType,
                                                  reason=reason,
                                                  tokenOwner=username,
                                                  tokenExpiration=tokenExpiration)
    if not newStatus['OK']:
      return newStatus

    return S_OK(reason)
