""" PublisherHandler

This service has been built to provide the RSS web views with all the information
they need. NO OTHER COMPONENT THAN Web controllers should make use of it.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

#  pylint: disable=no-self-use

import six
from datetime import datetime, timedelta

# DIRAC
from DIRAC import S_OK, gConfig, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.SiteSEMapping import getSEHosts, getStorageElementsHosts
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites, getSiteCEMapping
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities import Utils
ResourceManagementClient = getattr(
    Utils.voimport('DIRAC.ResourceStatusSystem.Client.ResourceManagementClient'),
    'ResourceManagementClient')


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

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    """Initialization of client objects
    """
    cls.rsClient = ResourceStatusClient()
    cls.rmClient = ResourceManagementClient()

    return S_OK()

  types_getSites = []

  def export_getSites(self):
    """
    Returns list of all sites considered by RSS

    :return: S_OK( [ sites ] ) | S_ERROR
    """

    return getSites()

  types_getSitesResources = [(six.string_types, list, type(None))]

  def export_getSitesResources(self, siteNames):
    """
    Returns dictionary with SEs and CEs for the given site(s). If siteNames is
    None, all sites are taken into account.

    :return: S_OK( { site1 : { ces : [ ces ], 'ses' : [ ses  ] },... } ) | S_ERROR
    """

    if siteNames is None:
      res = getSites()
      if not res['OK']:
        self.log.error("Error getting sites", res['Message'])
        return res
      siteNames = res['Value']

    if isinstance(siteNames, six.string_types):
      siteNames = [siteNames]

    sitesRes = {}
    for siteName in siteNames:

      result = getSiteCEMapping()
      if not result['OK']:
        self.log.error("Error getting sites/CEs mapping", result['Message'])
        return result
      res = {}
      res['ces'] = result['Value'][siteName]
      # Convert StorageElements to host names
      result = DMSHelpers().getSiteSEMapping()
      if not result['OK']:
        self.log.error("Error getting sites/SEs mapping", result['Message'])
        sitesRes[siteName] = res
        continue
      ses = result['Value'][1].get(siteName, [])
      result = getStorageElementsHosts(ses)
      if not result['OK']:
        self.log.error("Error getting storage element hosts", result['Message'])
        return result
      # Remove duplicates
      res['ses'] = list(set(result['Value']))

      sitesRes[siteName] = res

    return S_OK(sitesRes)

  types_getElementStatuses = [six.string_types,
                              (six.string_types, list, type(None)),
                              (six.string_types, list, type(None)),
                              (six.string_types, list, type(None)),
                              (six.string_types, list, type(None)),
                              (six.string_types, list, type(None))]

  @classmethod
  def export_getElementStatuses(cls, element, name, elementType, statusType, status, tokenOwner):
    """
    Returns element statuses from the ResourceStatusDB
    """

    return cls.rsClient.selectStatusElement(
        element, 'Status', name=name, elementType=elementType,
        statusType=statusType, status=status, tokenOwner=tokenOwner)

  types_getElementHistory = [six.string_types,
                             (six.string_types, list, type(None)),
                             (six.string_types, list, type(None)),
                             (six.string_types, list, type(None))]

  @classmethod
  def export_getElementHistory(cls, element, name, elementType, statusType):
    """
    Returns element history from ResourceStatusDB
    """

    columns = ['Status', 'DateEffective', 'Reason']
    return cls.rsClient.selectStatusElement(
        element, 'History', name=name, elementType=elementType,
        statusType=statusType, meta={'columns': columns})

  types_getElementPolicies = [six.string_types,
                              (six.string_types, list, type(None)),
                              (six.string_types, list, type(None))]

  @classmethod
  def export_getElementPolicies(cls, element, name, statusType):
    """
    Returns policies for a given element
    """

    columns = ['Status', 'PolicyName', 'DateEffective', 'LastCheckTime', 'Reason']
    return cls.rmClient.selectPolicyResult(element=element, name=name,
                                           statusType=statusType,
                                           meta={'columns': columns})

  types_getNodeStatuses = []

  @classmethod
  def export_getNodeStatuses(cls):
    return cls.rsClient.selectStatusElement('Node', 'Status')

  types_getTree = [six.string_types, six.string_types]

  def export_getTree(self, elementType, elementName):
    """
    Given an element type and name,
    finds its parent site and returns all descendants of that site.
    """

    site = self.getSite(elementType, elementName)
    if not site:
      return S_ERROR('No site')

    siteStatus = self.rsClient.selectStatusElement(
        'Site', 'Status', name=site, meta={'columns': ['StatusType', 'Status']})
    if not siteStatus['OK']:
      return siteStatus

    tree = {site: {'statusTypes': dict(siteStatus['Value'])}}

    result = getSiteCEMapping()
    if not result['OK']:
      return result
    ces = result['Value'][site]
    cesStatus = self.rsClient.selectStatusElement(
        'Resource', 'Status',
        name=ces, meta={'columns': ['Name', 'StatusType', 'Status']})
    if not cesStatus['OK']:
      return cesStatus

    res = DMSHelpers().getSiteSEMapping()
    if not res['OK']:
      self.log.error('Could not get site to SE mapping', res['Message'])
      return S_OK()
    ses = res['Value'][1].get(site, [])
    sesStatus = self.rsClient.selectStatusElement(
        'Resource', 'Status', name=list(ses),
        meta={'columns': ['Name', 'StatusType', 'Status']})
    if not sesStatus['OK']:
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

  types_setToken = [six.string_types] * 7

  def export_setToken(self, element, name, statusType, token, elementType, username, lastCheckTime):

    lastCheckTime = datetime.strptime(lastCheckTime, '%Y-%m-%d %H:%M:%S')

    credentials = self.getRemoteCredentials()
    self.log.info(credentials)

    elementInDB = self.rsClient.selectStatusElement(
        element, 'Status',
        name=name, statusType=statusType,
        elementType=elementType, lastCheckTime=lastCheckTime)
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

    newStatus = self.rsClient.addOrModifyStatusElement(
        element, 'Status',
        name=name, statusType=statusType, elementType=elementType,
        reason=reason, tokenOwner=tokenOwner, tokenExpiration=tokenExpiration)
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

        elements = gConfig.getValue(
            'Resources/Sites/%s/%s/%s' % (domainName, site, elementType), '')
        if elementName in elements:
          return site

    return ''

  # ResourceManagementClient ...................................................

  types_getDowntimes = [six.string_types, six.string_types, six.string_types]

  @classmethod
  def export_getDowntimes(cls, element, elementType, name):

    if elementType == 'StorageElement':
      res = getSEHosts(name)
      if not res['OK']:
        return res
      names = res['Value']
    else:
      names = name

    return cls.rmClient.selectDowntimeCache(
        element=element, name=names,
        meta={'columns': ['StartDate', 'EndDate', 'Link', 'Description', 'Severity']})

  types_getCachedDowntimes = [(six.string_types, type(None), list),
                              (six.string_types, type(None), list),
                              (six.string_types, type(None), list),
                              (six.string_types, type(None), list)]

  def export_getCachedDowntimes(self, element, elementType, name, severity):

    if elementType == 'StorageElement':
      res = getSEHosts(name)
      if not res['OK']:
        return res
      names = res['Value']
    else:
      names = name

    columns = ['Element', 'Name', 'StartDate', 'EndDate', 'Severity', 'Description', 'Link']

    res = self.rmClient.selectDowntimeCache(
        element=element, name=names, severity=severity, meta={'columns': columns})
    if not res['OK']:
      self.log.error("Error selecting downtime cache", res['Message'])
      return res

    result = S_OK(res['Value'])
    result['Columns'] = columns

    return result

  types_setStatus = [six.string_types] * 7

  def export_setStatus(self, element, name, statusType, status, elementType, username, lastCheckTime):

    if not lastCheckTime:
      lastCheckTime = None
    else:
      lastCheckTime = datetime.strptime(lastCheckTime, '%Y-%m-%d %H:%M:%S')

    credentials = self.getRemoteCredentials()
    self.log.info(credentials)

    elementInDB = self.rsClient.selectStatusElement(
        element, 'Status',
        name=name, statusType=statusType,  # status = status,
        elementType=elementType, lastCheckTime=lastCheckTime)
    if not elementInDB['OK']:
      self.log.error("Error selecting status elements", elementInDB['Message'])
      return elementInDB
    elif not elementInDB['Value']:
      return S_ERROR('Your selection has been modified. Please refresh.')

    reason = 'Status %s forced by %s ( web )' % (status, username)
    tokenExpiration = datetime.utcnow() + timedelta(days=1)

    newStatus = self.rsClient.addOrModifyStatusElement(
        element, 'Status',
        name=name, statusType=statusType, status=status,
        elementType=elementType, reason=reason,
        tokenOwner=username, tokenExpiration=tokenExpiration)
    if not newStatus['OK']:
      self.log.error("Error setting status", newStatus['Message'])
      return newStatus

    return S_OK(reason)
