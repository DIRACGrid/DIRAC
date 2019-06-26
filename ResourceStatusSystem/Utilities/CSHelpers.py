""" CSHelpers

  Module containing functions interacting with the CS and useful for the RSS
  modules.
"""

__RCSID__ = '$Id$'

from DIRAC import gConfig, gLogger, S_OK
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.Core.Utilities.SiteSEMapping import getSEParameters
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueues
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.ResourceStatusSystem.Utilities import Utils


def warmUp():
  """
    gConfig has its own dark side, it needs some warm up phase.
  """
  from DIRAC.ConfigurationSystem.private.Refresher import gRefresher
  gRefresher.refreshConfigurationIfNeeded()


@deprecated("Use DIRAC.ConfigurationSystem.Client.Helpers.Resources.getSites() instead")
def getSites():
  """
    Gets all sites from /Resources/Sites
  """

  _basePath = 'Resources/Sites'

  sites = []

  domainNames = gConfig.getSections(_basePath)
  if not domainNames['OK']:
    return domainNames
  domainNames = domainNames['Value']

  for domainName in domainNames:
    domainSites = gConfig.getSections('%s/%s' % (_basePath, domainName))
    if not domainSites['OK']:
      return domainSites

    domainSites = domainSites['Value']

    sites.extend(domainSites)

  # Remove duplicated ( just in case )
  sites = list(set(sites))
  return S_OK(sites)


@deprecated("unused")
def getDomainSites():
  """
    Gets all sites from /Resources/Sites
  """

  _basePath = 'Resources/Sites'

  sites = {}

  domainNames = gConfig.getSections(_basePath)
  if not domainNames['OK']:
    return domainNames
  domainNames = domainNames['Value']

  for domainName in domainNames:
    domainSites = gConfig.getSections('%s/%s' % (_basePath, domainName))
    if not domainSites['OK']:
      return domainSites

    domainSites = domainSites['Value']

    sites[domainName] = domainSites

  return S_OK(sites)


def getResources():
  """
    Gets all resources
  """

  resources = DMSHelpers().getStorageElements()

  fts = getFTS()
  if fts['OK']:
    resources = resources + fts['Value']

  fc = getFileCatalogs()
  if fc['OK']:
    resources = resources + fc['Value']

  ce = getComputingElements()
  if ce['OK']:
    resources = resources + ce['Value']

  return S_OK(resources)


@deprecated("unused")
def getNodes():
  """
    Gets all nodes
  """

  nodes = []

  queues = getQueues()
  if queues['OK']:
    nodes = nodes + queues['Value']

  return S_OK(nodes)


@deprecated("unused")
def getStorageElements():
  """
    Gets all storage elements from /Resources/StorageElements
  """

  _basePath = 'Resources/StorageElements'

  seNames = gConfig.getSections(_basePath)
  return seNames


@deprecated("unused")
def getSEToken(seName):
  """ Get StorageElement token
  """

  seParameters = getSEParameters(seName)
  if not seParameters['OK']:
    gLogger.warn("Could not get SE parameters", "SE: %s" % seName)
    return seParameters

  return S_OK(seParameters['Value']['SpaceToken'])


def getStorageElementEndpoint(seName):
  """ Get endpoints of a StorageElement

      :param str seName: name of the storage element

      :returns: S_OK() or S_ERROR
                for historical reasons, if the protocol is SRM, you get  'httpg://host:port/WSUrl'
                For other protocols, you get :py:meth:`~DIRAC.Resources.Storage.StorageBase.StorageBase.getEndpoint`

  """
  seParameters = getSEParameters(seName)
  if not seParameters['OK']:
    gLogger.warn("Could not get SE parameters", "for SE %s" % seName)
    return seParameters

  seEndpoints = []

  for parameters in seParameters['Value']:
    if parameters['Protocol'].lower() == 'srm':
      # we need to construct the URL with httpg://
      host = parameters['Host']
      port = parameters['Port']
      wsurl = parameters['WSUrl']
      # MAYBE wusrl is not defined
      if host and port:
        url = 'httpg://%s:%s%s' % (host, port, wsurl)
        url = url.replace('?SFN=', '')
        seEndpoints.append(url)
    else:
      seEndpoints.append(parameters['Endpoint'])

  return S_OK(seEndpoints)


@deprecated("unused")
def getStorageElementEndpoints(storageElements=None):
  """ get the endpoints of the Storage ELements
  """

  if storageElements is None:
    storageElements = DMSHelpers().getStorageElements()

  storageElementEndpoints = []

  for se in storageElements:

    seEndpoint = getStorageElementEndpoint(se)
    if not seEndpoint['OK']:
      continue
    storageElementEndpoints.append(seEndpoint['Value'])

  return S_OK(list(set(storageElementEndpoints)))


def getFTS():
  """
    Gets all FTS endpoints
  """

  # FIXME: FTS2 will be deprecated (first 2 lines that follow)
  ftsEndpoints = gConfig.getValue('Resources/FTSEndpoints/Default/FTSEndpoint', [])
  ftsEndpoints += _getFTSEndpoints('Resources/FTSEndpoints/FTS2')
  ftsEndpoints += _getFTSEndpoints()

  return S_OK(ftsEndpoints)


def _getFTSEndpoints(basePath='Resources/FTSEndpoints/FTS3'):
  """
    Gets all FTS endpoints that are in CS
  """

  result = gConfig.getOptions(basePath)
  if result['OK']:
    return result['Value']
  return []


def getSpaceTokenEndpoints():
  """ Get Space Token Endpoints """

  return Utils.getCSTree('Shares/Disk')


def getFileCatalogs():
  """
    Gets all storage elements from /Resources/FileCatalogs
  """

  _basePath = 'Resources/FileCatalogs'

  fileCatalogs = gConfig.getSections(_basePath)
  return fileCatalogs


def getComputingElements():
  """
    Gets all computing elements from /Resources/Sites/<>/<>/CE
  """
  _basePath = 'Resources/Sites'

  ces = []

  domainNames = gConfig.getSections(_basePath)
  if not domainNames['OK']:
    return domainNames
  domainNames = domainNames['Value']

  for domainName in domainNames:
    domainSites = gConfig.getSections('%s/%s' % (_basePath, domainName))
    if not domainSites['OK']:
      return domainSites
    domainSites = domainSites['Value']

    for site in domainSites:
      siteCEs = gConfig.getSections('%s/%s/%s/CEs' % (_basePath, domainName, site))
      if not siteCEs['OK']:
        # return siteCEs
        gLogger.error(siteCEs['Message'])
        continue
      siteCEs = siteCEs['Value']
      ces.extend(siteCEs)

  # Remove duplicated ( just in case )
  ces = list(set(ces))

  return S_OK(ces)


def getSiteComputingElements(siteName):
  """
    Gets all computing elements from /Resources/Sites/<>/<siteName>/CE
  """

  _basePath = 'Resources/Sites'

  domainNames = gConfig.getSections(_basePath)
  if not domainNames['OK']:
    return domainNames
  domainNames = domainNames['Value']

  for domainName in domainNames:
    ces = gConfig.getValue('%s/%s/%s/CE' % (_basePath, domainName, siteName), '')
    if ces:
      return ces.split(', ')

  return []


@deprecated("Use DIRAC.Core.Utilities.SiteSEMapping.getSEsForSite() instead")
def getSiteStorageElements(siteName):
  """
    Gets all storage elements from /Resources/Sites/<>/<siteName>/SE

      Used by WebApp/SiteSummaryHandler.py
  """

  _basePath = 'Resources/Sites'

  domainNames = gConfig.getSections(_basePath)
  if not domainNames['OK']:
    return domainNames
  domainNames = domainNames['Value']

  for domainName in domainNames:
    ses = gConfig.getValue('%s/%s/%s/SE' % (_basePath, domainName, siteName), '')
    if ses:
      return ses.split(', ')

  return []


def getSiteElements(siteName):
  """
    Gets all the computing and storage elements for a given site
  """

  res = DMSHelpers().getSiteSEMapping()
  if not res['OK']:
    return res
  resources = res['Value'][1].get(siteName, [])

  res = getQueues(siteName)
  if not res['OK']:
    return res
  resources = list(resources) + list(res['Value'][siteName])

  return S_OK(resources)


def getQueuesRSS():
  """
    Gets all computing elements from /Resources/Sites/<>/<>/CE/Queues
  """
  _basePath = 'Resources/Sites'

  queues = []

  domainNames = gConfig.getSections(_basePath)
  if not domainNames['OK']:
    return domainNames
  domainNames = domainNames['Value']

  for domainName in domainNames:
    domainSites = gConfig.getSections('%s/%s' % (_basePath, domainName))
    if not domainSites['OK']:
      return domainSites
    domainSites = domainSites['Value']

    for site in domainSites:
      siteCEs = gConfig.getSections('%s/%s/%s/CEs' % (_basePath, domainName, site))
      if not siteCEs['OK']:
        # return siteCEs
        gLogger.error(siteCEs['Message'])
        continue
      siteCEs = siteCEs['Value']

      for siteCE in siteCEs:
        siteQueue = gConfig.getSections('%s/%s/%s/CEs/%s/Queues' % (_basePath, domainName, site, siteCE))
        if not siteQueue['OK']:
          # return siteQueue
          gLogger.error(siteQueue['Message'])
          continue
        siteQueue = siteQueue['Value']

        queues.extend(siteQueue)

  # Remove duplicated ( just in case )
  queues = list(set(queues))

  return S_OK(queues)


@deprecated("unused")
def getRegistryUsers():
  """
    Gets all users from /Registry/Users
  """

  _basePath = 'Registry/Users'

  registryUsers = {}

  userNames = gConfig.getSections(_basePath)
  if not userNames['OK']:
    return userNames
  userNames = userNames['Value']

  for userName in userNames:

    # returns { 'Email' : x, 'DN': y, 'CA' : z }
    userDetails = gConfig.getOptionsDict('%s/%s' % (_basePath, userName))
    if not userDetails['OK']:
      return userDetails

    registryUsers[userName] = userDetails['Value']

  return S_OK(registryUsers)
