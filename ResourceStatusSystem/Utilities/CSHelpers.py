""" CSHelpers

  Module containing functions interacting with the CS and useful for the RSS
  modules.
"""

__RCSID__ = '$Id$'

import errno

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getGOCSiteName
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueues
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.ResourceStatusSystem.Utilities import Utils
from DIRAC.Resources.Storage.StorageElement import StorageElement


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


def getGOCSites(diracSites=None):

  if diracSites is None:
    diracSites = getSites()
    if not diracSites['OK']:
      return diracSites
    diracSites = diracSites['Value']

  gocSites = []

  for diracSite in diracSites:
    gocSite = getGOCSiteName(diracSite)
    if not gocSite['OK']:
      continue
    gocSites.append(gocSite['Value'])

  return S_OK(list(set(gocSites)))


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


def getStorageElementsHosts(seNames=None):
  """ Get the hosts of the Storage Elements
  """

  seHosts = []

  if seNames is None:
    seNames = DMSHelpers().getStorageElements()

  for seName in seNames:

    seHost = getSEHost(seName)
    if not seHost['OK']:
      gLogger.warn("Could not get SE Host", "SE: %s" % seName)
      continue
    if seHost['Value']:
      seHosts.append(seHost['Value'])

  return S_OK(list(set(seHosts)))


def _getSEParameters(seName):
  se = StorageElement(seName, hideExceptions=True)

  seParameters = S_ERROR(errno.ENODATA, 'No SE parameters obtained')
  pluginsList = se.getPlugins()
  if not pluginsList['OK']:
    gLogger.warn(pluginsList['Message'], "SE: %s" % seName)
    return pluginsList
  pluginsList = pluginsList['Value']
  # Put the srm capable protocol first, but why doing that is a
  # mystery that will eventually need to be sorted out...
  for plugin in ('GFAL2_SRM2', 'SRM2'):
    if plugin in pluginsList:
      pluginsList.remove(plugin)
      pluginsList.insert(0, plugin)

  for plugin in pluginsList:
    seParameters = se.getStorageParameters(plugin)
    if seParameters['OK']:
      break

  return seParameters


def getSEToken(seName):
  """ Get StorageElement token
  """

  seParameters = _getSEParameters(seName)
  if not seParameters['OK']:
    gLogger.warn("Could not get SE parameters", "SE: %s" % seName)
    return seParameters

  return S_OK(seParameters['Value']['SpaceToken'])


def getSEHost(seName):
  """ Get StorageElement host name
  """

  seParameters = _getSEParameters(seName)
  if not seParameters['OK']:
    gLogger.warn("Could not get SE parameters", "SE: %s" % seName)
    return seParameters

  return S_OK(seParameters['Value']['Host'])


def getStorageElementEndpoint(seName):
  """ Get endpoint as combination of host, port, wsurl
  """
  seParameters = _getSEParameters(seName)
  if not seParameters['OK']:
    gLogger.warn("Could not get SE parameters", "SE: %s" % seName)
    return seParameters

  if seParameters['Value']['Protocol'].lower() == 'srm':
    # we need to construct the URL with httpg://
    host = seParameters['Value']['Host']
    port = seParameters['Value']['Port']
    wsurl = seParameters['Value']['WSUrl']
    # MAYBE wusrl is not defined
    if host and port:
      url = 'httpg://%s:%s%s' % (host, port, wsurl)
      url = url.replace('?SFN=', '')
      return S_OK(url)
  else:
    return S_OK(seParameters['Value']['URLBase'])

  return S_ERROR((host, port, wsurl))


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


# NOTE: this method is used by Web
def getSiteStorageElements(siteName):
  """
    Gets all computing elements from /Resources/Sites/<>/<siteName>/SE
  """

  _basePath = 'Resources/Sites'

  domainNames = gConfig.getSections(_basePath)
  if not domainNames['OK']:
    return domainNames
  domainNames = domainNames['Value']

  for domainName in domainNames:
    ses = gConfig.getValue('%s/%s/%s/SE' % (_basePath, domainName, siteName), '')
    if ses:
      return ses.split(',')

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
