""" Helper for the CS Resources section
"""

__RCSID__ = "$Id$"

import urlparse
from distutils.version import LooseVersion  # pylint: disable=no-name-in-module,import-error

import six

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.Core.Utilities.List import uniqueElements, fromChar


gBaseResourcesSection = "/Resources"


def getSites():
  """ Get the list of all the sites defined in the CS
  """
  result = gConfig.getSections(cfgPath(gBaseResourcesSection, 'Sites'))
  if not result['OK']:
    return result
  grids = result['Value']
  sites = []
  for grid in grids:
    result = gConfig.getSections(cfgPath(gBaseResourcesSection, 'Sites', grid))
    if not result['OK']:
      return result
    sites += result['Value']

  return S_OK(sites)


def getSiteCEMapping():
  """ :returns: dict of site: list of CEs
  """
  res = getSites()
  if not res['OK']:
    return res
  sites = res['Value']
  sitesCEsMapping = {}
  for site in sites:
    res = sitesCEsMapping[site] = gConfig.getSections(cfgPath(gBaseResourcesSection, 'Sites',
                                                              site.split('.')[0], site, 'CEs'),
                                                      [])
    if not res['OK']:
      return res
    sitesCEsMapping[site] = res['Value']
  return S_OK(sitesCEsMapping)


def getCESiteMapping(ceName=''):
  """ Returns a mapping of CE -> site
      It assumes that the ce names are unique (as they should be)

      :param str ceName: optional CE name. If not present, the whole mapping will be returned
      :return: S_OK/S_ERROR structure
  """
  res = getSiteCEMapping()
  if not res['OK']:
    return res
  sitesCEs = res['Value']
  ceSiteMapping = {}
  for site in sitesCEs:
    for ce in sitesCEs[site]:
      if ceName:
        if ce != ceName:
          continue
      ceSiteMapping[ce] = site
  return S_OK(ceSiteMapping)


def getGOCSiteName(diracSiteName):
  """
  Get GOC DB site name, given the DIRAC site name, as it is stored in the CS

  :param str diracSiteName: DIRAC site name (e.g. 'LCG.CERN.ch')
  :returns: S_OK/S_ERROR structure
  """
  gocDBName = gConfig.getValue(cfgPath(gBaseResourcesSection,
                                       'Sites',
                                       diracSiteName.split('.')[0],
                                       diracSiteName,
                                       'Name'))
  if not gocDBName:
    return S_ERROR("No GOC site name for %s in CS (Not a grid site ?)" % diracSiteName)
  return S_OK(gocDBName)


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


def getDIRACSiteName(gocSiteName):
  """
  Get DIRAC site name, given the GOC DB site name, as it stored in the CS

  :params str gocSiteName: GOC DB site name (e.g. 'CERN-PROD')
  :returns: S_OK/S_ERROR structure
  """
  res = getSites()
  if not res['OK']:
    return res
  sitesList = res['Value']

  tmpList = [(site, gConfig.getValue(cfgPath(gBaseResourcesSection,
                                             'Sites',
                                             site.split('.')[0],
                                             site,
                                             'Name'))) for site in sitesList]

  diracSites = [dirac for (dirac, goc) in tmpList if goc == gocSiteName]

  if diracSites:
    return S_OK(diracSites)

  return S_ERROR("There's no site with GOCDB name = %s in DIRAC CS" % gocSiteName)


def getGOCFTSName(diracFTSName):
  """
  Get GOC DB FTS server URL, given the DIRAC FTS server name, as it stored in the CS

  :param str diracFTSName: DIRAC FTS server name (e.g. 'CERN-FTS3')
  :returns: S_OK/S_ERROR structure
  """

  gocFTSName = gConfig.getValue(cfgPath(gBaseResourcesSection, 'FTSEndpoints', 'FTS3', diracFTSName))
  if not gocFTSName:
    return S_ERROR("No GOC FTS server name for %s in CS (Not a grid site ?)" % diracFTSName)
  return S_OK(gocFTSName)


def getFTS3Servers(hostOnly=False):
  """ get list of FTS3 servers that are in CS

      :param bool hostOnly: flag for stripping down the protocol and ports
  """

  csPath = cfgPath(gBaseResourcesSection, "FTSEndpoints/FTS3")
  # We do it in two times to keep the order
  ftsServerNames = gConfig.getOptions(csPath).get('Value', [])

  ftsServers = []
  for name in ftsServerNames:
    serverPath = gConfig.getValue(cfgPath(csPath, name))
    if hostOnly:
      serverPath = urlparse.urlparse(serverPath).hostname
    ftsServers.append(serverPath)

  return S_OK(ftsServers)


def getFTS3ServerDict():
  """:returns: dict of key = server name and value = server url
  """
  return gConfig.getOptionsDict(cfgPath(gBaseResourcesSection, "FTSEndpoints/FTS3"))


def getSiteTier(site):
  """
    Return Tier level of the given Site
  """
  result = getSitePath(site)
  if not result['OK']:
    return result
  sitePath = result['Value']
  return S_OK(gConfig.getValue(cfgPath(sitePath, 'MoUTierLevel'), 2))


def getSitePath(site):
  """
    Return path to the Site section on CS
  """
  result = getSiteGrid(site)
  if not result['OK']:
    return result
  grid = result['Value']
  return S_OK(cfgPath(gBaseResourcesSection, 'Sites', grid, site))


def getSiteGrid(site):
  """
   Return Grid component from Site Name
  """
  sitetuple = site.split(".")
  if len(sitetuple) != 3:
    return S_ERROR('Wrong Site Name format')
  return S_OK(sitetuple[0])


def getQueue(site, ce, queue):
  """ Get parameters of the specified queue
  """
  grid = site.split('.')[0]
  result = gConfig.getOptionsDict('/Resources/Sites/%s/%s/CEs/%s' % (grid, site, ce))
  if not result['OK']:
    return result
  resultDict = result['Value']

  # Get queue defaults
  result = gConfig.getOptionsDict('/Resources/Sites/%s/%s/CEs/%s/Queues/%s' % (grid, site, ce, queue))
  if not result['OK']:
    return result
  resultDict.update(result['Value'])

  # Handle tag lists for the queue
  for tagFieldName in ('Tag', 'RequiredTag'):
    tags = []
    ceTags = resultDict.get(tagFieldName)
    if ceTags:
      tags = fromChar(ceTags)
    queueTags = resultDict.get(tagFieldName)
    if queueTags:
      queueTags = fromChar(queueTags)
      tags = list(set(tags + queueTags))
    if tags:
      resultDict[tagFieldName] = tags

  resultDict['Queue'] = queue
  return S_OK(resultDict)


def getQueues(siteList=None, ceList=None, ceTypeList=None, community=None, mode=None):
  """ Get CE/queue options according to the specified selection
  """

  result = gConfig.getSections('/Resources/Sites')
  if not result['OK']:
    return result

  resultDict = {}

  grids = result['Value']
  for grid in grids:
    result = gConfig.getSections('/Resources/Sites/%s' % grid)
    if not result['OK']:
      continue
    sites = result['Value']
    for site in sites:
      if siteList is not None and site not in siteList:
        continue
      if community:
        comList = gConfig.getValue('/Resources/Sites/%s/%s/VO' % (grid, site), [])
        if comList and community.lower() not in [cl.lower() for cl in comList]:
          continue
      siteCEParameters = {}
      result = gConfig.getOptionsDict('/Resources/Sites/%s/%s/CEs' % (grid, site))
      if result['OK']:
        siteCEParameters = result['Value']
      result = gConfig.getSections('/Resources/Sites/%s/%s/CEs' % (grid, site))
      if not result['OK']:
        continue
      ces = result['Value']
      for ce in ces:
        if mode:
          ceMode = gConfig.getValue('/Resources/Sites/%s/%s/CEs/%s/SubmissionMode' % (grid, site, ce), 'Direct')
          if not ceMode or ceMode.lower() != mode.lower():
            continue
        if ceTypeList:
          ceType = gConfig.getValue('/Resources/Sites/%s/%s/CEs/%s/CEType' % (grid, site, ce), '')
          if not ceType or ceType not in ceTypeList:
            continue
        if ceList is not None and ce not in ceList:
          continue
        if community:
          comList = gConfig.getValue('/Resources/Sites/%s/%s/CEs/%s/VO' % (grid, site, ce), [])
          if comList and community.lower() not in [cl.lower() for cl in comList]:
            continue
        ceOptionsDict = dict(siteCEParameters)
        result = gConfig.getOptionsDict('/Resources/Sites/%s/%s/CEs/%s' % (grid, site, ce))
        if not result['OK']:
          continue
        ceOptionsDict.update(result['Value'])
        result = gConfig.getSections('/Resources/Sites/%s/%s/CEs/%s/Queues' % (grid, site, ce))
        if not result['OK']:
          continue
        queues = result['Value']
        for queue in queues:
          if community:
            comList = gConfig.getValue('/Resources/Sites/%s/%s/CEs/%s/Queues/%s/VO' % (grid, site, ce, queue), [])
            if comList and community.lower() not in [cl.lower() for cl in comList]:
              continue
          resultDict.setdefault(site, {})
          resultDict[site].setdefault(ce, ceOptionsDict)
          resultDict[site][ce].setdefault('Queues', {})
          result = gConfig.getOptionsDict('/Resources/Sites/%s/%s/CEs/%s/Queues/%s' % (grid, site, ce, queue))
          if not result['OK']:
            continue
          queueOptionsDict = result['Value']
          resultDict[site][ce]['Queues'][queue] = queueOptionsDict

  return S_OK(resultDict)


def getStorageElements(vo=None):
  """
  Get configuration of storage elements

  :param str vo: select SE's for the given VO

  :return: S_OK/S_ERROR, Value SE information dictionary
  """

  result = gConfig.getSections('Resources/StorageElements')
  if not result['OK']:
    return result
  storageElements = result['Value']
  baseSEs = {}
  for se in storageElements:
    for option in ('BaseSE', 'Alias'):
      originalSE = gConfig.getValue('Resources/StorageElements/%s/%s' % (se, option))
      if originalSE:
        baseSEs.setdefault(originalSE, []).append(se)
        break
    else:
      baseSEs.setdefault(se, [])

  seDict = {}
  for se in baseSEs:
    result = gConfig.getOptionsDict('Resources/StorageElements/%s' % se)
    if vo:
      seVOs = gConfig.getValue('Resources/StorageElements/%s/VO' % se, [])
      if seVOs and vo not in seVOs:
        continue
    seDict[se] = result['Value']
    seDict[se]['Aliases'] = baseSEs[se]
    protocols = []
    result = gConfig.getSections('Resources/StorageElements/%s' % se)
    for pluginSection in result['Value']:
      protocol = gConfig.getValue('Resources/StorageElements/%s/%s/Protocol' % (se, pluginSection))
      if protocol:
        protocols.append(protocol)
    seDict[se]['Protocols'] = protocols

  return S_OK(seDict)


def getCompatiblePlatforms(originalPlatforms):
  """ Get a list of platforms compatible with the given list
  """
  if isinstance(originalPlatforms, six.string_types):
    platforms = [originalPlatforms]
  else:
    platforms = list(originalPlatforms)

  platforms = list(platform.replace(' ', '') for platform in platforms)

  result = gConfig.getOptionsDict('/Resources/Computing/OSCompatibility')
  if not (result['OK'] and result['Value']):
    return S_ERROR("OS compatibility info not found")

  platformsDict = dict((k, v.replace(' ', '').split(',')) for k, v in result['Value'].items())  # can be an iterator
  for k, v in platformsDict.items():  # can be an iterator
    if k not in v:
      v.append(k)

  resultList = list(platforms)
  for p in platforms:
    tmpList = platformsDict.get(p, [])
    for pp in platformsDict:
      if p in platformsDict[pp]:
        tmpList.append(pp)
        tmpList += platformsDict[pp]
    if tmpList:
      resultList += tmpList

  return S_OK(uniqueElements(resultList))


def getDIRACPlatform(OSList):
  """ Get standard DIRAC platform(s) compatible with the argument.

      NB: The returned value is a list! ordered, in reverse, using distutils.version.LooseVersion
      In practice the "highest" version (which should be the most "desirable" one is returned first)

      :param list OSList: list of platforms defined by resource providers
      :return: a list of DIRAC platforms that can be specified in job descriptions
  """

  # For backward compatibility allow a single string argument
  osList = OSList
  if isinstance(OSList, six.string_types):
    osList = [OSList]

  result = gConfig.getOptionsDict('/Resources/Computing/OSCompatibility')
  if not (result['OK'] and result['Value']):
    return S_ERROR("OS compatibility info not found")

  platformsDict = dict((k, v.replace(' ', '').split(',')) for k, v in result['Value'].items())  # can be an iterator
  for k, v in platformsDict.items():  # can be an iterator
    if k not in v:
      v.append(k)

  # making an OS -> platforms dict
  os2PlatformDict = dict()
  for platform, osItems in platformsDict.items():  # can be an iterator
    for osItem in osItems:
      if os2PlatformDict.get(osItem):
        os2PlatformDict[osItem].append(platform)
      else:
        os2PlatformDict[osItem] = [platform]

  platforms = []
  for os in osList:
    if os in os2PlatformDict:
      platforms += os2PlatformDict[os]

  if not platforms:
    return S_ERROR('No compatible DIRAC platform found for %s' % ','.join(OSList))

  platforms.sort(key=LooseVersion, reverse=True)

  return S_OK(platforms)


def getDIRACPlatforms():
  """ just returns list of platforms defined in the CS
  """
  result = gConfig.getOptionsDict('/Resources/Computing/OSCompatibility')
  if not (result['OK'] and result['Value']):
    return S_ERROR("OS compatibility info not found")
  return S_OK(result['Value'].keys())


def getCatalogPath(catalogName):
  """  Return the configuration path of the description for a a given catalog
  """
  return '/Resources/FileCatalogs/%s' % catalogName


def getBackendConfig(backendID):
  """ Return a backend configuration for a given backend identifier

  :params backendID: string representing a backend identifier. Ex: stdout, file, f02
  """
  return gConfig.getOptionsDict('Resources/LogBackends/%s' % backendID)


def getFilterConfig(filterID):
  """Return a filter configuration for a given filter identifier.

  :params filterID: string representing a filter identifier.
  """
  return gConfig.getOptionsDict('Resources/LogFilters/%s' % filterID)


def getProvidersForInstance(instance, providerType=None):
  """ Get providers for instance

      :param str instance: instance of what this providers
      ;param str providerType: provider type

      :return: S_OK(list)/S_ERROR()
  """
  result = gConfig.getSections('%s/%sProviders' % (gBaseResourcesSection, instance))
  if not result['OK'] or not result['Value'] or (result['OK'] and not providerType):
    return result
  data = []
  for prov in result['Value']:
    if providerType == gConfig.getValue('%s/%sProviders/%s/ProviderType' %
                                        (gBaseResourcesSection, instance, prov)):
      data.append(prov)
  return S_OK(data)


def getProviderByAlias(alias, instance=None):
  """ Find provider name by alias

      :param str alias: other registered provider name
      :param str instance: provider of what

      :return: S_OK(str)/S_ERROR()
  """
  instances = [instance] or []
  if not instances:
    result = gConfig.getSections(gBaseResourcesSection)
    if not result['OK']:
      return result
    for section in result['Value']:
      if section[-9:] == 'Providers':
        instances.append(section[:-9])
  for instance in instances:
    result = getProvidersForInstance(instance)
    if not result['OK']:
      return result
    for provider in result['Value']:
      if alias in gConfig.getValue("%s/%sProviders/%s/Aliases" % (gBaseResourcesSection,
                                                         instance, provider), []):
        return S_OK(provider)
  return S_ERROR('No found any provider for %s' % alias)


def getProviderInfo(provider):
  """ Get provider info
  
      :param str provider: provider

      :return: S_OK(dict)/S_ERROR()
  """
  result = gConfig.getSections(gBaseResourcesSection)
  if not result['OK']:
    return result
  for section in result['Value']:
    if section[-9:] == 'Providers':
      result = getProvidersForInstance(section[:-9])
      if not result['OK']:
        return result
      if provider in result['Value']:
        return gConfig.getOptionsDictRecursively("%s/%s/%s/" % (gBaseResourcesSection,
                                                                section, provider))
  return S_ERROR('%s provider not found.' % provider)
