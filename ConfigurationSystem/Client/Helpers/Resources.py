""" Helper for the CS Resources section
"""

__RCSID__ = "$Id$"

from distutils.version import LooseVersion  # pylint: disable=no-name-in-module,import-error

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.Core.Utilities.List import uniqueElements, fromChar
from DIRAC.Core.Utilities.Decorators import deprecated


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


def getFTS3Servers():
  """ get FTSServers for sites
  """

  csPath = cfgPath(gBaseResourcesSection, "FTSEndpoints/FTS3")
  # We do it in two times to keep the order
  ftsServerNames = gConfig.getOptions(csPath).get('Value', [])

  ftsServers = []
  for name in ftsServerNames:
    ftsServers.append(gConfig.getValue(cfgPath(csPath, name)))

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
        if comList and community not in comList:
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
          if not ceMode or ceMode != mode:
            continue
        if ceTypeList:
          ceType = gConfig.getValue('/Resources/Sites/%s/%s/CEs/%s/CEType' % (grid, site, ce), '')
          if not ceType or ceType not in ceTypeList:
            continue
        if ceList is not None and ce not in ceList:
          continue
        if community:
          comList = gConfig.getValue('/Resources/Sites/%s/%s/CEs/%s/VO' % (grid, site, ce), [])
          if comList and community not in comList:
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
            if comList and community not in comList:
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


def getCompatiblePlatforms(originalPlatforms):
  """ Get a list of platforms compatible with the given list
  """
  if isinstance(originalPlatforms, basestring):
    platforms = [originalPlatforms]
  else:
    platforms = list(originalPlatforms)

  platforms = list(platform.replace(' ', '') for platform in platforms)

  result = gConfig.getOptionsDict('/Resources/Computing/OSCompatibility')
  if not (result['OK'] and result['Value']):
    return S_ERROR("OS compatibility info not found")

  platformsDict = dict((k, v.replace(' ', '').split(',')) for k, v in result['Value'].iteritems())
  for k, v in platformsDict.iteritems():
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
  if isinstance(OSList, basestring):
    osList = [OSList]

  result = gConfig.getOptionsDict('/Resources/Computing/OSCompatibility')
  if not (result['OK'] and result['Value']):
    return S_ERROR("OS compatibility info not found")

  platformsDict = dict((k, v.replace(' ', '').split(',')) for k, v in result['Value'].iteritems())
  for k, v in platformsDict.iteritems():
    if k not in v:
      v.append(k)

  # making an OS -> platforms dict
  os2PlatformDict = dict()
  for platform, osItems in platformsDict.iteritems():
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
