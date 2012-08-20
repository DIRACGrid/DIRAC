# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC                                              import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Path      import cfgPath
from DIRAC.Core.Utilities.List                          import uniqueElements
import re
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

# the following methods must be moved here ...
from DIRAC.Core.Utilities.SiteSEMapping                 import getSEsForSite, getSitesForSE

gBaseResourcesSection = "/Resources"

def getSiteTier( site ):
  """
    Return Tier level of the given Site
  """
  result = getSitePath( site )
  if not result['OK']:
    return result
  sitePath = result['Value']
  return S_OK( gConfig.getValue( cfgPath( sitePath, 'MoUTierLevel' ), 2 ) )

def getSitePath( site ):
  """
    Return path to the Site section on CS
  """
  result = getSiteGrid( site )
  if not result['OK']:
    return result
  grid = result['Value']
  return S_OK( cfgPath( gBaseResourcesSection, 'Sites', grid, site ) )

def getSiteGrid( site ):
  """
   Return Grid component from Site Name
  """
  sitetuple = site.split( "." )
  if len( sitetuple ) != 3:
    return S_ERROR( 'Wrong Site Name format' )
  return S_OK( sitetuple[0] )

def getStorageElementOptions( seName ):
  """ Get the CS StorageElementOptions
  """
  storageConfigPath = '/Resources/StorageElements/%s' % seName
  result = gConfig.getOptionsDict( storageConfigPath )
  if not result['OK']:
    return result
  options = result['Value']

  # Help distinguishing storage type
  diskSE = True
  tapeSE = False
  if options.has_key( 'SEType' ):
    # Type should follow the convention TXDY
    seType = options['SEType']
    diskSE = re.search( 'D[1-9]', seType ) != None
    tapeSE = re.search( 'T[1-9]', seType ) != None
  options['DiskSE'] = diskSE
  options['TapeSE'] = tapeSE

  return S_OK( options )

def getQueues( siteList = None, ceList = None, ceTypeList = None, community = None, mode = None ):
  """ Get CE/queue options according to the specified selection
  """

  result = gConfig.getSections( '/Resources/Sites' )
  if not result['OK']:
    return result

  resultDict = {}

  grids = result['Value']
  for grid in grids:
    result = gConfig.getSections( '/Resources/Sites/%s' % grid )
    if not result['OK']:
      continue
    sites = result['Value']
    for site in sites:
      if siteList is not None and not site in siteList:
        continue
      if community:
        comList = gConfig.getValue( '/Resources/Sites/%s/%s/VO' % ( grid, site ), [] )
        if comList and not community in comList:
          continue
      result = gConfig.getSections( '/Resources/Sites/%s/%s/CEs' % ( grid, site ) )
      if not result['OK']:
        continue
      ces = result['Value']
      for ce in ces:
        if mode:
          ceMode = gConfig.getValue( '/Resources/Sites/%s/%s/CEs/%s/SubmissionMode' % ( grid, site, ce ), 'InDirect' )
          if not ceMode or ceMode != mode:
            continue
        if ceTypeList:
          ceType = gConfig.getValue( '/Resources/Sites/%s/%s/CEs/%s/CEType' % ( grid, site, ce ), '' )
          if not ceType or not ceType in ceTypeList:
            continue
        if community:
          comList = gConfig.getValue( '/Resources/Sites/%s/%s/CEs/%s/VO' % ( grid, site, ce ), [] )
          if comList and not community in comList:
            continue
        result = gConfig.getOptionsDict( '/Resources/Sites/%s/%s/CEs/%s' % ( grid, site, ce ) )
        if not result['OK']:
          continue
        ceOptionsDict = result['Value']
        result = gConfig.getSections( '/Resources/Sites/%s/%s/CEs/%s/Queues' % ( grid, site, ce ) )
        if not result['OK']:
          continue
        queues = result['Value']
        for queue in queues:
          if community:
            comList = gConfig.getValue( '/Resources/Sites/%s/%s/CEs/%s/Queues/%s/VO' % ( grid, site, ce, queue ), [] )
            if comList and not community in comList:
              continue
          resultDict.setdefault( site, {} )
          resultDict[site].setdefault( ce, ceOptionsDict )
          resultDict[site][ce].setdefault( 'Queues', {} )
          result = gConfig.getOptionsDict( '/Resources/Sites/%s/%s/CEs/%s/Queues/%s' % ( grid, site, ce, queue ) )
          if not result['OK']:
            continue
          queueOptionsDict = result['Value']
          resultDict[site][ce]['Queues'][queue] = queueOptionsDict

  return S_OK( resultDict )

def getCompatiblePlatforms( originalPlatforms ):
  """ Get a list of platforms compatible with the given list
  """
  if type( originalPlatforms ) == type( ' ' ):
    platforms = [originalPlatforms]
  else:
    platforms = list( originalPlatforms )

  platformDict = {}
  result = gConfig.getOptionsDict( '/Resources/Computing/OSCompatibility' )
  if result['OK'] and result['Value']:
    platformDict = result['Value']
    for platform in platformDict:
      platformDict[platform] = [ x.strip() for x in platformDict[platform].split( ',' ) ]
  else:
    return S_ERROR( 'OS compatibility info not found' )

  resultList = list( platforms )
  for p in platforms:
    tmpList = platformDict.get( p, [] )
    for pp in platformDict:
      if p in platformDict[pp]:
        tmpList.append( pp )
        tmpList += platformDict[pp]
    if tmpList:
      resultList += tmpList

  return S_OK( uniqueElements( resultList ) )

def getDIRACPlatform( platform ):
  """ Get standard DIRAC platform compatible with the argument
  """
  platformDict = {}
  result = gConfig.getOptionsDict( '/Resources/Computing/OSCompatibility' )
  if result['OK'] and result['Value']:
    platformDict = result['Value']
    for platform in platformDict:
      platformDict[platform] = [ x.strip() for x in platformDict[platform].split( ',' ) ]
  else:
    return S_ERROR( 'OS compatibility info not found' )

  resultPlatform = ''
  for p in platformDict:
    if platform in platformDict[p]:
      resultPlatform = p

  return S_OK( resultPlatform )

def getCatalogPath( catalogName ):
  """  Return the configuration path of the description for a a given catalog
  """
  return '/Resources/FileCatalogs/%s' % catalogName

#############################################################################
def getSitesGroupedByTierLevel(siteName=''):
  """
    It builds a dictionary which key is the tier level obtained from the CS using the MoUTierLevel attribute.
    By default the Tier level is 3. The Tier level retrieved from the CS can be Tier0, Tier1, Tier2, Tier3
  """

  tierSiteMapping = {}

  gridTypes = gConfig.getSections('/Resources/Sites/')
  if not gridTypes['OK']:
    gLogger.warn('Problem retrieving sections in /Resources/Sites')
    return gridTypes

  gridTypes = gridTypes['Value']

  gLogger.debug('Grid Types are: %s' % (', '.join(gridTypes)))
  for grid in gridTypes:
    sites = gConfig.getSections('/Resources/Sites/%s' % grid)
    if not sites['OK']: #gConfig returns S_ERROR for empty sections until version
      gLogger.warn('Problem retrieving /Resources/Sites/%s section' % grid)
      return sites
    if sites:
      if siteName:
        if siteName in sites['Value']:
          tierLevel = gConfig.getValue('/Resources/Sites/%s/%s/MoUTierLevel' % (grid, siteName), 3)
          tierkey = 'Tier%d' % tierLevel
          if tierkey not in tierSiteMapping:
            tierSiteMapping[tierkey] = []
          tierSiteMapping[tierkey] += [siteName]
      else:
        for candidate in sites['Value']:
          tierLevel = gConfig.getValue('/Resources/Sites/%s/%s/MoUTierLevel' % (grid, candidate), 3)
          tierkey = 'Tier%d' % tierLevel
          if tierkey not in tierSiteMapping:
            tierSiteMapping[tierkey] = []
          tierSiteMapping[tierkey] += [candidate]
  return S_OK(tierSiteMapping)

#############################################################################
def getTier1WithAttachedTier2(siteName=''):
  """ this method iterates on the T2 sites and check the SE of the T2.
      In case a SE is found then the T2 is attached to the corresponding T1
  """
  tier1andTier2Maps = {}
  tiers = getSitesGroupedByTierLevel(siteName)
  if not tiers['OK']:
    return tiers

  tier1 = []
  if 'Tier0' in tiers['Value']:
    tier1 += tiers['Value']['Tier0']

  if 'Tier1' in tiers['Value']:
    tier1 += tiers['Value']['Tier1']

  tier2s = getSitesGroupedByTierLevel()

  if not tier2s['OK']:
    return tier2s

  tier2s = tier2s['Value']['Tier2']
  for site in tier1:
    t1SE = getSEsForSite(site)
    if not t1SE['OK']:
      return t1SE

    t1SE = t1SE['Value']
    for tier2 in tier2s:
      t2SE = getSEsForSite(tier2)
      if not t2SE['OK']:
        return t2SE

      t2SE = t2SE['Value']
      if len(t2SE) > 0:
        if isOneSEFound(t1SE, t2SE):
          if site not in tier1andTier2Maps:
            tier1andTier2Maps[site] = []
          tier1andTier2Maps[site] += [tier2]

  return S_OK(tier1andTier2Maps)

#############################################################################
def getTier1WithTier2(siteName=''):
  """
  It returns the T1 sites with the attached T2 using the SiteLocalSEMapping
  """
  tier1andTier2Maps = {}
  retVal = Operations().getOptionsDict('SiteLocalSEMapping')
  if not retVal['OK']:
    return retVal
  else:
    storages = retVal['Value']

  tiers = getSitesGroupedByTierLevel(siteName)
  if not tiers['OK']:
    return tiers
  tier1andTier2Maps = {} # initialize the dictionary with T1 sites.
  # no T2 associated to a T2 by default.
  if 'Tier0' in tiers['Value']:
    for site in tiers['Value']['Tier0']:
      tier1andTier2Maps[site] = []

  if 'Tier1' in tiers['Value']:
    for site in tiers['Value']['Tier1']:
      tier1andTier2Maps[site] = []

  for site in storages:
    sites = getSitesForSE(storages[site])
    if not sites['OK']:
      return sites

    sites = sites['Value']
    for i in sites:
      if i in tier1andTier2Maps:
        # it associates the tier2 site the corresponding Tier 1 site.
        tier1andTier2Maps[i] += [site]

  return S_OK(tier1andTier2Maps)

#############################################################################
def isOneSEFound(se1, se2):
  """
  It compares two list which contains different SEs. The two list not have to be identical,
  because we never attach a Tier2 all the SEs which provided by a Tier1.
  """
  if len(se1) >= len(se2):
    for i in se2:
      for j in se1:
        if i == j:
          return True
    return False
  elif len(se1) < len(se2):
    for i in se1:
      for j in se2:
        if i == j :
          return True
  return False

