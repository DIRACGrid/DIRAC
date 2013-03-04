# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC                                              import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Path      import cfgPath
from DIRAC.Core.Utilities.List                          import uniqueElements
import re

gBaseResourcesSection = "/Resources"

def getSites():
  """ Get the list of all the sites defined in the CS
  """
  result = gConfig.getSections( cfgPath( gBaseResourcesSection, 'Sites' ) )
  if not result['OK']:
    return result
  grids = result['Value']
  sites = []
  for grid in grids:
    result = gConfig.getSections( cfgPath( gBaseResourcesSection, 'Sites', grid ) )
    if not result['OK']:
      return result
    sites += result['Value']
    
  return S_OK( sites )  

def getStorageElementSiteMapping( siteList=[] ):
  """ Get Storage Element belonging to the given sites 
  """ 
  if not siteList:
    result = getSites()
    if not result['OK']:
      return result
    siteList = result['Value']
  siteDict = {}
  for site in siteList:
    grid = site.split('.')[0]
    ses = gConfig.getValue( cfgPath( gBaseResourcesSection, 'Sites', grid, site, 'SE' ), [] )
    siteDict[site] = ses
    
  return S_OK( siteDict )  

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

def getQueue( site, ce, queue ):
  """ Get parameters of the specified queue 
  """
  grid = site.split('.')[0]
  result = gConfig.getOptionsDict( '/Resources/Sites/%s/%s/CEs/%s' % ( grid, site, ce ) )
  if not result['OK']:
    return result 
  resultDict = result['Value']
  result = gConfig.getOptionsDict( '/Resources/Sites/%s/%s/CEs/%s/Queues/%s' % ( grid, site, ce, queue ) )
  if not result['OK']:
    return result
  resultDict.update( result['Value'] )
  resultDict['Queue'] = queue
  
  return S_OK( resultDict )

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
    for p in platformDict:
      if p == platform:
        return S_OK( platform ) 
      platformDict[p] = [ x.strip() for x in platformDict[p].split( ',' ) ]
  else:
    return S_ERROR( 'OS compatibility info not found' )    
      
  for p in platformDict:
    if platform in platformDict[p]:
      return S_OK( p )
  
  return S_ERROR( 'No compatible DIRAC platform found for %s' % platform )
  
def getCatalogPath( catalogName ):
  """  Return the configuration path of the description for a a given catalog
  """
  return '/Resources/FileCatalogs/%s' % catalogName
