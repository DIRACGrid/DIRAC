# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC                                              import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Path      import cfgPath

import re

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

def getQueues( siteList=None, ceList=None, ceTypeList=None, community=None, mode=None ):
  """ Get CE/queue options according to the specified selection
  """
  
  result = gConfig.getSections('/Resources/Sites')
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
        comList = gConfig.getValue( '/Resources/Sites/%s/%s/VO' % (grid,site), [] )
        if comList and not community in comList:
          continue
      result = gConfig.getSections( '/Resources/Sites/%s/%s/CEs' % (grid,site) )
      if not result['OK']:
        continue
      ces = result['Value']
      for ce in ces:
        if mode:
          ceMode = gConfig.getValue( '/Resources/Sites/%s/%s/CEs/%s/SubmissionMode' % (grid,site,ce), 'InDirect' )
          if not ceMode or ceMode != mode:
            continue
        if ceTypeList:
          ceType = gConfig.getValue( '/Resources/Sites/%s/%s/CEs/%s/CEType' % (grid,site,ce), None )
          if not ceType or not ceType in ceTypeList:
            continue   
        if community:
          comList = gConfig.getValue( '/Resources/Sites/%s/%s/CEs/%s/VO' % (grid,site,ce), [] )
          if comList and not community in comList:
            continue   
        result = gConfig.getOptionsDict( '/Resources/Sites/%s/%s/CEs/%s' % (grid,site,ce) )
        if not result['OK']:
          continue  
        ceOptionsDict = result['Value']
        result = gConfig.getSections( '/Resources/Sites/%s/%s/CEs/%s/Queues' % (grid,site,ce) )
        if not result['OK']:
          continue     
        queues = result['Value']
        for queue in queues:
          if community:
            comList = gConfig.getValue( '/Resources/Sites/%s/%s/CEs/%s/Queues/%s/VO' % (grid,site,ce,queue), [] )
            if comList and not community in comList:
              continue   
          resultDict.setdefault(site,{})
          resultDict[site].setdefault(ce,ceOptionsDict)
          resultDict[site][ce].setdefault('Queues',{})  
          result = gConfig.getOptionsDict( '/Resources/Sites/%s/%s/CEs/%s/Queues/%s' % (grid,site,ce,queue) )
          if not result['OK']:
            continue  
          queueOptionsDict = result['Value']
          resultDict[site][ce]['Queues'][queue] = queueOptionsDict
          
  return S_OK(resultDict)       

def getCatalogPath(catalogName):
  """  Return the configuration path of the description for a a given catalog
  """
  return '/Resources/FileCatalogs/%s' % catalogName