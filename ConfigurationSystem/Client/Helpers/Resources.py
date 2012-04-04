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
  
  print result
  
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

def getCatalogPath(catalogName):
  """  Return the configuration path of the description for a a given catalog
  """
  return '/Resources/FileCatalogs/%s' % catalogName