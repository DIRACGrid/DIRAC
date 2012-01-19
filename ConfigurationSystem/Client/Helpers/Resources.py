# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC                                              import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Path      import cfgPath

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
