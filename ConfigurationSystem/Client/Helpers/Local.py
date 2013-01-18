# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC                                              import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Path      import cfgPath

gBaseLocalSiteSection = "/LocalSite"

def gridEnv():
  """
    Return location of gridenv file to get a UI environment
  """
  return gConfig.getValue( cfgPath( gBaseLocalSiteSection, 'GridEnv' ), '' )
