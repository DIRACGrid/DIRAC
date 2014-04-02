########################################################################
# $HeadURL$
# File :    CSGlobals.py
# Author :  Ricardo Graciani
########################################################################
"""
Some Helper functions to retrieve common location from the CS
"""
__RCSID__ = "$Id$"

#from DIRAC import gConfig

def getSetup():
  from DIRAC import gConfig
  return gConfig.getValue( "/DIRAC/Setup", "" )

def getVO( defaultVO = '' ):
  """
    Return VO from configuration
  """
  from DIRAC import gConfig
  return gConfig.getValue( '/DIRAC/VirtualOrganization', defaultVO )

def getCSExtensions():
  """
    Return list of extensions registered in the CS
    They do not include DIRAC
  """
  from DIRAC import gConfig
  return gConfig.getValue( '/DIRAC/Extensions', [] )

def getInstalledExtensions():
  """
    Return list of extensions registered in the CS and available in local installation
  """
  import imp
  extensions = []
  for extension in getCSExtensions():
    try:
      if not "DIRAC" in extension: 
        extension = '%sDIRAC' % extension
      imp.find_module( extension )
      extensions.append( extension )
    except ImportError:
      pass
  extensions.append( 'DIRAC' )
  return extensions


def skipCACheck():
  from DIRAC import gConfig
  return gConfig.getValue( "/DIRAC/Security/SkipCAChecks", "false" ).lower() in ( "y", "yes", "true" )

def useServerCertificate():
  from DIRAC import gConfig
  return gConfig.getValue( "/DIRAC/Security/UseServerCertificate", "false" ).lower() in ( "y", "yes", "true" )
