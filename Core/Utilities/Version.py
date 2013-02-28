# $HeadURL$
__RCSID__ = "$Id$"

import os
from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers import getCSExtensions

def getCurrentVersion():
  """ Get a string corresponding to the current version of the DIRAC package and all the installed
      extension packages
  """
  import DIRAC
  version = 'DIRAC ' + DIRAC.version

  for ext in getCSExtensions():
    try:
      import imp
      module = imp.find_module( "%sDIRAC" % ext )
      extModule = imp.load_module( "%sDIRAC" % ext, *module )
      version = extModule.version
    except ImportError:
      pass
    except AttributeError:
      pass

  return S_OK( version )

def getVersion():
  """ Get a dictionary corresponding to the current version of the DIRAC package and all the installed
      extension packages
  """

  import DIRAC
  vDict = {'Extensions':{}}
  vDict['DIRAC'] = DIRAC.version

  for ext in getCSExtensions():
    try:
      import imp
      module = imp.find_module( "%sDIRAC" % ext )
      extModule = imp.load_module( "%sDIRAC" % ext, *module )
      vDict['Extensions'][ext] = extModule.version
    except ImportError:
      pass
    except AttributeError:
      pass

  return S_OK( vDict )

def getReleasenoteVersion():

  """
  Scan the sub-directories of DIRAC root for release.notes files and return a dict
  { "subdirectory" : "version_from_release.notes" }
  """

  try:
    path = os.environ[ "DIRAC" ]
  except Exception, x:
    return S_ERROR( x )
  filename = "release.notes"

  notes = dict()
  entrylist = os.listdir( path )

  for entry in entrylist:

    normalentry = os.path.join( path , entry )

    if not os.path.isdir( normalentry ):
      continue

    name = os.path.join( normalentry , filename )
    if not os.path.exists( name ):
      continue

    f = open( name , "r" )
    version = ""

    while len( version ) < 1:
      version = f.readline()
      if len( version ) == 0:
        break
      version = version.strip().lstrip("[").rstrip("]")

    f.close()
    notes[ entry ] = version

  return S_OK( notes )
