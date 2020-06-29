from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"

import DIRAC

from DIRAC import S_OK
from DIRAC.ConfigurationSystem.Client.Helpers import getCSExtensions


def getCurrentVersion():
  """ Get a string corresponding to the current version of the DIRAC package and all the installed
      extension packages
  """

  version = 'DIRAC ' + DIRAC.version

  for ext in getCSExtensions():
    try:
      import imp
      module = imp.find_module("%sDIRAC" % ext)
      extModule = imp.load_module("%sDIRAC" % ext, *module)
      version = extModule.version
    except ImportError:
      pass
    except AttributeError:
      pass

  return S_OK(version)


def getVersion():
  """ Get a dictionary corresponding to the current version of the DIRAC package and all the installed
      extension packages
  """

  vDict = {'Extensions': {}}
  vDict['DIRAC'] = DIRAC.version

  for ext in getCSExtensions():
    try:
      import imp
      module = imp.find_module("%sDIRAC" % ext)
      extModule = imp.load_module("%sDIRAC" % ext, *module)
      vDict['Extensions'][ext] = extModule.version
    except ImportError:
      pass
    except AttributeError:
      pass

  return S_OK(vDict)
