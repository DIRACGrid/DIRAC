"""
   DIRAC - Distributed Infrastructure with Remote Agent Control

   The distributed data production and analysis system of LHCb and other VOs.

   DIRAC is a software framework for distributed computing which
   allows to integrate various computing resources in a single
   system. At the same time it integrates all kinds of computing
   activities like Monte Carlo simulations, data processing, or
   final user analysis.

   It is build as number of cooperating systems:
    - Accounting
    - Configuration
    - Core
      - Base
      - DISET
      - Security
      - Utilities
      - Workflow
    - Framework
    - RequestManagement
    - Resources
    - Transformation

    Which are used by other system providing functionality to
    the end user:
    - DataManagement
    - Interfaces
    - ResourceStatus
    - StorageManagement
    - WorkloadManagement

    It defines the following data members:
    - majorVersion:  DIRAC Major version number
    - minorVersion:  DIRAC Minor version number
    - patchLevel:    DIRAC Patch level number
    - preVersion:    DIRAC Pre release number
    - version:       DIRAC version string
    - buildVersion:  DIRAC version string

    - errorMail:     mail address for important errors
    - alarmMail:     mail address for important alarms

    - pythonPath:    absolute real path to the directory that contains this file
    - rootPath:      absolute real path to the parent of DIRAC.pythonPath

    It loads Modules from :
    - DIRAC.Core.Utililies

    It loads:
    - S_OK:           OK return structure
    - S_ERROR:        ERROR return structure
    - gLogger:        global Logger object
    - gConfig:        global Config object

    It defines the following functions:
    - abort:          aborts execution
    - exit:           finish execution using callbacks
    - siteName:       returns DIRAC name for current site

    - getPlatform():      DIRAC platform string for current host
    - getPlatformTuple(): DIRAC platform tuple for current host

"""

import sys
import os
import platform as pyPlatform
from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)


__RCSID__ = "$Id$"


# Define Version

majorVersion = 6
minorVersion = 21
patchLevel = 10
preVersion = 0

version = "v%sr%s" % (majorVersion, minorVersion)
buildVersion = "v%dr%d" % (majorVersion, minorVersion)
if patchLevel:
  version = "%sp%s" % (version, patchLevel)
  buildVersion = "%s build %s" % (buildVersion, patchLevel)
if preVersion:
  version = "%s-pre%s" % (version, preVersion)
  buildVersion = "%s pre %s" % (buildVersion, preVersion)

# Check of python version

__pythonMajorVersion = ("2", )
__pythonMinorVersion = ("7")

pythonVersion = pyPlatform.python_version_tuple()
if str(pythonVersion[0]) not in __pythonMajorVersion or str(pythonVersion[1]) not in __pythonMinorVersion:
  print "Python Version %s not supported by DIRAC" % pyPlatform.python_version()
  print "Supported versions are: "
  for major in __pythonMajorVersion:
    for minor in __pythonMinorVersion:
      print "%s.%s.x" % (major, minor)

  sys.exit(1)

errorMail = "dirac.alarms@gmail.com"
alarmMail = "dirac.alarms@gmail.com"

# Set rootPath of DIRAC installation

pythonPath = os.path.realpath(__path__[0])
rootPath = os.path.dirname(pythonPath)

# Import DIRAC.Core.Utils modules

#from DIRAC.Core.Utilities import *
from DIRAC.Core.Utilities.Network import getFQDN
import DIRAC.Core.Utilities.ExitCallback as ExitCallback

from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR


# Logger
from DIRAC.FrameworkSystem.Client.Logger import gLogger

# Configuration client
from DIRAC.ConfigurationSystem.Client.Config import gConfig

# Some Defaults if not present in the configuration
FQDN = getFQDN()
if len(FQDN.split('.')) > 2:
  # Use the last component of the FQDN as country code if there are more than 2 components
  _siteName = 'DIRAC.Client.%s' % FQDN.split('.')[-1]
else:
  # else use local as country code
  _siteName = 'DIRAC.Client.local'

__siteName = False


# # Update DErrno with the extensions errors
# from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
# from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
# allExtensions = CSGlobals.getCSExtensions()
#
# # Update for each extension. Careful to conflict :-)
# for extension in allExtensions:
#   ol = ObjectLoader( baseModules = ["%sDIRAC" % extension] )
#   extraErrorModule = ol.loadModule( 'Core.Utilities.DErrno' )
#   if extraErrorModule['OK']:
#     extraErrorModule = extraErrorModule['Value']
#
#     # The next 3 dictionary MUST be present for consistency
#
#     # Global name of errors
#     DErrno.__dict__.update( extraErrorModule.extra_dErrName )
#     # Dictionary with the error codes
#     DErrno.dErrorCode.update( extraErrorModule.extra_dErrorCode )
#     # Error description string
#     DErrno.dStrError.update( extraErrorModule.extra_dStrError )
#
#     # extra_compatErrorString is optional
#     for err in getattr( extraErrorModule, 'extra_compatErrorString', [] ) :
#       DErrno.compatErrorString.setdefault( err, [] ).extend( extraErrorModule.extra_compatErrorString[err] )


def siteName():
  """
  Determine and return DIRAC name for current site
  """
  global __siteName
  if not __siteName:
    __siteName = gConfig.getValue('/LocalSite/Site', _siteName)
  return __siteName


# Callbacks
ExitCallback.registerSignals()

# platform detection
from DIRAC.Core.Utilities.Platform import getPlatformString, getPlatform, getPlatformTuple


def exit(exitCode=0):
  """
  Finish execution using callbacks
  """
  ExitCallback.execute(exitCode, [])
  sys.exit(exitCode)


def abort(exitCode, *args, **kwargs):
  """
  Abort execution
  """
  try:
    gLogger.fatal(*args, **kwargs)
    os._exit(exitCode)
  except OSError:
    gLogger.exception('Error while executing DIRAC.abort')
    os._exit(exitCode)
