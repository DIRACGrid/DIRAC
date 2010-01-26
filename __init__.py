# $HeadURL$
"""
   DIRAC - Distributed Infrastructure with Remote Agent Control

   The LHCb distributed data production and analysis system

   DIRAC is a software system which allows to integrate various distributed
   computing resources in a single system which is used for the MC data
   production, data reprocessing and final user analysis. It consists
   of the following main subsystems:

     - Core
       - Utils



   It defines the following data members:

     - majorVersion: DIRAC Major version number
     - minorVersion: DIRAC Minor version number
     - patchLevel:   DIRAC Patch level number
     - version:      DIRAC version string

     - pythonPath:   absolute real path to the directory that contains this file
     - rootPath:     absolute real path to the parent of DIRAC.PythonPath

   It loads Modules from the DIRAC.Core.Utils package.

"""
__RCSID__ = "$Id$"

from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)

import sys, os, platform

# Define Version

majorVersion = 5
minorVersion = 0
patchLevel   = 0
preVersion   = 10

version      = "v%sr%s" % ( majorVersion, minorVersion )
buildVersion = "v%dr%d" % ( majorVersion, minorVersion )
if patchLevel:
  version = "%sp%s" % ( version, patchLevel )
  buildVersion = "%s build %s" % ( buildVersion, patchLevel )
if preVersion:
  version = "%s-pre%s" % ( version, preVersion )
  buildVersion = "%s pre %s" % ( buildVersion, preVersion )
  
# Check of python version

__pythonMajorVersion = ["2",]
__pythonMinorVersion = ["4","5","6"]

if not ( __pythonMajorVersion.__contains__( platform.python_version_tuple()[0] ) and
         __pythonMinorVersion.__contains__( platform.python_version_tuple()[1] ) ):
  print "Python Version %s not supported by DIRAC" % platform.python_version()
  print "Supported versions are: "
  for major in __pythonMajorVersion:
    for minor in __pythonMinorVersion:
      print "%s.%s.x" % ( major, minor )

  sys.exit(-1)

errorMail = "dirac.alarms@gmail.com"
alarmMail = "dirac.alarms@gmail.com"

# Set rootPath of DIRAC installation

pythonPath = os.path.realpath( __path__[0] )
rootPath = os.path.dirname( pythonPath )

# Import DIRAC.Core.Utils modules

from DIRAC.Core.Utilities import *

from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

#Logger
from DIRAC.FrameworkSystem.Client.Logger import gLogger

#Configuration client
from DIRAC.ConfigurationSystem.Client.Config import gConfig

#Monitoring client
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor

# Some Defaults if not present in the configuration
FQDN = getFQDN()
if len( FQDN.split('.') ) > 2 :
  # Use the last component of the FQDN as country code if there are more than 2 components
  _siteName = 'DIRAC.Client.%s' % FQDN.split('.')[-1]
else:
  # else use local as country code
  _siteName = 'DIRAC.Client.local'

__siteName = False

def siteName():
  global __siteName
  if not __siteName:
    __siteName = gConfig.getValue('/LocalSite/Site', _siteName )
  return __siteName

#Callbacks
ExitCallback.registerSignals()

#Set the platform
from DIRAC.Core.Utilities.Platform import getPlatformString
platform = getPlatformString()
platformTuple = tuple(platform.split('_'))

def exit( exitCode = 0 ):
  ExitCallback.execute( exitCode, [] )
  sys.exit( exitCode )

def abort( exitCode, *args, **kwargs ):
  gLogger.fatal( *args, **kwargs )
  os._exit( exitCode )
