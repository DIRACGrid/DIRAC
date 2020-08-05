#!/usr/bin/env python
########################################################################
# File :   tornado-start-all
# Author : Louis MARTIN
########################################################################
# Just run this script to start Tornado and all services
# Use CS to change port

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# Must be define BEFORE any dirac import
import os
import sys
os.environ['DIRAC_USE_TORNADO_IOLOOP'] = "True"


from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Tornado.Server.TornadoServer import TornadoServer
from DIRAC.Core.Base import Script

from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC import gConfig
from DIRAC.ConfigurationSystem.Client import PathFinder

from DIRAC.Core.Utilities.DErrno import includeExtensionErrors


# We check if there is no configuration server started as master
# If you want to start a master CS you should use Configuration_Server.cfg and
# use tornado-start-CS.py
if gConfigurationData.isMaster() and gConfig.getValue(
    '/Systems/Configuration/%s/Services/Server/Protocol' %
    PathFinder.getSystemInstance('Configuration'),
   'dips').lower() == 'https':
  gLogger.fatal("You can't run the CS and services in the same server!")
  sys.exit(0)

localCfg = LocalConfiguration()
localCfg.addMandatoryEntry("/DIRAC/Setup")
localCfg.addDefaultEntry("/DIRAC/Security/UseServerCertificate", "yes")
localCfg.addDefaultEntry("LogLevel", "INFO")
localCfg.addDefaultEntry("LogColor", True)
resultDict = localCfg.loadUserData()
if not resultDict['OK']:
  gLogger.initialize("Tornado", "/")
  gLogger.error("There were errors when loading configuration", resultDict['Message'])
  sys.exit(1)

includeExtensionErrors()


gLogger.initialize('Tornado', "/")


serverToLaunch = TornadoServer()
serverToLaunch.startTornado()
