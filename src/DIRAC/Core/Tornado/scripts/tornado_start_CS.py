#!/usr/bin/env python
########################################################################
# File :   tornado-start-CS
# Author : Louis MARTIN
########################################################################
# Just run this script to start Tornado and CS service
# Use dirac.cfg (or other cfg given in the command line) to change port

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os
import sys
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  # Must be defined BEFORE any dirac import
  os.environ['DIRAC_USE_TORNADO_IOLOOP'] = "True"

  from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection
  from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
  from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
  from DIRAC.ConfigurationSystem.private.Refresher import gRefresher
  from DIRAC.Core.Utilities.DErrno import includeExtensionErrors
  from DIRAC.Core.Tornado.Server.TornadoServer import TornadoServer
  from DIRAC.FrameworkSystem.Client.Logger import gLogger

  if gConfigurationData.isMaster():
    gRefresher.disable()

  localCfg = LocalConfiguration()
  localCfg.addMandatoryEntry("/DIRAC/Setup")
  localCfg.addDefaultEntry("/DIRAC/Security/UseServerCertificate", "yes")
  localCfg.addDefaultEntry("LogLevel", "INFO")
  localCfg.addDefaultEntry("LogColor", True)
  resultDict = localCfg.loadUserData()
  if not resultDict['OK']:
    gLogger.initialize("Tornado-CS", "/")
    gLogger.error("There were errors when loading configuration", resultDict['Message'])
    sys.exit(1)

  includeExtensionErrors()

  gLogger.initialize('Tornado-CS', "/")

  # get the specific master CS port
  try:
    csPort = int(gConfigurationData.extractOptionFromCFG('%s/Port' % getServiceSection('Configuration/Server')))
  except TypeError:
    csPort = None

  serverToLaunch = TornadoServer(services='Configuration/Server', port=csPort)
  serverToLaunch.startTornado()


if __name__ == "__main__":
  main()
