#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os
import sys
import tornado

from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  # Must be define BEFORE any dirac import
  os.environ['DIRAC_USE_TORNADO_IOLOOP'] = "True"

  from DIRAC.ConfigurationSystem.Client.PathFinder import getAPISection
  from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
  from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
  from DIRAC.Core.Tornado.Server.TornadoServer import TornadoServer
  from DIRAC.Core.Utilities.DErrno import includeExtensionErrors
  from DIRAC.FrameworkSystem.Client.Logger import gLogger

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

  endpoints = ['Framework/Auth']
  try:
    asPort = int(gConfigurationData.extractOptionFromCFG('%s/Port' % getAPISection('Framework/Auth')))
  except TypeError:
    asPort = None

  serverToLaunch = TornadoServer(False, endpoints, port=asPort)

  serverToLaunch.startTornado()


if __name__ == "__main__":
  main()
