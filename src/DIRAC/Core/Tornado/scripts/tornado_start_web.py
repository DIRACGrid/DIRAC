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

  from DIRAC import gConfig
  from DIRAC.ConfigurationSystem.Client import PathFinder
  from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
  from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
  from DIRAC.Core.Tornado.Server.TornadoServer import TornadoServer
  from DIRAC.Core.Utilities.DErrno import includeExtensionErrors
  from DIRAC.FrameworkSystem.Client.Logger import gLogger

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

  services = ['DataManagement/TornadoFileCatalog']
  endpoints = ['Configuration/Configuration', 'Framework/Auth', 'Framework/Proxy']

  try:
    from WebAppDIRAC.Core.App import App
  except ImportError:
    gLogger.fatal('Web portal is not installed.')
    sys.exit(1)

  # Get routes and settings for a portal
  result = App().getAppToDict(8000)
  if not result['OK']:
    gLogger.fatal(result['Message'])
    sys.exit(1)
  app = result['Value']

  serverToLaunch = TornadoServer(services, endpoints, port=8000, balancer='nginx')
  serverToLaunch.addHandlers(app['routes'], app['settings'])
  serverToLaunch.startTornado()


if __name__ == "__main__":
  main()
