#!/usr/bin/env python
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import sys
import tornado

from DIRAC import gConfig, S_OK
from DIRAC.Core.Base import Script
from DIRAC.Core.Web.App import App
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration

__RCSID__ = "$Id$"

if __name__ == "__main__":

  def disableDevMode(op):
    gConfig.setOptionValue("/WebApp/DevelopMode", "False")
    return S_OK()

  def setHandlers(op):
    localCfg.handlersLoc = op
    gLogger.notice("SET handlersLoc to ", localCfg.handlersLoc)
    return S_OK()

  localCfg = LocalConfiguration()

  localCfg.handlersLoc = 'WebApp.handler'
  localCfg.setConfigurationForWeb("WebApp")
  localCfg.addMandatoryEntry("/DIRAC/Setup")
  localCfg.addDefaultEntry("/DIRAC/Security/UseServerCertificate", "yes")
  localCfg.addDefaultEntry("LogLevel", "INFO")
  localCfg.addDefaultEntry("LogColor", True)
  localCfg.registerCmdOpt("p", "production", "Enable production mode", disableDevMode)
  localCfg.registerCmdOpt("S:", "service_api_path=",
                          "Specify path(s) to handlers, for ex. 'DIRAC.FrameworkSystem.Utilities'",
                          setHandlers)

  result = localCfg.loadUserData()
  if not result['OK']:
    gLogger.initialize("WebApp", "/")
    gLogger.fatal("There were errors when loading configuration", result['Message'])
    sys.exit(1)

  gLogger.notice("Set next path(s): ", localCfg.handlersLoc)
  app = App(handlersLoc=localCfg.handlersLoc)
  result = app.bootstrap()
  if not result['OK']:
    gLogger.fatal(result['Message'])
    sys.exit(1)
  app.run()
