#!/usr/bin/env python
########################################################################
# File :   dirac-service
# Author : Adria Casajus
########################################################################
"""
This is a script to launch DIRAC services. Mostly internal.

Usage:
  dirac-service [options] ...
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys

from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.ServiceReactor import ServiceReactor
from DIRAC.Core.Utilities.DErrno import includeExtensionErrors
from DIRAC.Core.Utilities.DIRACScript import DIRACScript

__RCSID__ = "$Id$"


@DIRACScript()
def main(self):
  positionalArgs = self.localCfg.getPositionalArguments()
  if len(positionalArgs) == 0:
    gLogger.fatal("You must specify which server to run!")
    sys.exit(1)

  serverName = positionalArgs[0]
  self.localCfg.setConfigurationForServer(serverName)
  self.localCfg.addMandatoryEntry("Port")
  # self.localCfg.addMandatoryEntry( "HandlerPath" )
  self.localCfg.addMandatoryEntry("/DIRAC/Setup")
  self.localCfg.addDefaultEntry("/DIRAC/Security/UseServerCertificate", "yes")
  self.localCfg.addDefaultEntry("LogLevel", "INFO")
  self.localCfg.addDefaultEntry("LogColor", True)
  resultDict = self.localCfg.loadUserData()
  if not resultDict['OK']:
    gLogger.initialize(serverName, "/")
    gLogger.error("There were errors when loading configuration", resultDict['Message'])
    sys.exit(1)

  includeExtensionErrors()

  serverToLaunch = ServiceReactor()
  result = serverToLaunch.initialize(positionalArgs)
  if not result['OK']:
    gLogger.error(result['Message'])
    sys.exit(1)

  result = serverToLaunch.serve()
  if not result['OK']:
    gLogger.error(result['Message'])
    sys.exit(1)


if __name__ == "__main__":
  main()
