#!/usr/bin/env python
########################################################################
# File :   dirac-executor
# Author : Adria Casajus
########################################################################
"""
This is a script to launch DIRAC executors
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import sys

from DIRAC import gLogger
from DIRAC.Core.Base.ExecutorReactor import ExecutorReactor
from DIRAC.Core.Utilities.DErrno import includeExtensionErrors
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration


@DIRACScript()
def main():
  localCfg = LocalConfiguration()
  localCfg.setUsageMessage(__doc__)

  positionalArgs = localCfg.getPositionalArguments()
  if len(positionalArgs) == 0:
    gLogger.fatal("You must specify which executor to run!")
    sys.exit(1)

  if len(positionalArgs) == 1 and positionalArgs[0].find("/") > -1:
    mainName = positionalArgs[0]
  else:
    mainName = "Framework/MultiExecutor"

  localCfg.setConfigurationForExecutor(mainName)
  localCfg.addMandatoryEntry("/DIRAC/Setup")
  localCfg.addDefaultEntry("/DIRAC/Security/UseServerCertificate", "yes")
  localCfg.addDefaultEntry("LogLevel", "INFO")
  localCfg.addDefaultEntry("LogColor", True)
  resultDict = localCfg.loadUserData()
  if not resultDict['OK']:
    gLogger.fatal("There were errors when loading configuration", resultDict['Message'])
    sys.exit(1)

  includeExtensionErrors()
  executorReactor = ExecutorReactor()

  result = executorReactor.loadModules(positionalArgs)
  if not result['OK']:
    gLogger.fatal("Error while loading executor", result['Message'])
    sys.exit(1)

  result = executorReactor.go()
  if not result['OK']:
    gLogger.fatal(result['Message'])
    sys.exit(1)

  gLogger.notice("Graceful exit. Bye!")
  sys.exit(0)


if __name__ == "__main__":
  main()
