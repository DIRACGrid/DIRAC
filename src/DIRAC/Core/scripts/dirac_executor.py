#!/usr/bin/env python
########################################################################
# File :   dirac-executor
# Author : Adria Casajus
########################################################################
"""
This is a script to launch DIRAC executors
"""
import sys

from DIRAC import gLogger
from DIRAC.Core.Base.ExecutorReactor import ExecutorReactor
from DIRAC.Core.Utilities.DErrno import includeExtensionErrors
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["executor: specify which executor to run"])
    positionalArgs = Script.getPositionalArgs()
    localCfg = Script.localCfg

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
    if not resultDict["OK"]:
        gLogger.fatal("There were errors when loading configuration", resultDict["Message"])
        sys.exit(1)

    includeExtensionErrors()
    executorReactor = ExecutorReactor()

    result = executorReactor.loadModules(positionalArgs)
    if not result["OK"]:
        gLogger.fatal("Error while loading executor", result["Message"])
        sys.exit(1)

    result = executorReactor.go()
    if not result["OK"]:
        gLogger.fatal(result["Message"])
        sys.exit(1)

    gLogger.notice("Graceful exit. Bye!")
    sys.exit(0)


if __name__ == "__main__":
    main()
