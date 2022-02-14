#!/usr/bin/env python
########################################################################
# File :   tornado-start-all
# Author : Louis MARTIN
########################################################################
# Just run this script to start Tornado and all services
# Use CS to change port
import os
import sys

from DIRAC.Core.Base.Script import Script


@Script()
def main():

    if os.environ.get("DIRAC_USE_TORNADO_IOLOOP", "false").lower() not in ("yes", "true"):
        raise RuntimeError(
            "DIRAC_USE_TORNADO_IOLOOP is not defined in the environment."
            + "\n"
            + "It is necessary to run with Tornado."
            + "\n"
            + "https://dirac.readthedocs.io/en/latest/DeveloperGuide/TornadoServices/index.html"
        )

    from DIRAC import gConfig
    from DIRAC.ConfigurationSystem.Client import PathFinder
    from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
    from DIRAC.Core.Tornado.Server.TornadoServer import TornadoServer
    from DIRAC.Core.Utilities.DErrno import includeExtensionErrors
    from DIRAC.FrameworkSystem.Client.Logger import gLogger

    localCfg = Script.localCfg
    localCfg.setConfigurationForServer("Tornado/Tornado")
    localCfg.addMandatoryEntry("/DIRAC/Setup")
    localCfg.addDefaultEntry("/DIRAC/Security/UseServerCertificate", "yes")
    localCfg.addDefaultEntry("LogLevel", "INFO")
    localCfg.addDefaultEntry("LogColor", True)
    resultDict = localCfg.loadUserData()
    if not resultDict["OK"]:
        gLogger.initialize("Tornado", "/")
        gLogger.error("There were errors when loading configuration", resultDict["Message"])
        sys.exit(1)

    includeExtensionErrors()

    gLogger.initialize("Tornado", "/")

    # We check if there is no configuration server started as master
    # If you want to start a master CS you should use Configuration_Server.cfg and
    # use tornado-start-CS.py
    key = "/Systems/Configuration/%s/Services/Server/Protocol" % PathFinder.getSystemInstance("Configuration")
    if gConfigurationData.isMaster() and gConfig.getValue(key, "dips").lower() == "https":
        gLogger.fatal("You can't run the CS and services in the same server!")
        sys.exit(0)

    serverToLaunch = TornadoServer(endpoints=True)
    serverToLaunch.startTornado()


if __name__ == "__main__":
    main()
