#!/usr/bin/env python
"""
Create a new DB in the MySQL server
"""
from DIRAC import exit as DIRACExit
from DIRAC import gConfig, gLogger
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["DB: Name of the Database"])
    _, args = Script.parseCommandLine()

    # Script imports
    from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import useServerCertificate
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo
    from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller
    from DIRAC.FrameworkSystem.Utilities import MonitoringUtilities

    user = "DIRAC"

    gComponentInstaller.exitOnError = True
    gComponentInstaller.getMySQLPasswords()
    for db in args:
        result = gComponentInstaller.installDatabase(db)
        if not result["OK"]:
            gLogger.error(f"Failed to correctly install {db}:", result["Message"])
            DIRACExit(1)
        extension, system = result["Value"]
        result = gComponentInstaller.addDatabaseOptionsToCS(gConfig, system, db, overwrite=True)
        if not result["OK"]:
            gLogger.error("Failed to add database options to CS:", result["Message"])
            DIRACExit(1)

        if db != "InstalledComponentsDB":
            # get the user that installed the DB
            if not useServerCertificate():
                result = getProxyInfo()
                if not result["OK"]:
                    return result
                proxyInfo = result["Value"]
                if "username" in proxyInfo:
                    user = proxyInfo["username"]

            result = MonitoringUtilities.monitorInstallation("DB", system, db, user=user)
            if not result["OK"]:
                gLogger.error("Failed to register installation in database:", result["Message"])
                DIRACExit(1)


if __name__ == "__main__":
    main()
