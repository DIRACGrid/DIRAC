#!/usr/bin/env python
########################################################################
# File :    dirac_setup_site.py
# Author :  Ricardo Graciani
########################################################################
"""
Initial installation and configuration of a new DIRAC server (DBs, Services, Agents, Web Portal,...)
"""
from DIRAC import S_OK
from DIRAC.Core.Base.Script import Script


class Params:
    def __init__(self):
        self.exitOnError = False

    def setExitOnError(self, value):
        self.exitOnError = True
        return S_OK()


@Script()
def main():
    cliParams = Params()

    Script.disableCS()
    Script.registerSwitch(
        "e", "exitOnError", "flag to exit on error of any component installation", cliParams.setExitOnError
    )

    Script.addDefaultOptionValue("/DIRAC/Security/UseServerCertificate", "yes")
    Script.addDefaultOptionValue("LogLevel", "INFO")
    Script.parseCommandLine()
    args = Script.getExtraCLICFGFiles()

    if len(args) > 1:
        Script.showHelp(exitCode=1)

    cfg = None
    if len(args):
        cfg = args[0]
    from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller

    gComponentInstaller.exitOnError = cliParams.exitOnError

    result = gComponentInstaller.setupSite(Script.localCfg, cfg)
    if not result["OK"]:
        print("ERROR:", result["Message"])
        exit(-1)

    result = gComponentInstaller.getStartupComponentStatus([])
    if not result["OK"]:
        print("ERROR:", result["Message"])
        exit(-1)

    print("\nStatus of installed components:\n")
    result = gComponentInstaller.printStartupStatus(result["Value"])
    if not result["OK"]:
        print("ERROR:", result["Message"])
        exit(-1)


if __name__ == "__main__":
    main()
