#!/usr/bin/env python
"""
Uninstallation of a DIRAC component
"""
import socket

from DIRAC import exit as DIRACexit
from DIRAC import gLogger, S_OK
from DIRAC.Core.Utilities.PromptUser import promptUser
from DIRAC.Core.Base.Script import Script
from DIRAC.FrameworkSystem.Utilities import MonitoringUtilities
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient

force = False


def setForce(opVal):
    global force
    force = True
    return S_OK()


@Script()
def main():
    global force

    from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller

    gComponentInstaller.exitOnError = True

    Script.registerSwitch("f", "force", "Forces the removal of the logs", setForce)
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(
        (
            "System/Component: Full component name (ie: WorkloadManagement/Matcher)",
            "System:           Name of the DIRAC system (ie: WorkloadManagement)",
        )
    )
    Script.registerArgument(" Component:        Name of the DIRAC service (ie: Matcher)", mandatory=False)
    _, args = Script.parseCommandLine()

    if len(args) == 1:
        args = args[0].split("/")

    if len(args) < 2:
        Script.showHelp(exitCode=1)

    system = args[0]
    component = args[1]

    monitoringClient = ComponentMonitoringClient()
    result = monitoringClient.getInstallations(
        {"Instance": component, "UnInstallationTime": None},
        {"DIRACSystem": system},
        {"HostName": socket.getfqdn()},
        True,
    )
    if not result["OK"]:
        gLogger.error(result["Message"])
        DIRACexit(1)
    if len(result["Value"]) < 1:
        gLogger.warn("Given component does not exist")
        DIRACexit(1)
    if len(result["Value"]) > 1:
        gLogger.error("Too many components match")
        DIRACexit(1)

    removeLogs = False
    if force:
        removeLogs = True
    else:
        if result["Value"][0]["Component"]["Type"] in gComponentInstaller.componentTypes:
            result = promptUser("Remove logs?", ["y", "n"], "n")
            if result["OK"]:
                removeLogs = result["Value"] == "y"
            else:
                gLogger.error(result["Message"])
                DIRACexit(1)

    result = gComponentInstaller.uninstallComponent(system, component, removeLogs)
    if not result["OK"]:
        gLogger.error(result["Message"])
        DIRACexit(1)

    result = MonitoringUtilities.monitorUninstallation(system, component)
    if not result["OK"]:
        gLogger.error(result["Message"])
        DIRACexit(1)
    gLogger.notice(f"Successfully uninstalled component {system}/{component}")
    DIRACexit()


if __name__ == "__main__":
    main()
