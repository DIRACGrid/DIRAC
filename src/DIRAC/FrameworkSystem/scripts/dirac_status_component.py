#!/usr/bin/env python
"""
Status of DIRAC components using runsvstat utility

Example:
  $ dirac-status-component
  DIRAC Root Path = /vo/dirac/versions/Lyon-HEAD-1296215324
                                           Name : Runit    Uptime    PID
            WorkloadManagement_PilotStatusAgent : Run        4029     1697
             WorkloadManagement_JobHistoryAgent : Run        4029     167
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.disableCS()
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(
        " System:  Name of the system for the component (default *: all)", mandatory=False, default="*"
    )
    Script.registerArgument(
        (
            "Service: Name of the particular component (default *: all)",
            "Agent:   Name of the particular component (default *: all)",
        ),
        mandatory=False,
        default="*",
    )
    _, args = Script.parseCommandLine()
    system, component = Script.getPositionalArgs(group=True)

    from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller

    if len(args) > 2:
        Script.showHelp(exitCode=1)

    if len(args) > 0:
        system = args[0]
    if system != "*":
        if len(args) > 1:
            component = args[1]
    #
    gComponentInstaller.exitOnError = True
    #
    result = gComponentInstaller.getStartupComponentStatus([system, component])
    if not result["OK"]:
        print("ERROR:", result["Message"])
        exit(-1)

    gComponentInstaller.printStartupStatus(result["Value"])


if __name__ == "__main__":
    main()
