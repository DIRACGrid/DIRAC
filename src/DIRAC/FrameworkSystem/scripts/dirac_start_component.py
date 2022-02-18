#!/usr/bin/env python
"""
Start DIRAC component using runsvctrl utility
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

    if len(args) > 2:
        Script.showHelp(exitCode=1)

    if system != "*":
        if len(args) > 1:
            component = args[1]
    #
    from DIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller

    #
    gComponentInstaller.exitOnError = True
    #
    result = gComponentInstaller.runsvctrlComponent(system, component, "u")
    if not result["OK"]:
        print("ERROR:", result["Message"])
        exit(-1)

    gComponentInstaller.printStartupStatus(result["Value"])


if __name__ == "__main__":
    main()
