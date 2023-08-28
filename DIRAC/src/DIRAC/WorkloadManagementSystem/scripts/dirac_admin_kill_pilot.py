#! /usr/bin/env python
########################################################################
# File :    dirac-admin-kill-pilot
# Author :  A.T.
########################################################################
"""
Kill the specified pilot
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("PilotRef: pilot reference")
    _, args = Script.parseCommandLine(ignoreErrors=True)

    pilotRef = args[0]

    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

    diracAdmin = DiracAdmin()
    exitCode = 0

    result = diracAdmin.killPilot(pilotRef)
    if not result["OK"]:
        DIRAC.gLogger.error("Failed to kill pilot", pilotRef)
        DIRAC.gLogger.error(result["Message"])
        exitCode = 1

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
