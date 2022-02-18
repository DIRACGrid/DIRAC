#!/usr/bin/env python
"""
  Get pilot output from a VM
"""
from DIRAC import gLogger, exit as DIRACExit
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerArgument("pilotRef: pilot reference")
    Script.parseCommandLine(ignoreErrors=True)
    args = Script.getPositionalArgs()

    from DIRAC.WorkloadManagementSystem.Client.VMClient import VMClient

    pilotRef = args[0]

    vmClient = VMClient()
    result = vmClient.getPilotOutput(pilotRef)
    if not result["OK"]:
        gLogger.error(result["Message"])
        DIRACExit(-1)

    print(result)

    DIRACExit(0)


if __name__ == "__main__":
    main()
