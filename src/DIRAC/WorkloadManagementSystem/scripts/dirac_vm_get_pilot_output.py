#!/usr/bin/env python
"""
  Get pilot output from a VM
"""

from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from DIRAC import gLogger, exit as DIRACExit
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
    Script.setUsageMessage(
        "\n".join(
            [
                "Get VM pilot output",
                "Usage:",
                "%s [option]... [cfgfile]" % Script.scriptName,
                "Arguments:",
                " cfgfile: DIRAC Cfg with description of the configuration (optional)",
            ]
        )
    )
    Script.parseCommandLine(ignoreErrors=True)
    args = Script.getPositionalArgs()

    from DIRAC.WorkloadManagementSystem.Client.VMClient import VMClient

    if len(args) != 1:
        print(Script.showHelp())
        DIRACExit(-1)

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
