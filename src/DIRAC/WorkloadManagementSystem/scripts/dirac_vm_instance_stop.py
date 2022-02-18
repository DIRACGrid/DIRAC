#!/usr/bin/env python
"""
  Get VM instances available in the configured cloud sites
"""
from DIRAC import gLogger, exit as DIRACExit
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerArgument("site: Site name")
    Script.registerArgument("CE:   Cloud Endpoint Name")
    Script.registerArgument("node: node name")
    Script.parseCommandLine(ignoreErrors=True)
    site, ce, node = Script.getPositionalArgs()

    from DIRAC.WorkloadManagementSystem.Client.VMClient import VMClient

    vmClient = VMClient()
    result = vmClient.stopInstance(site, ce, node)
    if not result["OK"]:
        gLogger.error(result["Message"])
        DIRACExit(-1)

    DIRACExit(0)


if __name__ == "__main__":
    main()
