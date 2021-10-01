#!/usr/bin/env python
"""
  Get VM instances available in the configured cloud sites
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
                "Get VM nodes information",
                "Usage:",
                "%s site ce node [option]... [cfgfile]" % Script.scriptName,
                "Arguments:",
                " cfgfile: DIRAC Cfg with description of the configuration (optional)",
            ]
        )
    )
    Script.parseCommandLine(ignoreErrors=True)
    args = Script.getPositionalArgs()

    from DIRAC.WorkloadManagementSystem.Client.VMClient import VMClient
    from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup

    if len(args) != 3:
        print(Script.showHelp())
        DIRACExit(-1)

    site, ce, node = args

    vmClient = VMClient()
    result = vmClient.stopInstance(site, ce, node)
    if not result["OK"]:
        gLogger.error(result["Message"])
        DIRACExit(-1)

    DIRACExit(0)


if __name__ == "__main__":
    main()
