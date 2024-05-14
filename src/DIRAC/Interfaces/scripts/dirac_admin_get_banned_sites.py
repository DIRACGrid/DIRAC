#!/usr/bin/env python
########################################################################
# File :    dirac-admin-get-banned-sites
# Author :  Stuart Paterson
########################################################################
"""
Get banned sites

Example:
  $ dirac-admin-get-banned-sites
  LCG.IN2P3.fr                      Site not present in logging table
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.parseCommandLine(ignoreErrors=True)

    from DIRAC import gLogger, exit as DIRACExit
    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

    diracAdmin = DiracAdmin()

    result = diracAdmin.getBannedSites()
    if not result["OK"]:
        gLogger.error(result["Message"])
        DIRACExit(2)

    bannedSites = result["Value"]
    for site in bannedSites:
        result = diracAdmin.getSiteMaskLogging(site, printOutput=True)
        if not result["OK"]:
            gLogger.error(result["Message"])
            DIRACExit(2)

    DIRACExit(0)


if __name__ == "__main__":
    main()
