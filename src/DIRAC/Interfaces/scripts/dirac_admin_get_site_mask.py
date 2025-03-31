#!/usr/bin/env python
"""
Get the list of sites enabled in the mask for job submission

Example:
  $ dirac-admin-get-site-mask
  LCG.CGG.fr
  LCG.CPPM.fr
  LCG.LAPP.fr
  LCG.LPSC.fr
  LCG.M3PEC.fr
  LCG.MSFG.fr
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.parseCommandLine(ignoreErrors=True)

    from DIRAC import exit as DIRACExit, gLogger
    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

    if not (result := DiracAdmin().getSiteMask(printOutput=True, status="Active"))["OK"]:
        gLogger.error(result["Message"])
        DIRACExit(2)

    DIRACExit(0)


if __name__ == "__main__":
    main()
