#!/usr/bin/env python
########################################################################
# File :    dirac-admin-get-site-mask
# Author :  Stuart Paterson
########################################################################
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

    diracAdmin = DiracAdmin()

    gLogger.setLevel("ALWAYS")

    result = diracAdmin.getSiteMask(printOutput=True, status="Active")
    if result["OK"]:
        DIRACExit(0)
    else:
        print(result["Message"])
        DIRACExit(2)


if __name__ == "__main__":
    main()
