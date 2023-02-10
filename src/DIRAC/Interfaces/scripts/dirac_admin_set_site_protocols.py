#! /usr/bin/env python
########################################################################
# File :    dirac-admin-set-site-protocols
# Author :  Stuart Paterson
########################################################################
"""
Defined protocols for each SE for a given site.

Example:
  $ dirac-admin-set-site-protocols --Site=LCG.IN2P3.fr SRM2
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerSwitch("", "Site=", "Site for which protocols are to be set (mandatory)")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["Protocol: SE access protocol"], mandatory=False)
    switches, args = Script.parseCommandLine(ignoreErrors=True)

    site = None
    for switch in switches:
        if switch[0].lower() == "site":
            site = switch[1]

    if not site or not args:
        Script.showHelp(exitCode=1)

    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

    diracAdmin = DiracAdmin()
    exitCode = 0
    result = diracAdmin.setSiteProtocols(site, args, printOutput=True)
    if not result["OK"]:
        print(f"ERROR: {result['Message']}")
        exitCode = 2

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
