#!/usr/bin/env python
########################################################################
# File :    dirac-admin-lfn-replicas
# Author :  Stuart Paterson
########################################################################
"""
Obtain replica information from file catalogue client.

Example:
  $ dirac-dms-lfn-replicas /formation/user/v/vhamar/Test.txt
  {'Failed': {},
   'Successful': {'/formation/user/v/vhamar/Test.txt':\
   {'M3PEC-disk': 'srm://se0.m3pec.u-bordeaux1.fr/dpm/m3pec.u-bordeaux1.fr/home/formation/user/v/vhamar/Test.txt'}}}
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerSwitch("a", "All", "  Also show inactive replicas")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["LFN:      Logical File Name or file containing LFNs"])
    switches, lfns = Script.parseCommandLine(ignoreErrors=True)

    active = True
    for switch in switches:
        opt = switch[0].lower()
        if opt in ("a", "all"):
            active = False

    from DIRAC.Interfaces.API.Dirac import Dirac

    dirac = Dirac()
    exitCode = 0

    if len(lfns) == 1:
        try:
            with open(lfns[0]) as f:
                lfns = f.read().splitlines()
        except Exception:
            pass

    result = dirac.getReplicas(lfns, active=active, printOutput=True)
    if not result["OK"]:
        print("ERROR: ", result["Message"])
        exitCode = 2

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
