#!/usr/bin/env python
########################################################################
# File :    dirac-dms-lfn-accessURL
# Author :  Stuart Paterson
########################################################################
"""
Retrieve an access URL for an LFN replica given a valid DIRAC SE.

Example:
  $ dirac-dms-lfn-accessURL /formation/user/v/vhamar/Example.txt DIRAC-USER
  {'Failed': {},
   'Successful': {'/formation/user/v/vhamar/Example.txt': 'dips://dirac.in2p3.fr:9148/DataManagement/StorageElement\
   /formation/user/v/vhamar/Example.txt'}}
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("LFN:      Logical File Name or file containing LFNs")
    Script.registerArgument("SE:       Valid DIRAC SE")
    Script.registerArgument("PROTO:    Optional protocol for accessURL", default=False, mandatory=False)
    _, args = Script.parseCommandLine(ignoreErrors=True)
    lfn, seName, proto = Script.getPositionalArgs(group=True)

    # pylint: disable=wrong-import-position
    from DIRAC.Interfaces.API.Dirac import Dirac

    if len(args) > 3:
        print("Only one LFN SE pair will be considered")

    dirac = Dirac()
    exitCode = 0

    try:
        with open(lfn) as f:
            lfns = f.read().splitlines()
    except OSError:
        lfns = [lfn]

    for lfn in lfns:
        result = dirac.getAccessURL(lfn, seName, protocol=proto, printOutput=True)
        if not result["OK"]:
            print("ERROR: ", result["Message"])
            exitCode = 2

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
