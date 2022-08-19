#!/usr/bin/env python
########################################################################
# File :    dirac-dms-replicate-lfn
# Author  : Stuart Paterson
########################################################################
"""
Replicate an existing LFN to another Storage Element

Example:
  $ dirac-dms-replicate-lfn /formation/user/v/vhamar/Test.txt DIRAC-USER
  {'Failed': {},
   'Successful': {'/formation/user/v/vhamar/Test.txt': {'register': 0.50833415985107422,
                                                        'replicate': 11.878520965576172}}}
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("LFN:      Logical File Name or file containing LFNs")
    Script.registerArgument("Dest:     Valid DIRAC SE")
    Script.registerArgument("Source:   Valid DIRAC SE", default="", mandatory=False)
    Script.registerArgument("Cache:    Local directory to be used as cache", default="", mandatory=False)
    _, args = Script.parseCommandLine(ignoreErrors=True)

    if len(args) > 4:
        Script.showHelp(exitCode=1)

    lfn, seName, sourceSE, localCache = Script.getPositionalArgs(group=True)

    from DIRAC.Interfaces.API.Dirac import Dirac

    dirac = Dirac()
    exitCode = 0

    try:
        f = open(lfn)
        lfns = f.read().splitlines()
        f.close()
    except Exception:
        lfns = [lfn]

    finalResult = {"Failed": [], "Successful": []}
    for lfn in lfns:
        result = dirac.replicateFile(lfn, seName, sourceSE, localCache, printOutput=True)
        if not result["OK"]:
            finalResult["Failed"].append(lfn)
            print("ERROR %s" % (result["Message"]))
            exitCode = 2
        else:
            finalResult["Successful"].append(lfn)

    if len(lfns) > 1:
        print(finalResult)

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
