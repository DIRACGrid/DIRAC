#!/usr/bin/env python
"""
Remove the given file or a list of files from the File Catalog and from the storage

Example:
  $ dirac-dms-remove-files /formation/user/v/vhamar/Test.txt
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(("LocalFile: Path to local file containing LFNs", "LFN:       Logical File Names"))
    Script.registerArgument(["LFN:       Logical File Names"], mandatory=False)
    Script.parseCommandLine()

    import os
    import DIRAC
    from DIRAC import gLogger

    first, lfns = Script.getPositionalArgs(group=True)
    if os.path.exists(first):
        with open(first) as inputFile:
            string = inputFile.read()
        lfns.extend([lfn.strip() for lfn in string.splitlines()])
    else:
        lfns.insert(0, first)

    from DIRAC.Core.Utilities.List import breakListIntoChunks
    from DIRAC.DataManagementSystem.Client.DataManager import DataManager

    dm = DataManager()

    errorReasons = {}
    successfullyRemoved = 0
    for lfnList in breakListIntoChunks(lfns, 100):
        res = dm.removeFile(lfnList)
        if not res["OK"]:
            gLogger.error("Failed to remove data", res["Message"])
            DIRAC.exit(-2)
        for lfn, r in res["Value"]["Failed"].items():
            reason = str(r)
            if reason not in errorReasons:
                errorReasons[reason] = []
            errorReasons[reason].append(lfn)
        successfullyRemoved += len(res["Value"]["Successful"])

    for reason, lfns in errorReasons.items():
        gLogger.notice("Failed to remove %d files with error: %s" % (len(lfns), reason))
    if successfullyRemoved > 0:
        gLogger.notice("Successfully removed %d files" % successfullyRemoved)
    DIRAC.exit(0)


if __name__ == "__main__":
    main()
