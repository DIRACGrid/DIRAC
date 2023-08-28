#! /usr/bin/env python
"""
Clean the given directory or a list of directories by removing it and all the
contained files and subdirectories from the physical storage and from the
file catalogs.

Example:
  $ dirac-dms-clean-directory /formation/user/v/vhamar/newDir
  Cleaning directory /formation/user/v/vhamar/newDir ...  OK
"""
import os

from DIRAC import exit as DIRACExit, gLogger
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(("LocalFile: Path to local file containing LFNs", "LFN:       Logical File Name"))

    Script.parseCommandLine()

    # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
    inputFileName = Script.getPositionalArgs(group=True)

    if os.path.exists(inputFileName):
        lfns = [lfn.strip().split()[0] for lfn in sorted(open(inputFileName).read().splitlines())]
    else:
        lfns = [inputFileName]

    from DIRAC.DataManagementSystem.Client.DataManager import DataManager

    dm = DataManager()
    retVal = 0
    for lfn in [lfn for lfn in lfns if lfn]:
        gLogger.notice(f"Cleaning directory {lfn!r} ... ")
        result = dm.cleanLogicalDirectory(lfn)
        if not result["OK"]:
            gLogger.error("Failed to clean directory", result["Message"])
            retVal = -1
        else:
            if not result["Value"]["Failed"]:
                gLogger.notice("OK")
            else:
                for folder, message in result["Value"]["Failed"].items():
                    gLogger.error("Failed to clean folder", f"{folder!r}: {message}")
                    retVal = -1

        DIRACExit(retVal)


if __name__ == "__main__":
    main()
