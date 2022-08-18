#!/usr/bin/env python
"""
Get the size of the given file or a list of files

Example:
  $ dirac-dms-data-size  /formation/user/v/vhamar/Example.txt
  ------------------------------
  Files          |      Size (GB)
  ------------------------------
  1              |            0.0
  ------------------------------
"""
import os
import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    unit = "GB"
    Script.registerSwitch("u:", "Unit=", "   Unit to use [default %s] (MB,GB,TB,PB)" % unit)
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(("LocalFile: Path to local file containing LFNs", "LFN:       Logical File Name"))
    Script.registerArgument(["LFN:       Logical File Name"], mandatory=False)
    unprSwitches, args = Script.parseCommandLine(ignoreErrors=False)

    for switch in unprSwitches:
        if switch[0].lower() == "u" or switch[0].lower() == "unit":
            unit = switch[1]
    scaleDict = {
        "MB": 1000 * 1000.0,
        "GB": 1000 * 1000 * 1000.0,
        "TB": 1000 * 1000 * 1000 * 1000.0,
        "PB": 1000 * 1000 * 1000 * 1000 * 1000.0,
    }
    if unit not in scaleDict.keys():
        gLogger.error("Unit must be one of MB,GB,TB,PB")
        DIRAC.exit(2)
    scaleFactor = scaleDict[unit]

    lfns = []
    for inputFileName in args:
        if os.path.exists(inputFileName):
            inputFile = open(inputFileName)
            string = inputFile.read()
            inputFile.close()
            lfns.extend([lfn.strip() for lfn in string.splitlines()])
        else:
            lfns.append(inputFileName)

    from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

    res = FileCatalog().getFileSize(lfns)
    if not res["OK"]:
        gLogger.error("Failed to get size of data", res["Message"])
        DIRAC.exit(-2)
    for lfn, reason in res["Value"]["Failed"].items():
        gLogger.error("Failed to get size for %s" % lfn, reason)
    totalSize = 0
    totalFiles = 0
    for lfn, size in res["Value"]["Successful"].items():
        totalFiles += 1
        totalSize += size
    gLogger.notice("-" * 30)
    gLogger.notice("{}|{}".format("Files".ljust(15), ("Size (%s)" % unit).rjust(15)))
    gLogger.notice("-" * 30)
    gLogger.notice("{}|{}".format(str(totalFiles).ljust(15), str("%.1f" % (totalSize / scaleFactor)).rjust(15)))
    gLogger.notice("-" * 30)
    DIRAC.exit(0)


if __name__ == "__main__":
    main()
