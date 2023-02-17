#!/usr/bin/env python
"""
Remove files from the FileCatalog (and all replicas from Storage Elements)

Examples:
    $ drm ./some_lfn_file
"""
import os

import DIRAC
from DIRAC import S_OK
from DIRAC.Core.Base.Script import Script
from DIRAC.Interfaces.Utilities.DCommands import DSession
from DIRAC.Interfaces.Utilities.DCommands import DCatalog
from DIRAC.Interfaces.Utilities.DCommands import pathFromArgument
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache


@Script()
def main():
    lfnFileName = ""

    def setLfnFileName(arg):
        global lfnFileName
        lfnFileName = arg
        return S_OK()

    targetSE = ""

    def setSE(arg):
        global targetSE
        targetSE = arg
        return S_OK()

    rmDirFlag = False

    def setDirFlag(arg):
        global rmDirFlag
        rmDirFlag = True
        return S_OK()

    Script.registerArgument(["lfn: logical file name"], mandatory=False)
    Script.registerSwitch("F:", "lfnFile=", "file containing a list of LFNs", setLfnFileName)
    Script.registerSwitch("D:", "destination-se=", "Storage Element from where to remove replica", setSE)
    Script.registerSwitch("r", "", "remove directory recursively", setDirFlag)

    configCache = ConfigCache()
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()

    args = Script.getPositionalArgs()

    session = DSession()
    catalog = DCatalog()

    if not args and not lfnFileName:
        print(f"Error: No argument provided\n{Script.scriptName}:")
        Script.showHelp(exitCode=-1)

    lfns = set()
    for path in args:
        lfns.add(pathFromArgument(session, path))

    if lfnFileName:
        if not os.path.exists(lfnFileName):
            print(f"Error: non-existent file {lfnFileName}:")
            DIRAC.exit(-1)
        lfnFile = open(lfnFileName)
        lfnList = lfnFile.readlines()
        lfnSet = {pathFromArgument(session, lfn.strip()) for lfn in lfnList if lfn}
        lfns.update(lfnSet)

    from DIRAC.Interfaces.API.Dirac import Dirac
    from DIRAC import gLogger
    from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
    from DIRAC.DataManagementSystem.Client.DataManager import DataManager

    dirac = Dirac()
    dm = DataManager()

    nLfns = len(lfns)
    if nLfns > 1:
        gLogger.notice("Removing %d objects" % nLfns)

    exitCode = 0
    goodCounter = 0
    badCounter = 0
    for lfn in lfns:
        if rmDirFlag and not catalog.isFile(lfn):
            result = returnSingleResult(dm.cleanLogicalDirectory(lfn))
            if result["OK"]:
                goodCounter += 1
            else:
                print("ERROR:", result["Message"])
                badCounter += 1
                exitCode = 3
        else:
            if targetSE:
                result = returnSingleResult(dirac.removeReplica(lfn, targetSE, printOutput=False))
            else:
                result = returnSingleResult(dirac.removeFile(lfn, printOutput=False))
            if not result["OK"]:
                if "No such file or directory" == result["Message"]:
                    gLogger.notice(f"{lfn} no such file")
                else:
                    gLogger.error(f"ERROR {lfn}: {result['Message']}")
                    badCounter += 1
                    exitCode = 2
            else:
                goodCounter += 1
                if goodCounter % 10 == 0:
                    gLogger.notice("%d files removed" % goodCounter)
                    if badCounter:
                        gLogger.notice("%d files failed removal" % badCounter)

    gLogger.notice("\n%d object(s) removed in total" % goodCounter)
    if badCounter:
        gLogger.notice("%d object(s) failed removal in total" % badCounter)

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
