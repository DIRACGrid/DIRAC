#!/usr/bin/env python
"""
Remove files from the FileCatalog (and all replicas from Storage Elements)

Examples:
    $ drm /your/lfn/goes/here
    $ drm -F myfilecontaininglfns.txt
"""
import os

import DIRAC
from DIRAC import S_OK, gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Interfaces.Utilities.DCommands import DSession
from DIRAC.Interfaces.Utilities.DCommands import DCatalog
from DIRAC.Interfaces.Utilities.DCommands import pathFromArgument
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache


class Params:
    """handles input options for drm command"""

    def __init__(self):
        """creates a Params class with default values"""
        self.lfnFileName = ""
        self.targetSE = ""
        self.rmDirFlag = False

    def setLfnFileName(self, lfnFile):
        """sets filename for file containing the lfns to de deleted"""
        self.lfnFileName = lfnFile
        return S_OK()

    def setSE(self, targetSE):
        """
        sets the name of the storage element from which the files
        are to be removed
        """
        self.targetSE = targetSE
        return S_OK()

    def setDirFlag(self, _):
        """flag to remove directories recursively"""
        self.rmDirFlag = True
        return S_OK()

    def registerCLISwitches(self):
        """adds options to the DIRAC options parser"""
        Script.registerArgument(["lfn: logical file name"], mandatory=False)
        Script.registerSwitch("F:", "lfnFile=", "file containing a list of LFNs", self.setLfnFileName)
        Script.registerSwitch("D:", "destination-se=", "Storage Element from where to remove replica", self.setSE)
        Script.registerSwitch("r", "", "remove directory recursively", self.setDirFlag)


@Script()
def main():
    """where all the action is"""
    configCache = ConfigCache()
    options = Params()
    options.registerCLISwitches()

    Script.parseCommandLine(ignoreErrors=True)
    args = Script.getPositionalArgs()

    configCache.cacheConfig()

    session = DSession()
    catalog = DCatalog()

    if not args and not options.lfnFileName:
        gLogger.error(f"No argument provided for:\n{Script.scriptName}")
        Script.showHelp(exitCode=-1)

    lfns = set()
    for path in args:
        lfns.add(pathFromArgument(session, path))

    if options.lfnFileName:
        if not os.path.exists(options.lfnFileName):
            gLogger.error(f"non-existent file {options.lfnFileName}:")
            DIRAC.exit(-1)
        lfnFile = open(options.lfnFileName)
        lfnList = lfnFile.readlines()
        # ignore empty lines anywhere in the file
        lfnList = [lfn.strip() for lfn in lfnList]
        lfnSet = {pathFromArgument(session, lfn) for lfn in lfnList if lfn}
        lfns.update(lfnSet)

    from DIRAC.Interfaces.API.Dirac import Dirac
    from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
    from DIRAC.DataManagementSystem.Client.DataManager import DataManager

    dirac = Dirac()
    dm = DataManager()

    nLfns = len(lfns)
    if nLfns > 1:
        gLogger.notice(f"Removing {nLfns} objects")

    exitCode = 0
    goodCounter = 0
    badCounter = 0
    for lfn in lfns:
        if options.rmDirFlag and not catalog.isFile(lfn):
            result = returnSingleResult(dm.cleanLogicalDirectory(lfn))
            if result["OK"]:
                goodCounter += 1
            else:
                gLogger.error(result["Message"])
                badCounter += 1
                exitCode = 3
        else:
            if options.targetSE:
                result = returnSingleResult(dirac.removeReplica(lfn, options.targetSE, printOutput=False))
            else:
                result = returnSingleResult(dirac.removeFile(lfn, printOutput=False))
            if not result["OK"]:
                if "No such file or directory" == result["Message"]:
                    gLogger.notice(f"{lfn} no such file")
                else:
                    gLogger.error(f"{lfn}: {result['Message']}")
                    badCounter += 1
                    exitCode = 2
            else:
                goodCounter += 1
                if goodCounter % 10 == 0:
                    gLogger.notice(f"{goodCounter} files removed")
                    if badCounter:
                        gLogger.notice(f"{badCounter} files failed removal")

    gLogger.notice(f"\n{goodCounter} object(s) removed in total")
    if badCounter:
        gLogger.notice(f"{badCounter} object(s) failed removal in total")

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
