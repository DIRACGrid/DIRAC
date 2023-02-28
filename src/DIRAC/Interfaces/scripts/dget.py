#!/usr/bin/env python
"""
download files from storage element

Examples:
    $ dget ./some_lfn_file /tmp
"""
import os

import DIRAC

from DIRAC.Interfaces.Utilities.DCommands import DSession
from DIRAC.Interfaces.Utilities.DCommands import DCatalog
from DIRAC.Interfaces.Utilities.DCommands import pathFromArgument
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache
from DIRAC.Core.Base.Script import Script
from DIRAC import gLogger


class Params:
    def __init__(self):
        self.recursive = False

    def setRecursive(self, arg=None):
        self.recursive = True

    def getRecursive(self):
        return self.recursive


@Script()
def main():
    params = Params()

    Script.setUsageMessage(
        "\n".join(
            [
                __doc__.split("\n")[1],
                "Usage:",
                f"  {Script.scriptName} lfn... [local_dir]",
                "Arguments:",
                " lfn:          file to download",
                " local_dir:   destination directory",
                "",
                "Examples:",
                "  $ dget ./some_lfn_file /tmp",
            ]
        )
    )

    Script.registerSwitch("r", "recursive", "recursively get contents of lfn", params.setRecursive)

    configCache = ConfigCache()
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()

    args = Script.getPositionalArgs()

    session = DSession()
    catalog = DCatalog()

    from DIRAC.Interfaces.API.Dirac import Dirac

    dirac = Dirac()

    if len(args) < 1:
        gLogger.error("Not enough arguments provided:")
        Script.showHelp()
        DIRAC.exit(-1)

    # lfn
    lfn = pathFromArgument(session, args[0])

    localDir = os.getcwd()
    lfns = [(lfn, localDir)]

    if len(args) > 1:
        # localDir provided must be last argument
        localDir = args[-1]
        lfns = [(pathFromArgument(session, lfn), localDir) for lfn in args[:-1]]

        if not os.path.isdir(localDir):
            gLogger.error("Destination local path must be a directory")
            DIRAC.exit(-1)

    exitCode = 0

    if params.getRecursive():
        newLFNs = []
        for lfn, localDir in lfns:
            # make sure lfn is an existing directory
            if not catalog.isDir(lfn):
                if catalog.isFile(lfn):
                    # lfn is a file: simply add it to the list
                    newLFNs.append((lfn, localDir))
                    continue
                exitCode = -1
                gLogger.error(f"Invalid path: '{lfn}'")
                continue

            retVal = catalog.findFilesByMetadata({}, lfn)

            if not retVal["OK"]:
                exitCode = -2
                gLogger.error(retVal["Message"])
                continue

            # compute new local destination for subtree files
            lfnDirname = os.path.dirname(lfn)
            for newLFN in retVal["Value"]:
                newLocalDir = os.path.dirname(os.path.join(localDir, os.path.relpath(newLFN, lfnDirname)))
                newLFNs.append((newLFN, newLocalDir))

        lfns = newLFNs
    for lfn, localDir in lfns:
        if params.getRecursive():
            if not os.path.exists(localDir):
                os.makedirs(localDir)

        ret = dirac.getFile(lfn, localDir)
        if not ret["OK"]:
            exitCode = -3
            gLogger.error(ret["Message"])

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
