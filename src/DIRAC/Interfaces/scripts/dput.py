#!/usr/bin/env python

"""
Uploads a file to an SE and registers it in the file catalog.
Uses the default SE specified in $HOME/.dirac/dcommands.conf unless overridden
on the command line.
"""
import os

import DIRAC
from DIRAC import S_OK, gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Interfaces.Utilities.DCommands import DSession
from DIRAC.Interfaces.Utilities.DCommands import DCatalog
from DIRAC.Interfaces.Utilities.DCommands import pathFromArgument
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache


@Script()
def main():
    class Params:
        def __init__(self):
            self.destinationSE = False
            self.recursive = False

        def setDestinationSE(self, arg):
            self.destinationSE = arg
            return S_OK()

        def getDestinationSE(self):
            return self.destinationSE

        def setRecursive(self, arg=None):
            self.recursive = True

        def getRecursive(self):
            return self.recursive

    params = Params()

    Script.setUsageMessage(
        "\n".join(
            [
                __doc__.split("\n")[1],
                "Usage:",
                f"  {Script.scriptName} [options] local_path[... lfn]",
                "Arguments:",
                " local_path:   local file",
                " lfn:          file or directory entry in the file catalog",
                "",
                "Examples:",
                "  $ dput some_local_file ./some_lfn_file",
                "  $ dput local_file1 local_file2 ./some_lfn_dir/",
            ]
        )
    )
    Script.registerSwitch(
        "D:",
        "destination-se=",
        "Storage Element where to put replica",
        params.setDestinationSE,
    )
    Script.registerSwitch("r", "recursive", "recursively put contents of local_path", params.setRecursive)

    configCache = ConfigCache()
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()

    args = Script.getPositionalArgs()

    session = DSession()
    catalog = DCatalog()

    from DIRAC.Interfaces.API.Dirac import Dirac

    dirac = Dirac()

    if len(args) < 1:
        gLogger.error(f"No argument provided\n{Script.scriptName}:")
        Script.showHelp(exitCode=-1)

    # local file
    localPath = args[0]

    # default lfn: same file name as localPath
    lfn = pathFromArgument(session, os.path.basename(localPath))
    pairs = [(localPath, lfn)]

    if len(args) > 1:
        # lfn provided must be last argument
        lfn = pathFromArgument(session, args[-1])
        localPaths = args[:-1]
        pairs = []
        if catalog.isDir(lfn):
            # we can accept one ore more local files
            for lp in localPaths:
                pairs.append((lp, os.path.join(lfn, os.path.basename(lp))))
        else:
            if len(localPaths) > 1:
                gLogger.error("Destination LFN must be a directory when registering multiple local files")
                DIRAC.exit(-1)

            # lfn filename replace local filename
            pairs.append((localPath, lfn))

    # destination SE
    se = params.getDestinationSE()
    if not se:
        retVal = session.getEnv("default_se", "NO DEFAULT")
        # the returned error message is not very user friendly
        # use "NO DEFAULT" to distinguish no default SE set from all
        # other error cases
        if not retVal["OK"]:
            gLogger.error(retVal["Message"])
        if retVal["Value"] == "NO DEFAULT":
            gLogger.error(
                "No default SE specified, please set default SE or specify SE on command line using -D option"
            )
            DIRAC.exit(-1)
        se = retVal["Value"]

    exitCode = 0

    if params.getRecursive():
        newPairs = []
        for localPath, lfn in pairs:
            if os.path.isdir(localPath):
                for path, _subdirs, files in os.walk(localPath):
                    newLFNDir = os.path.normpath(os.path.join(lfn, os.path.relpath(path, localPath)))
                    for f in files:
                        pairs.append((os.path.join(path, f), os.path.join(newLFNDir, f)))
            else:
                newPairs.append((localPath, lfn))
        pairs = newPairs

    for localPath, lfn in pairs:
        ret = dirac.addFile(lfn, localPath, se, printOutput=False)

        if not ret["OK"]:
            exitCode = -2
            gLogger.error(f"{lfn}:", ret["Message"])

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
