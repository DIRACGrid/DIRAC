#!/usr/bin/env python

"""
replicate file in the FileCatalog

Can work in two modes.

In the first mode, yuser provides the destination SE with option "-D".
In the second mode, when no destination is given, drepl will look for configuration options "replication_scheme" and "replication_ses".
If found, those variables will define a list of SEs where to put replicas.
If not found drepl will fallback to configuration option "default_se".

Supported schemes for automated replication (in option "replication_scheme") are:
* all() - replicate file to all SEs listed in option "replication_ses"
* first(N) - replicate file to N first SEs listed in option "replication_ses"
* random(N) - replicatefile to N randomly chosen SEs from the list in option "replication_ses"

Examples
    $ drepl ./some_lfn_file
    $ drepl -D SOME-DESTINATION-SE-disk ./some_lfn_file
"""
import DIRAC
from DIRAC import S_OK, gLogger
from DIRAC.Core.Base.Script import Script

from DIRAC.Interfaces.Utilities.DCommands import DSession
from DIRAC.Interfaces.Utilities.DCommands import pathFromArguments
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache


@Script()
def main():
    class Params:
        def __init__(self):
            self.destinationSE = False
            self.sourceSE = False

        def setDestinationSE(self, arg):
            self.destinationSE = arg
            return S_OK()

        def getDestinationSE(self):
            return self.destinationSE

        def setSourceSE(self, arg):
            self.sourceSE = arg
            return S_OK()

        def getSourceSE(self):
            return self.sourceSE

    params = Params()

    Script.registerArgument(["lfn: file entry in the FileCatalog"])
    Script.registerSwitch(
        "D:",
        "destination-se=",
        "Storage Element where to put replica (or a comma separated list)",
        params.setDestinationSE,
    )
    Script.registerSwitch("S:", "source-se=", "source Storage Element for replication", params.setSourceSE)

    configCache = ConfigCache()
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()

    args = Script.getPositionalArgs()

    session = DSession()

    Script.enableCS()

    from DIRAC.Interfaces.API.Dirac import Dirac

    dirac = Dirac()

    # default lfn: same file name as local_path
    lfns = pathFromArguments(session, args)

    # destination SE
    dsts = []

    if params.destinationSE:
        dsts = params.destinationSE.split(",")
    else:
        dsts = session.getReplicationSEs()
        if not dsts:
            dsts = [session.getEnv("default_se", "DIRAC-USER")["Value"]]

    srcopt = ""
    if params.sourceSE:
        srcopt = params.sourceSE

    exitCode = 0

    for lfn in lfns:
        for dst in dsts:
            ret = dirac.replicateFile(lfn, dst, srcopt)

            if not ret["OK"]:
                gLogger.error(f"{lfn} -> {dst}:", ret["Message"])
                exitCode = -2

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
