#!/usr/bin/env python
"""
Print list replicas for files in the FileCatalog
"""
import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.Interfaces.Utilities.DCommands import DSession
from DIRAC.Interfaces.Utilities.DCommands import DCatalog
from DIRAC.Interfaces.Utilities.DCommands import pathFromArgument
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache


@Script()
def main():
    configCache = ConfigCache()
    Script.registerArgument(["lfn: logical file name"])
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()

    args = Script.getPositionalArgs()

    session = DSession()
    catalog = DCatalog()

    exitCode = 0

    for arg in args:
        # lfn
        lfn = pathFromArgument(session, args[0])
        # fccli.do_replicas( lfn )
        ret = returnSingleResult(catalog.catalog.getReplicas(lfn))
        if ret["OK"]:
            replicas = ret["Value"]
            gLogger.notice(f"{lfn}:")
            for se, path in replicas.items():
                gLogger.notice("  ", f"{se} {path}")
        else:
            gLogger.error(f"{lfn}:", ret["Message"])
            exitCode = -2

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
