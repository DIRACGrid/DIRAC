#!/usr/bin/env python
"""
remove FileCatalog directories. Attention ! This command does not remove
directories and files on the physical storage.

Examples:
    $ drmdir ./some_lfn_directory
"""
from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Interfaces.Utilities.DCommands import DSession
from DIRAC.Interfaces.Utilities.DCommands import createCatalog
from DIRAC.Interfaces.Utilities.DCommands import pathFromArguments
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache


@Script()
def main():
    configCache = ConfigCache()
    Script.registerArgument(["Path: directory path"])
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()

    args = Script.getPositionalArgs()

    session = DSession()
    catalog = createCatalog()

    result = catalog.removeDirectory(pathFromArguments(session, args))
    if result["OK"]:
        if result["Value"]["Failed"]:
            for p in result["Value"]["Failed"]:
                gLogger.error(f'"{p}":', result["Value"]["Failed"][p])
    else:
        gLogger.error(result["Message"])


if __name__ == "__main__":
    main()
