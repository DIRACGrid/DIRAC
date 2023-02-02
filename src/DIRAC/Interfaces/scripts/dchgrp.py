#!/usr/bin/env python
"""
Change file owner's group in file catalog.

Examples:
    $ dchgrp atsareg ././some_lfn_file
    $ dchgrp -R pgay ./
"""
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache
from DIRAC.Interfaces.Utilities.DCommands import DSession
from DIRAC.Interfaces.Utilities.DCommands import pathFromArgument
from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC import S_OK


class Params:
    def __init__(self):
        self.recursive = False

    def setRecursive(self, opt):
        self.recursive = True
        return S_OK()

    def getRecursive(self):
        return self.recursive


@Script()
def main():
    params = Params()

    Script.registerArgument(" group: new group name")
    Script.registerArgument(["Path:  path to file"])
    Script.registerSwitch("R", "recursive", "recursive", params.setRecursive)

    configCache = ConfigCache()
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()

    group, paths = Script.getPositionalArgs(group=True)

    session = DSession()

    lfns = []
    for path in paths:
        lfns.append(pathFromArgument(session, path))

    from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

    fc = FileCatalog()

    for lfn in lfns:
        try:
            pathDict = {lfn: group}
            result = fc.changePathGroup(pathDict, params.recursive)
            if not result["OK"]:
                gLogger.error("Error:", result["Message"])
                break
            if lfn in result["Value"]["Failed"]:
                gLogger.error("Error:", result["Value"]["Failed"][lfn])
        except Exception as x:
            print("Exception:", str(x))


if __name__ == "__main__":
    main()
