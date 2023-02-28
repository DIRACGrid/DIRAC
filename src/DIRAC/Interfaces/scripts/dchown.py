#!/usr/bin/env python
"""
Change file owner

Examples:
    $ dchown atsareg ././some_lfn_file
    $ dchown -R pgay ./
"""
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache
from DIRAC.Interfaces.Utilities.DCommands import DSession
from DIRAC.Interfaces.Utilities.DCommands import pathFromArgument
from DIRAC.Core.Base.Script import Script
from DIRAC import gLogger, S_OK


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

    Script.registerArgument(" owner: new owner name")
    Script.registerArgument(["Path:  path to file"])
    Script.registerSwitch("R", "recursive", "recursive", params.setRecursive)

    configCache = ConfigCache()
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()

    owner, paths = Script.getPositionalArgs(group=True)

    session = DSession()

    lfns = []
    for path in paths:
        lfns.append(pathFromArgument(session, path))

    from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

    fc = FileCatalog()

    for lfn in lfns:
        try:
            pathDict = {lfn: owner}
            result = fc.changePathOwner(pathDict, params.recursive)
            if not result["OK"]:
                gLogger.error(result["Message"])
                break
            if lfn in result["Value"]["Failed"]:
                gLogger.error(result["Value"]["Failed"][lfn])
        except Exception as err:
            gLogger.exception(err)


if __name__ == "__main__":
    main()
