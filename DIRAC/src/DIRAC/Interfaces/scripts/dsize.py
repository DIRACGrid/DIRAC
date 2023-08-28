#!/usr/bin/env python
"""
Print file or directory disk usage as recorded in the file catalog.
"""
from signal import signal, SIGPIPE, SIG_DFL

from DIRAC import S_OK
from DIRAC.Core.Base.Script import Script
from DIRAC.Interfaces.Utilities.DCommands import DSession
from DIRAC.Interfaces.Utilities.DCommands import createCatalog
from DIRAC.Interfaces.Utilities.DCommands import pathFromArguments
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache


@Script()
def main():
    class Params:
        def __init__(self):
            self.long = False
            self.rawFiles = False

        def setLong(self, arg=None):
            self.long = True
            return S_OK()

        def getLong(self):
            return self.long

        def setRawFiles(self, arg=None):
            self.rawFiles = True
            return S_OK()

        def getRawFiles(self):
            return self.rawFiles

    params = Params()

    Script.setUsageMessage(
        "\n".join(
            [
                __doc__.split("\n")[1],
                "Usage:",
                f"  {Script.scriptName} [options] [path]",
                "Arguments:",
                " path:     file/directory path",
            ]
        )
    )
    Script.registerArgument("path: file/directory path", mandatory=False)
    Script.registerSwitch("l", "long", "detailled listing", params.setLong)
    Script.registerSwitch("f", "raw-files", "reverse sort order", params.setRawFiles)

    configCache = ConfigCache()
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()

    args = Script.getPositionalArgs()

    from DIRAC.DataManagementSystem.Client.FileCatalogClientCLI import (
        FileCatalogClientCLI,
    )

    session = DSession()

    fccli = FileCatalogClientCLI(createCatalog())

    optstr = ""
    if params.long:
        optstr += "-l "
    if params.rawFiles:
        optstr += "-f "

    for p in pathFromArguments(session, args):
        fccli.do_size(optstr + p)


if __name__ == "__main__":
    main()
