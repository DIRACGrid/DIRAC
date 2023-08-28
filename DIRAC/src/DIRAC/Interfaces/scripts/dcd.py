#!/usr/bin/env python
"""
Change current DIRAC File Catalog working directory

Examples:
    $ dcd /dirac/user
    $ dcd
"""
import DIRAC
from DIRAC.Interfaces.Utilities.DCommands import DSession
from DIRAC.Interfaces.Utilities.DCommands import DCatalog
from DIRAC.Interfaces.Utilities.DCommands import pathFromArgument
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache
from DIRAC.Core.Base.Script import Script
from DIRAC import gLogger


@Script()
def main():
    Script.registerArgument(
        "Path:     path to new working directory (defaults to home directory)",
        mandatory=False,
    )

    configCache = ConfigCache()
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()

    args = Script.getPositionalArgs()

    session = DSession()

    if len(args) > 1:
        gLogger.notice(f"Too many arguments provided\n{Script.scriptName}:")
        Script.showHelp(exitCode=-1)

    if len(args):
        arg = pathFromArgument(session, args[0])
    else:
        arg = session.homeDir()

    catalog = DCatalog()

    if catalog.isDir(arg):
        if session.getCwd() != arg:
            session.setCwd(arg)
            session.write()
    else:
        gLogger.error(f'"{arg}" not a valid directory')
        DIRAC.exit(-1)


if __name__ == "__main__":
    main()
