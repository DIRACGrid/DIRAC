#!/usr/bin/env python
"""
print DCommands session environment variables
"""
import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Interfaces.Utilities.DCommands import DSession
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache


@Script()
def main():
    configCache = ConfigCache()
    Script.registerArgument(
        "[section.]option: section:              display all options in section\n"
        "                  section.option:       display section specific option",
        mandatory=False,
    )
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()

    args = Script.getPositionalArgs()

    session = DSession()

    if not args:
        retVal = session.listEnv()
        if not retVal["OK"]:
            gLogger.error(retVal["Message"])
            DIRAC.exit(-1)
        for o, v in retVal["Value"]:
            gLogger.notice(f"{o} = {v}")
        DIRAC.exit(0)

    section, option = arg.split(".") if "." in (arg := args[0]) else (None, arg)

    result = session.get(section, option) if section else session.getEnv(option)

    if not result["OK"]:
        gLogger.error(result["Message"])
        DIRAC.exit(-1)

    gLogger.notice(result["Value"])


if __name__ == "__main__":
    main()
