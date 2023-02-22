#!/usr/bin/env python
"""
Prints the current file datalogue directory.
"""
from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Interfaces.Utilities.DCommands import DSession
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache


@Script()
def main():
    configCache = ConfigCache()
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()

    gLogger.notice(DSession().getCwd())


if __name__ == "__main__":
    main()
