#!/usr/bin/env python
"""
Generate a single CA file with all the PEMs
"""
import sys

from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Security import Utilities


@Script()
def main():
    Script.parseCommandLine(ignoreErrors=True)

    result = Utilities.generateCAFile()
    if not result["OK"]:
        gLogger.error(result["Message"])
        sys.exit(1)


if __name__ == "__main__":
    main()
