#!/usr/bin/env python
"""
Generate a single CRLs file
"""
import sys

from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Security import Utilities


@Script()
def main():
    Script.parseCommandLine(ignoreErrors=True)

    result = Utilities.generateRevokedCertsFile()
    if not result["OK"]:
        gLogger.error(result["Message"])
        sys.exit(1)


if __name__ == "__main__":
    main()
