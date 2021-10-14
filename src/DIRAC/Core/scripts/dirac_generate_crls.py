#!/usr/bin/env python
"""
Generate a single CRLs file
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import sys

from DIRAC import gLogger
from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script
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
