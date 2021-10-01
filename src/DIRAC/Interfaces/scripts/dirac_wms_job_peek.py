#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-delete
# Author :  Stuart Paterson
########################################################################
"""
Peek StdOut of the given DIRAC job

Example:
  $ dirac-wms-job-peek 1
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["JobID:    DIRAC Job ID"])
    _, args = Script.parseCommandLine(ignoreErrors=True)

    from DIRAC.Interfaces.API.Dirac import Dirac, parseArguments

    dirac = Dirac()
    exitCode = 0
    errorList = []

    for job in parseArguments(args):

        result = dirac.peekJob(job, printOutput=True)
        if not result["OK"]:
            errorList.append((job, result["Message"]))
            exitCode = 2

    for error in errorList:
        print("ERROR %s: %s" % error)

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
