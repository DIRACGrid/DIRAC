#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-get-input
# Author :  Stuart Paterson
########################################################################
"""
Retrieve input sandbox for DIRAC Job

Example:
  $ dirac-wms-job-get-input 13
  Job input sandbox retrieved in InputSandbox13/
"""
import os

import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerSwitch("D:", "Dir=", "Store the output in this directory")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["JobID:    DIRAC Job ID"])
    sws, args = Script.parseCommandLine(ignoreErrors=True)

    from DIRAC.Interfaces.API.Dirac import Dirac, parseArguments

    dirac = Dirac()
    exitCode = 0
    errorList = []

    outputDir = None
    for sw, v in sws:
        if sw in ("D", "Dir"):
            outputDir = v

    for job in parseArguments(args):
        result = dirac.getInputSandbox(job, outputDir=outputDir)
        if result["OK"]:
            if os.path.exists(f"InputSandbox{job}"):
                print(f"Job input sandbox retrieved in InputSandbox{job}/")
        else:
            errorList.append((job, result["Message"]))
            exitCode = 2

    for error in errorList:
        print("ERROR %s: %s" % error)

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
