#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-get-output-data
# Author :  Stuart Paterson
########################################################################
"""
Retrieve the output data files of a DIRAC job
"""
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

    outputDir = ""
    for sw, v in sws:
        if sw in ("D", "Dir"):
            outputDir = v

    for job in parseArguments(args):
        result = dirac.getJobOutputData(job, destinationDir=outputDir)
        if result["OK"]:
            print(f"Job {job} output data retrieved")
        else:
            errorList.append((job, result["Message"]))
            exitCode = 2

    for error in errorList:
        print("ERROR %s: %s" % error)

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
