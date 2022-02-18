#!/usr/bin/env python
########################################################################
# File :    dirac-admin-get-job-pilot-output
# Author :  Stuart Paterson
########################################################################
"""
Retrieve the output of the pilot that executed a given job

Example:
  $ dirac-admin-get-job-pilot-output 34
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["JobID:    DIRAC ID of the Job"])
    # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
    _, args = Script.parseCommandLine(ignoreErrors=True)

    from DIRAC import exit as DIRACExit
    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

    diracAdmin = DiracAdmin()
    exitCode = 0
    errorList = []

    for job in args:

        try:
            job = int(job)
        except Exception:
            errorList.append(("Expected integer for JobID", job))
            exitCode = 2
            continue

        result = diracAdmin.getJobPilotOutput(job)
        if not result["OK"]:
            errorList.append((job, result["Message"]))
            exitCode = 2

    for error in errorList:
        print("ERROR %s: %s" % error)

    DIRACExit(exitCode)


if __name__ == "__main__":
    main()
