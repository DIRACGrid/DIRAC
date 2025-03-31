#!/usr/bin/env python
"""
Reset a job or list of jobs in the WMS

Example:
  $ dirac-admin-reset-job 1848
  Reset Job 1848
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["JobID:    DIRAC Job IDs"])
    _, args = Script.parseCommandLine(ignoreErrors=True)

    from DIRAC import gLogger
    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

    diracAdmin = DiracAdmin()
    exitCode = 0
    errorList = []

    for job in args:
        try:
            job = int(job)
        except Exception as x:
            errorList.append(("Expected integer for jobID", job))
            exitCode = 2
            continue

        result = diracAdmin.resetJob(job)
        if result["OK"]:
            gLogger.notice(f"Reset Job {job}")
        else:
            errorList.append((job, result["Message"]))
            exitCode = 2

    for error in errorList:
        gLogger.error(error)

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
