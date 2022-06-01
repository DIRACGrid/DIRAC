#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-status
# Author :  Stuart Paterson
########################################################################
"""
Retrieve status of the given DIRAC job

Example:
  $ dirac-wms-job-status 2
  JobID=2 Status=Done; MinorStatus=Execution Complete; Site=EELA.UTFSM.cl;
"""
import os
import datetime
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerSwitch("f:", "File=", "Get status for jobs with IDs from the file")
    Script.registerSwitch("g:", "JobGroup=", "Get status for jobs in the given group")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["JobID:    DIRAC Job ID"], mandatory=False)
    sws, args = Script.parseCommandLine(ignoreErrors=True)

    from DIRAC import exit as DIRACExit
    from DIRAC.Core.Utilities.TimeUtilities import toString, day
    from DIRAC.Interfaces.API.Dirac import Dirac, parseArguments

    dirac = Dirac()
    exitCode = 0

    jobs = []
    for key, value in sws:
        if key.lower() in ("f", "file"):
            if os.path.exists(value):
                jFile = open(value)
                jobs += jFile.read().split()
                jFile.close()
        elif key.lower() in ("g", "jobgroup"):
            jobDate = toString(datetime.datetime.utcnow().date() - 30 * day)
            # Choose jobs no more than 30 days old
            result = dirac.selectJobs(jobGroup=value, date=jobDate)
            if not result["OK"]:
                print("Error:", result["Message"])
                DIRACExit(-1)
            jobs += result["Value"]

    if len(args) < 1 and not jobs:
        Script.showHelp(exitCode=1)

    if len(args) > 0:
        jobs += parseArguments(args)

    result = dirac.getJobStatus(jobs)
    if result["OK"]:
        for job in result["Value"]:
            print("JobID=" + str(job), end=" ")
            for status in result["Value"][job].items():
                print("%s=%s;" % status, end=" ")
            print()
    else:
        exitCode = 2
        print("ERROR: %s" % result["Message"])

    DIRACExit(exitCode)


if __name__ == "__main__":
    main()
