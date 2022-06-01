#!/usr/bin/env python
########################################################################
# File : dirac-wms-job-delete
# Author : Stuart Paterson
########################################################################
"""
Delete DIRAC job from WMS, if running it will be killed

Example:
  $ dirac-wms-job-delete 12
  Deleted job 12
"""
import datetime
import os.path

from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerSwitch("f:", "File=", "Get output for jobs with IDs from the file")
    Script.registerSwitch("g:", "JobGroup=", "Get output for jobs in the given group")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["JobID:    DIRAC Job ID"], mandatory=False)
    sws, args = Script.parseCommandLine(ignoreErrors=True)

    import DIRAC
    from DIRAC.Interfaces.API.Dirac import Dirac, parseArguments
    from DIRAC.Core.Utilities.TimeUtilities import toString, day

    dirac = Dirac()

    jobs = []
    for sw, value in sws:
        if sw.lower() in ("f", "file"):
            if os.path.exists(value):
                jFile = open(value)
                jobs += jFile.read().split()
                jFile.close()
        elif sw.lower() in ("g", "jobgroup"):
            group = value
            jobDate = toString(datetime.datetime.utcnow().date() - 30 * day)
            result = dirac.selectJobs(jobGroup=value, date=jobDate)
            if not result["OK"]:
                if "No jobs selected" not in result["Message"]:
                    print("Error:", result["Message"])
                    DIRAC.exit(-1)
            else:
                jobs += result["Value"]

    for arg in parseArguments(args):
        jobs.append(arg)

    if not jobs:
        print("Warning: no jobs selected")
        Script.showHelp()
        DIRAC.exit(0)

    result = dirac.deleteJob(jobs)
    if result["OK"]:
        print("Deleted jobs %s" % ",".join([str(j) for j in result["Value"]]))
        exitCode = 0
    else:
        print(result["Message"])
        exitCode = 2

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
