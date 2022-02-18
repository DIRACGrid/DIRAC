#! /usr/bin/env python
########################################################################
# File :    dirac-wms-jobs-select-output-search
# Author :  Vladimir Romanovsky
########################################################################
"""
Retrieve output sandbox for DIRAC Jobs for the given selection and search for a string in their std.out
"""
import os
from shutil import rmtree

import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerSwitch("", "Status=", "Primary status")
    Script.registerSwitch("", "MinorStatus=", "Secondary status")
    Script.registerSwitch("", "ApplicationStatus=", "Application status")
    Script.registerSwitch("", "Site=", "Execution site")
    Script.registerSwitch("", "Owner=", "Owner (DIRAC nickname)")
    Script.registerSwitch("", "JobGroup=", "Select jobs for specified job group")
    Script.registerSwitch("", "Date=", "Date in YYYY-MM-DD format, if not specified default is today")
    Script.registerSwitch("", "File=", "File name,if not specified default is std.out ")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("String: string to search for")
    _, args = Script.parseCommandLine(ignoreErrors=True)

    # Default values
    status = None
    minorStatus = None
    appStatus = None
    site = None
    owner = None
    jobGroup = None
    date = None
    filename = "std.out"

    if len(args) != 1:
        Script.showHelp()

    searchstring = str(args[0])

    for switch in Script.getUnprocessedSwitches():
        if switch[0].lower() == "status":
            status = switch[1]
        elif switch[0].lower() == "minorstatus":
            minorStatus = switch[1]
        elif switch[0].lower() == "applicationstatus":
            appStatus = switch[1]
        elif switch[0].lower() == "site":
            site = switch[1]
        elif switch[0].lower() == "owner":
            owner = switch[1]
        elif switch[0].lower() == "jobgroup":
            jobGroup = switch[1]
        elif switch[0].lower() == "date":
            date = switch[1]
        elif switch[0].lower() == "file":
            filename = switch[1]

    selDate = date
    if not date:
        selDate = "Today"

    from DIRAC.Interfaces.API.Dirac import Dirac

    dirac = Dirac()
    exitCode = 0
    errorList = []
    resultDict = {}

    result = dirac.selectJobs(
        status=status,
        minorStatus=minorStatus,
        applicationStatus=appStatus,
        site=site,
        owner=owner,
        jobGroup=jobGroup,
        date=date,
    )
    if result["OK"]:
        jobs = result["Value"]
    else:
        print("Error in selectJob", result["Message"])
        DIRAC.exit(2)

    for job in jobs:

        result = dirac.getOutputSandbox(job)
        if result["OK"]:
            if os.path.exists("%s" % job):

                lines = []
                try:
                    lines = open(os.path.join(job, filename)).readlines()
                except Exception as x:
                    errorList.append((job, x))
                for line in lines:
                    if line.count(searchstring):
                        resultDict[job] = line
                rmtree("%s" % (job))
        else:
            errorList.append((job, result["Message"]))
            exitCode = 2

    for result in resultDict.items():
        print(result)

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
