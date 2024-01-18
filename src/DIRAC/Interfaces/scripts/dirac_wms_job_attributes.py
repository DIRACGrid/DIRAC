#!/usr/bin/env python
"""
Retrieve attributes associated with the given DIRAC job

Example:
  $ dirac-wms-job-attributes  1
  {'AccountedFlag': 'False',
   'ApplicationStatus': 'Unknown',
   'CPUTime': '0.0',
   'EndExecTime': '2011-02-14 11:28:01',
   'HeartBeatTime': '2011-02-14 11:28:01',
   'JobGroup': 'NoGroup',
   'JobID': '1',
   'JobName': 'DIRAC_vhamar_602138',
   'JobType': 'normal',
   'LastUpdateTime': '2011-02-14 11:28:11',
   'MinorStatus': 'Execution Complete',
   'Owner': 'vhamar',
   'OwnerGroup': 'eela_user',
   'VO': 'dteam',
   'RescheduleCounter': '0',
   'RescheduleTime': 'None',
   'Site': 'EELA.UTFSM.cl',
   'StartExecTime': '2011-02-14 11:27:48',
   'Status': 'Done',
   'SubmissionTime': '2011-02-14 10:12:40',
   'UserPriority': '1'}
"""
from DIRAC import exit as dExit
from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script


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
        result = dirac.getJobAttributes(job, printOutput=True)
        if not result["OK"]:
            errorList.append((job, result["Message"]))
            exitCode = 2

    for error in errorList:
        gLogger.error(f"{error}")

    dExit(exitCode)


if __name__ == "__main__":
    main()
