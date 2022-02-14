#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-attributes
# Author :  Stuart Paterson
########################################################################
"""
Retrieve attributes associated with the given DIRAC job

Example:
  $ dirac-wms-job-attributes  1
  {'AccountedFlag': 'False',
   'ApplicationNumStatus': '0',
   'ApplicationStatus': 'Unknown',
   'CPUTime': '0.0',
   'DIRACSetup': 'EELA-Production',
   'DeletedFlag': 'False',
   'EndExecTime': '2011-02-14 11:28:01',
   'FailedFlag': 'False',
   'HeartBeatTime': '2011-02-14 11:28:01',
   'ISandboxReadyFlag': 'False',
   'JobGroup': 'NoGroup',
   'JobID': '1',
   'JobName': 'DIRAC_vhamar_602138',
   'JobSplitType': 'Single',
   'JobType': 'normal',
   'KilledFlag': 'False',
   'LastUpdateTime': '2011-02-14 11:28:11',
   'MasterJobID': '0',
   'MinorStatus': 'Execution Complete',
   'OSandboxReadyFlag': 'False',
   'Owner': 'vhamar',
   'OwnerDN': '/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar',
   'OwnerGroup': 'eela_user',
   'RescheduleCounter': '0',
   'RescheduleTime': 'None',
   'RetrievedFlag': 'False',
   'RunNumber': '0',
   'Site': 'EELA.UTFSM.cl',
   'StartExecTime': '2011-02-14 11:27:48',
   'Status': 'Done',
   'SubmissionTime': '2011-02-14 10:12:40',
   'SystemPriority': '0',
   'UserPriority': '1',
   'VerifiedFlag': 'True'}
"""
import DIRAC
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
        print("ERROR %s: %s" % error)

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
