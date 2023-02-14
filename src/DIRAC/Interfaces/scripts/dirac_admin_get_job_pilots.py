#!/usr/bin/env python
########################################################################
# File :    dirac-admin-get-job-pilots
# Author :  Stuart Paterson
########################################################################
"""
Retrieve info about pilots that have matched a given Job

Example:
  $ dirac-admin-get-job-pilots 1848
  {'https://marlb.in2p3.fr:9000/bqYViq6KrVgGfr6wwgT45Q': {'AccountingSent': 'False',
                                                          'BenchMark': 8.1799999999999997,
                                                          'Broker': 'marwms.in2p3.fr',
                                                          'DestinationSite': 'lpsc-ce.in2p3.fr',
                                                          'GridSite': 'LCG.LPSC.fr',
                                                          'GridType': 'gLite',
                                                          'Jobs': [1848L],
                                                          'LastUpdateTime': datetime.datetime(2011, 2, 21, 12, 39, 10),
                                                          'OutputReady': 'True',
                                                          'OwnerDN': '/O=GRID/C=FR/O=CNRS/OU=LPC/CN=Sebastien Guizard',
                                                          'OwnerGroup': '/biomed',
                                                          'ParentID': 0L,
                                                          'PilotID': 2247L,
                                                          'PilotJobReference': 'https://marlb.in2p3.fr:9000/biq6KT45Q',
                                                          'PilotStamp': '',
                                                          'Status': 'Done',
                                                          'SubmissionTime': datetime.datetime(2011, 2, 21, 12, 27, 52),
                                                          'TaskQueueID': 399L}}
"""
# pylint: disable=wrong-import-position
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
        except ValueError:
            errorList.append((job, "Expected integer for jobID"))
            exitCode = 2
            continue

        result = diracAdmin.getJobPilots(job)
        if not result["OK"]:
            errorList.append((job, result["Message"]))
            exitCode = 2

    for error in errorList:
        print("ERROR %s: %s" % error)

    DIRACExit(exitCode)


if __name__ == "__main__":
    main()
