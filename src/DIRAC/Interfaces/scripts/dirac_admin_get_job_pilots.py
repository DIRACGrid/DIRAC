#!/usr/bin/env python
"""
Retrieve info about pilots that have matched a given Job

Example:
  $ dirac-admin-get-job-pilots 1848
  {'https://marlb.in2p3.fr:9000/bqYViq6KrVgGfr6wwgT45Q': {'AccountingSent': 'False',
                                                          'BenchMark': 8.1799999999999997,
                                                          'DestinationSite': 'lpsc-ce.in2p3.fr',
                                                          'GridSite': 'LCG.LPSC.fr',
                                                          'GridType': 'gLite',
                                                          'Jobs': [1848L],
                                                          'LastUpdateTime': datetime.datetime(2011, 2, 21, 12, 39, 10),
                                                          'VO': 'biomed',
                                                          'PilotJobReference': 'https://marlb.in2p3.fr:9000/biq6KT45Q',
                                                          'PilotStamp': '',
                                                          'Status': 'Done',
                                                          'SubmissionTime': datetime.datetime(2011, 2, 21, 12, 27, 52)}}
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerArgument(["JobID:    DIRAC ID of the Job"])
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

    for job, error in errorList:
        print(f"ERROR for {job}: {error}")

    DIRACExit(exitCode)


if __name__ == "__main__":
    main()
