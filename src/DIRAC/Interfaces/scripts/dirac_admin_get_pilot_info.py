#!/usr/bin/env python
"""
Retrieve available info about the given pilot

Example:
  $ dirac-admin-get-pilot-info https://marlb.in2p3.fr:9000/26KCLKBFtxXKHF4_ZrQjkw
  {'https://marlb.in2p3.fr:9000/26KCLKBFtxXKHF4_ZrQjkw': {'AccountingSent': 'False',
                                                          'BenchMark': 0.0,
                                                          'DestinationSite': 'cclcgceli01.in2p3.fr',
                                                          'GridSite': 'LCG.IN2P3.fr',
                                                          'GridType': 'gLite',
                                                          'LastUpdateTime': datetime.datetime(2011, 2, 21, 12, 49, 14),
                                                          'VO': 'biomed',
                                                          'PilotJobReference': 'https://marlb.in2p3.fr:9000/2KHFrQjkw',
                                                          'PilotStamp': '',
                                                          'Status': 'Done',
                                                          'SubmissionTime': datetime.datetime(2011, 2, 21, 12, 27, 52)}}
"""
from DIRAC.Core.Base.Script import Script

extendedPrint = False


def setExtendedPrint(_arg):
    global extendedPrint
    extendedPrint = True


@Script()
def main():
    Script.registerSwitch("e", "extended", "Get extended printout", setExtendedPrint)
    _, args = Script.parseCommandLine(ignoreErrors=True)

    from DIRAC import exit as DIRACExit
    from DIRAC.Interfaces.API.Dirac import Dirac
    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

    diracAdmin = DiracAdmin()
    dirac = Dirac()
    exitCode = 0
    errorList = []

    for gridID in args:
        result = diracAdmin.getPilotInfo(gridID)
        if not result["OK"]:
            errorList.append((gridID, result["Message"]))
            exitCode = 2
        else:
            res = result["Value"][gridID]
            if extendedPrint:
                tab = ""
                for key in [
                    "PilotJobReference",
                    "Status",
                    "VO",
                    "SubmissionTime",
                    "DestinationSite",
                    "GridSite",
                ]:
                    if key in res:
                        diracAdmin.log.notice(f"{tab}{key}: {res[key]}")
                        if not tab:
                            tab = "  "
                diracAdmin.log.notice("")
                for jobID in res["Jobs"]:
                    tab = "  "
                    result = dirac.getJobAttributes(int(jobID))
                    if not result["OK"]:
                        errorList.append((gridID, result["Message"]))
                        exitCode = 2
                    else:
                        job = result["Value"]
                        diracAdmin.log.notice(f"{tab}Job ID: {jobID}")
                        tab += "  "
                        for key in [
                            "JobName",
                            "Status",
                            "VO",
                            "StartExecTime",
                            "LastUpdateTime",
                            "EndExecTime",
                        ]:
                            if key in job:
                                diracAdmin.log.notice(f"{tab}{key}:", job[key])
                diracAdmin.log.notice("")
            else:
                print(diracAdmin.pPrint.pformat({gridID: res}))

    for job, error in errorList:
        print(f"ERROR for {job}: {error}")

    DIRACExit(exitCode)


if __name__ == "__main__":
    main()
