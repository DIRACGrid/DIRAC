#!/usr/bin/env python
########################################################################
# File :    dirac-wms-job-get-jdl
# Author :  Stuart Paterson
########################################################################
"""
Retrieve the current JDL of a DIRAC job

Usage:
  dirac-wms-job-get-jdl [options] ... JobID ...

Arguments:
  JobID:    DIRAC Job ID

Example:
  $ dirac-wms-job-get-jdl 1
  {'Arguments': '-ltrA',
   'CPUTime': '86400',
   'Executable': '/bin/ls',
   'JobID': '1',
   'JobName': 'DIRAC_vhamar_602138',
   'JobRequirements': '[OwnerGroup = eela_user;
                        Setup = EELA-Production;
                        UserPriority = 1;
                        CPUTime = 0 ]',
   'OutputSandbox': ['std.out', 'std.err'],
   'Owner': 'vhamar',
   'OwnerGroup': 'eela_user',
   'Priority': '1'}
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    original = False
    Script.registerSwitch("O", "Original", "Gets the original JDL")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["JobID:    DIRAC Job ID"])
    sws, args = Script.parseCommandLine(ignoreErrors=True)

    for switch in sws:
        if switch[0] == "Original" or switch[0] == "O":
            original = True

    from DIRAC.Interfaces.API.Dirac import Dirac, parseArguments

    dirac = Dirac()
    exitCode = 0
    errorList = []

    for job in parseArguments(args):
        result = dirac.getJobJDL(job, original=original, printOutput=True)
        if not result["OK"]:
            errorList.append((job, result["Message"]))
            exitCode = 2

    for error in errorList:
        print("ERROR %s: %s" % error)

    DIRAC.exit(exitCode)


if __name__ == "__main__":
    main()
