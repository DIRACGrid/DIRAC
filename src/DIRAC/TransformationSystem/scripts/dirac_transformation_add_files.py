#!/usr/bin/env python
"""
Add files to an existing transformation
"""
import os
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("TransID: transformation ID")
    Script.registerArgument(("LFN: LFN", "FileName: file containing LFNs"))
    Script.parseCommandLine()

    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

    tID, inputFileName = Script.getPositionalArgs(group=True)

    # get arguments
    lfns = []
    if os.path.exists(inputFileName):
        inputFile = open(inputFileName)
        string = inputFile.read()
        inputFile.close()
        lfns.extend([lfn.strip() for lfn in string.splitlines()])
    else:
        lfns.append(inputFileName)

    tc = TransformationClient()
    res = tc.addFilesToTransformation(tID, lfns)  # Files added here

    if not res["OK"]:
        DIRAC.gLogger.error(res["Message"])
        DIRAC.exit(2)

    successfullyAdded = 0
    alreadyPresent = 0
    for lfn, message in res["Value"]["Successful"].items():
        if message == "Added":
            successfullyAdded += 1
        elif message == "Present":
            alreadyPresent += 1

    if successfullyAdded > 0:
        DIRAC.gLogger.notice("Successfully added %d files" % successfullyAdded)
    if alreadyPresent > 0:
        DIRAC.gLogger.notice("Already present %d files" % alreadyPresent)
    DIRAC.exit(0)


if __name__ == "__main__":
    main()
