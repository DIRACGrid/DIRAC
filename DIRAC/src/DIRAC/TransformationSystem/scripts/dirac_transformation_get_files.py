#!/usr/bin/env python

"""
Get the files attached to a transformation
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument("transID: transformation ID")
    _, args = Script.parseCommandLine()

    from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

    if len(args) != 1:
        Script.showHelp(exitCode=1)

    tc = TransformationClient()
    res = tc.getTransformationFiles({"TransformationID": args[0]})

    if not res["OK"]:
        DIRAC.gLogger.error(res["Message"])
        DIRAC.exit(2)

    for transfile in res["Value"]:
        DIRAC.gLogger.notice(transfile["LFN"])


if __name__ == "__main__":
    main()
