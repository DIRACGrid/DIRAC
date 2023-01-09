#!/usr/bin/env python

"""This script allows one to print information about a (list of) transformations.
"""


from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.TransformationSystem.Utilities.ScriptUtilities import getTransformations


@Script()
def main():

    informations = [
        "AuthorDN",
        "AuthorGroup",
        "Body",
        "CreationDate",
        "Description",
        "EventsPerTask",
        "FileMask",
        "GroupSize",
        "InheritedFrom",
        "LastUpdate",
        "LongDescription",
        "MaxNumberOfTasks",
        "Plugin",
        "Status",
        "TransformationGroup",
        "TransformationName",
        "Type",
        "Request",
    ]
    Script.registerSwitch("", "Information=", "   Specify which information is required")
    Script.parseCommandLine(ignoreErrors=True)

    tr = TransformationClient()

    requestedInfo = informations
    switches = Script.getUnprocessedSwitches()
    infoList = []
    for switch, val in switches:
        if switch == "Information":
            infoList = [info.lower() for info in val.split(",")]
            requestedInfo = [info for info in informations if info.lower() in infoList]
    if "body" not in infoList and "Body" in requestedInfo:
        requestedInfo.remove("Body")

    transIDs = getTransformations(Script.getPositionalArgs())

    for transID in transIDs:
        try:
            res = tr.getTransformation(int(transID))
            gLogger.notice(f"==== Transformation {transID} ====")
            for info in requestedInfo:
                getInfo = info if info != "Request" else "TransformationFamily"
                gLogger.notice(f"\t{info}: {res.get('Value', {}).get(getInfo, 'Unknown')}")
        except Exception:
            gLogger.error("Invalid transformation ID", transID)


if __name__ == "__main__":
    main()
