#!/usr/bin/env python
"""
Set the status of the replicas of given files at the provided SE
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.DIRACScript import DIRACScript as Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(("LFN:      LFN", "File:     File name containing a list of affected LFNs"))
    Script.registerArgument(" SE:       Name of Storage Element")
    Script.registerArgument(" Status:   New Status for the replica")
    Script.parseCommandLine(ignoreErrors=False)

    import DIRAC
    from DIRAC import gLogger
    from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
    import os

    # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
    inputFileName, storageElement, status = Script.getPositionalArgs(group=True)

    if os.path.exists(inputFileName):
        inputFile = open(inputFileName, "r")
        string = inputFile.read()
        inputFile.close()
        lfns = sorted(string.splitlines())
    else:
        lfns = [inputFileName]

    fc = FileCatalog()

    res = fc.getReplicas(lfns, allStatus=True)
    if not res["OK"]:
        gLogger.error("Failed to get catalog replicas.", res["Message"])
        DIRAC.exit(-1)
    lfnDict = {}
    for lfn, error in res["Value"]["Failed"].items():
        gLogger.error("Failed to get replicas for file.", "%s:%s" % (lfn, error))
    for lfn, replicas in res["Value"]["Successful"].items():
        if storageElement not in replicas.keys():
            gLogger.error("LFN not registered at provided storage element.", "%s %s" % (lfn, storageElement))
        else:
            lfnDict[lfn] = {"SE": storageElement, "PFN": replicas[storageElement], "Status": status}
    if not lfnDict:
        gLogger.error("No files found at the supplied storage element.")
        DIRAC.exit(2)

    res = fc.setReplicaStatus(lfnDict)
    if not res["OK"]:
        gLogger.error("Failed to set catalog replica status.", res["Message"])
        DIRAC.exit(-1)
    for lfn, error in res["Value"]["Failed"].items():
        gLogger.error("Failed to set replica status for file.", "%s:%s" % (lfn, error))
    gLogger.notice(
        "Successfully updated the status of %d files at %s." % (len(res["Value"]["Successful"].keys()), storageElement)
    )
    DIRAC.exit(0)


if __name__ == "__main__":
    main()
