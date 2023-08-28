#! /usr/bin/env python
"""
Change status of replica of a given file or a list of files at a given Storage Element
"""
from DIRAC import exit as DIRACExit
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(("LocalFile: Path to local file containing LFNs", "LFN:       Logical File Name"))
    Script.registerArgument(" SE:        Storage Element")
    Script.registerArgument(" status:    status")
    Script.parseCommandLine()

    from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

    catalog = FileCatalog()
    import os

    # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
    inputFileName, se, newStatus = Script.getPositionalArgs(group=True)

    if os.path.exists(inputFileName):
        inputFile = open(inputFileName)
        string = inputFile.read()
        lfns = string.splitlines()
        inputFile.close()
    else:
        lfns = [inputFileName]

    res = catalog.getReplicas(lfns, True)
    if not res["OK"]:
        print(res["Message"])
        DIRACExit(-1)
    replicas = res["Value"]["Successful"]

    lfnDict = {}
    for lfn in lfns:
        lfnDict[lfn] = {}
        lfnDict[lfn]["SE"] = se
        lfnDict[lfn]["Status"] = newStatus
        lfnDict[lfn]["PFN"] = replicas[lfn][se]

    res = catalog.setReplicaStatus(lfnDict)
    if not res["OK"]:
        print("ERROR:", res["Message"])
    if res["Value"]["Failed"]:
        print(f"Failed to update {len(res['Value']['Failed'])} replica status")
    if res["Value"]["Successful"]:
        print(f"Successfully updated {len(res['Value']['Successful'])} replica status")


if __name__ == "__main__":
    main()
