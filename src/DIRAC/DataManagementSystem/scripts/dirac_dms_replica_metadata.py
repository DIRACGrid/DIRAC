#!/usr/bin/env python
"""
Get the given file replica metadata from the File Catalog
"""
import os

from DIRAC import exit as DIRACExit
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(("LocalFile: Path to local file containing LFNs", "LFN:       Logical File Names"))
    Script.registerArgument(" SE:        Storage element")
    Script.parseCommandLine()

    from DIRAC import gLogger
    from DIRAC.DataManagementSystem.Client.DataManager import DataManager

    # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
    inputFileName, storageElement = Script.getPositionalArgs(group=True)

    if os.path.exists(inputFileName):
        inputFile = open(inputFileName)
        string = inputFile.read()
        lfns = [lfn.strip() for lfn in string.splitlines()]
        inputFile.close()
    else:
        lfns = [inputFileName]

    res = DataManager().getReplicaMetadata(lfns, storageElement)
    if not res["OK"]:
        print("Error:", res["Message"])
        DIRACExit(1)

    print(f"{'File'.ljust(100)} {'Migrated'.ljust(8)} {'Cached'.ljust(8)} {'Size (bytes)'.ljust(10)}")
    for lfn, metadata in res["Value"]["Successful"].items():
        print(
            "%s %s %s %s"
            % (
                lfn.ljust(100),
                str(metadata["Migrated"]).ljust(8),
                str(metadata.get("Cached", metadata["Accessible"])).ljust(8),
                str(metadata["Size"]).ljust(10),
            )
        )
    for lfn, reason in res["Value"]["Failed"].items():
        print(f"{lfn.ljust(100)} {reason.ljust(8)}")


if __name__ == "__main__":
    main()
