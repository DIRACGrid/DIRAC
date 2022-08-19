#!/usr/bin/env python
"""
Remove the given file replica or a list of file replicas from the File Catalog
and from the storage.

Example:
  $ dirac-dms-remove-replicas /formation/user/v/vhamar/Test.txt IBCP-disk
  Successfully removed DIRAC-USER replica of /formation/user/v/vhamar/Test.txt
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(("LocalFile: Path to local file containing LFNs", "LFN:       Logical File Names"))
    Script.registerArgument(["SE:        Storage element"])

    Script.parseCommandLine()

    from DIRAC.Core.Utilities.List import breakListIntoChunks
    from DIRAC.DataManagementSystem.Client.DataManager import DataManager

    dm = DataManager()
    import os

    # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
    first, storageElementNames = Script.getPositionalArgs(group=True)

    if os.path.exists(first):
        with open(first) as inputFile:
            string = inputFile.read()
        lfns = [lfn.strip() for lfn in string.splitlines()]
        inputFile.close()
    else:
        lfns = [first]

    for lfnList in breakListIntoChunks(sorted(lfns, reverse=True), 500):
        for storageElementName in storageElementNames:
            res = dm.removeReplica(storageElementName, lfnList)
            if not res["OK"]:
                print("Error:", res["Message"])
                continue
            for lfn in sorted(res["Value"]["Successful"]):
                print(f"Successfully removed {storageElementName} replica of {lfn}")
            for lfn in sorted(res["Value"]["Failed"]):
                message = res["Value"]["Failed"][lfn]
                print(f"Error: failed to remove {storageElementName} replica of {lfn}: {message}")


if __name__ == "__main__":
    main()
