#!/usr/bin/env python
"""
Remove the given file replica or a list of file replicas from the File Catalog
This script should be used with great care as it may leave dark data in the storage!
Use dirac-dms-remove-replicas instead
"""
import os

from DIRAC import exit as dexit
from DIRAC.Core.Base.Script import Script
from DIRAC import gLogger


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(("LocalFile: Path to local file containing LFNs", "LFN:       Logical File Names"))
    Script.registerArgument(" SE:        Storage element")
    Script.parseCommandLine()

    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

    allowUsers = Operations().getValue("DataManagement/AllowUserReplicaManagement", False)

    from DIRAC.Core.Security.ProxyInfo import getProxyInfo

    res = getProxyInfo()
    if not res["OK"]:
        gLogger.fatal("Can't get proxy info", res["Message"])
        dexit(1)
    properties = res["Value"].get("groupProperties", [])

    if not allowUsers:
        if "FileCatalogManagement" not in properties:
            gLogger.error("You need to use a proxy from a group with FileCatalogManagement")
            dexit(5)

    from DIRAC.DataManagementSystem.Client.DataManager import DataManager

    dm = DataManager()
    # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
    inputFileName, storageElementName = Script.getPositionalArgs(group=True)

    if os.path.exists(inputFileName):
        inputFile = open(inputFileName)
        string = inputFile.read()
        lfns = [lfn.strip() for lfn in string.splitlines()]
        inputFile.close()
    else:
        lfns = [inputFileName]

    res = dm.removeReplicaFromCatalog(storageElementName, lfns)
    if not res["OK"]:
        print(res["Message"])
        dexit(0)
    for lfn in sorted(res["Value"]["Failed"]):
        message = res["Value"]["Failed"][lfn]
        print(f"Failed to remove {storageElementName} replica of {lfn}: {message}")
    print(f"Successfully remove {len(res['Value']['Successful'])} catalog replicas at {storageElementName}")


if __name__ == "__main__":
    main()
