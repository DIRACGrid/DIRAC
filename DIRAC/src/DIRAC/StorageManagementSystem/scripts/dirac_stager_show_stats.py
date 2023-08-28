#! /usr/bin/env python
########################################################################
# File :    dirac-stager-show-stats
# Author :  Daniela Remenska
########################################################################
"""
Reports breakdown of file(s) number/size in different staging states across Storage Elements.
Currently used Cache per SE is also reported. (active pins)

Example:
  $ dirac-stager-show-stats

   Status               SE                   NumberOfFiles        Size(GB)
  --------------------------------------------------------------------------
   Staged               GRIDKA-RDST          1                    4.5535
   StageSubmitted       GRIDKA-RDST          5                    22.586
   Waiting              PIC-RDST             3                    13.6478

  WARNING: the Size for files with Status=New is not yet determined at the point of selection!

   --------------------- current status of the SE Caches from the DB-----------
   GRIDKA-RDST    :      6 replicas with a size of 29.141 GB.

"""
from DIRAC.Core.Base.Script import Script
from DIRAC import gLogger, exit as DIRACExit


@Script()
def main():
    Script.parseCommandLine(ignoreErrors=False)
    from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient

    client = StorageManagerClient()

    res = client.getCacheReplicasSummary()
    if not res["OK"]:
        gLogger.fatal(res["Message"])
        DIRACExit(2)
    stagerInfo = res["Value"]
    outStr = "\n"
    outStr += f"  {'Status'.ljust(20)}"
    outStr += f"  {'SE'.ljust(20)}"
    outStr += f"  {'NumberOfFiles'.ljust(20)}"
    outStr += f"  {'Size(GB)'.ljust(20)}"
    outStr += " \n--------------------------------------------------------------------------\n"
    if stagerInfo:
        for info in stagerInfo.values():
            outStr += f"  {info['Status'].ljust(20)}"
            outStr += f"  {info['SE'].ljust(20)}"
            outStr += f"  {str(info['NumFiles']).ljust(20)}"
            outStr += f"  {str(info['SumFiles']).ljust(20)}\n"
    else:
        outStr += f"  {'Nothing to see here...Bye'}"
    outStr += "  \nWARNING: the Size for files with Status=New is not yet determined at the point of selection!\n"
    outStr += "--------------------- current status of the SE Caches from the DB-----------"
    res = client.getSubmittedStagePins()
    if not res["OK"]:
        gLogger.fatal(res["Message"])
        DIRACExit(2)
    storageElementUsage = res["Value"]
    if storageElementUsage:
        for storageElement in storageElementUsage.keys():
            seDict = storageElementUsage[storageElement]
            seDict["TotalSize"] = int(seDict["TotalSize"] / (1000 * 1000 * 1000.0))
            outStr += " \n {}: {} replicas with a size of {:.3f} GB.".format(
                storageElement.ljust(15),
                str(seDict["Replicas"]).rjust(6),
                seDict["TotalSize"],
            )
    else:
        outStr += "  %s" % "\nStageRequest.getStorageUsage: No active stage/pin requests found."
    gLogger.notice(outStr)
    DIRACExit(0)


if __name__ == "__main__":
    main()
