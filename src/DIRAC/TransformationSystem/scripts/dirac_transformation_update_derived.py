#!/usr/bin/env python
"""Move files that are Unused or MaxReset from a parent production to its
derived production. The argument is a list of productions: comma separated list
of ranges (a range has the form p1:p2)"""

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient


@Script()
def main():

    Script.registerSwitch("", "NoReset", "Don't reset the MaxReset files to Unused (default is to reset)")
    Script.parseCommandLine(ignoreErrors=True)

    transClient = TransformationClient()

    resetUnused = True
    switches = Script.getUnprocessedSwitches()
    for switch in switches:
        if switch[0] == "NoReset":
            resetUnused = False
    args = Script.getPositionalArgs()

    if not args:
        gLogger.error("Specify transformation number...")
        DIRAC.exit(0)

    ids = args[0].split(",")
    idList = []
    for transId in ids:
        r = transId.split(":")
        if len(r) > 1:
            for i in range(int(r[0]), int(r[1]) + 1):
                idList.append(i)
        else:
            idList.append(int(r[0]))

    for prod in idList:
        res = transClient.getTransformation(prod, extraParams=True)
        if not res["OK"]:
            gLogger.error("Error getting transformation", f"{prod}: {res['Message']}")
            continue
        res = transClient.moveFilesToDerivedTransformation(res["Value"], resetUnused)
        if not res["OK"]:
            gLogger.error("Error updating a derived transformation", f"{prod}: {res['Message']}")
            continue
        parentProd, movedFiles = res["Value"]
        if movedFiles:
            gLogger.info("Successfully moved files", f"from {parentProd} to {prod}")
            for status, val in movedFiles.items():
                gLogger.info(f"\t{val} files to status {status}")


if __name__ == "__main__":
    main()
