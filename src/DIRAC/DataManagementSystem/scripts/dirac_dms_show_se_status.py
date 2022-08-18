#!/usr/bin/env python
"""
Get status of the available Storage Elements

Usage:
  dirac-dms-show-se-status [<options>]

Example:
  $ dirac-dms-show-se-status
  Storage Element               Read Status    Write Status
  DIRAC-USER                         Active          Active
  IN2P3-disk                         Active          Active
  IPSL-IPGP-disk                     Active          Active
  IRES-disk                        InActive        InActive
  M3PEC-disk                         Active          Active
  ProductionSandboxSE                Active          Active
"""
from DIRAC import S_OK, exit as DIRACexit
from DIRAC.Core.Base.Script import Script

vo = None


def setVO(arg):
    global vo
    vo = arg
    return S_OK()


allVOsFlag = False


def setAllVO(arg):
    global allVOsFlag
    allVOsFlag = True
    return S_OK()


noVOFlag = False


def setNoVO(arg):
    global noVOFlag, allVOsFlag
    noVOFlag = True
    allVOsFlag = False
    return S_OK()


@Script()
def main():
    global vo
    global noVOFlag
    global allVOsFlag

    Script.registerSwitch("V:", "vo=", "Virtual Organization", setVO)
    Script.registerSwitch("a", "all", "All Virtual Organizations flag", setAllVO)
    Script.registerSwitch("n", "noVO", "No Virtual Organizations assigned flag", setNoVO)

    Script.parseCommandLine()

    from DIRAC import gConfig, gLogger
    from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
    from DIRAC.Core.Utilities.PrettyPrint import printTable
    from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup

    storageCFGBase = "/Resources/StorageElements"

    res = gConfig.getSections(storageCFGBase, True)
    if not res["OK"]:
        gLogger.error("Failed to get storage element info")
        gLogger.error(res["Message"])
        DIRACexit(1)

    gLogger.info("{} {} {}".format("Storage Element".ljust(25), "Read Status".rjust(15), "Write Status".rjust(15)))

    seList = sorted(res["Value"])

    resourceStatus = ResourceStatus()

    res = resourceStatus.getElementStatus(seList, "StorageElement")
    if not res["OK"]:
        gLogger.error("Failed to get StorageElement status for %s" % str(seList))
        DIRACexit(1)

    fields = ["SE", "ReadAccess", "WriteAccess", "RemoveAccess", "CheckAccess"]
    records = []

    if vo is None and not allVOsFlag:
        result = getVOfromProxyGroup()
        if not result["OK"]:
            gLogger.error("Failed to determine the user VO")
            DIRACexit(1)
        vo = result["Value"]

    for se, statusDict in res["Value"].items():

        # Check if the SE is allowed for the user VO
        if not allVOsFlag:
            voList = gConfig.getValue("/Resources/StorageElements/%s/VO" % se, [])
            if noVOFlag and voList:
                continue
            if voList and vo not in voList:
                continue

        record = [se]
        for status in fields[1:]:
            value = statusDict.get(status, "Unknown")
            record.append(value)
        records.append(record)

    printTable(fields, records, numbering=False, sortField="SE")

    DIRACexit(0)


if __name__ == "__main__":
    main()
