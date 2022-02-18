#!/usr/bin/env python
"""
  Get VM instances available in the configured cloud sites
"""
from DIRAC import gLogger, exit as DIRACExit
from DIRAC.Core.Base.Script import Script

site = None
ce = None
image = None
voName = None


def setCE(args):
    global ce
    ce = args


def setSite(args):
    global Site
    Site = args


def setImage(args):
    global image
    image = args


def setVO(args):
    global vo
    vo = args


@Script()
def main():
    Script.registerSwitch("S:", "Site=", "Site Name", setSite)
    Script.registerSwitch("C:", "CE=", "Cloud Endpoint Name ", setCE)
    Script.registerSwitch("I:", "Image=", "Image Name", setImage)
    Script.registerSwitch("v:", "vo=", "VO name", setVO)
    Script.parseCommandLine(ignoreErrors=True)
    args = Script.getExtraCLICFGFiles()

    from DIRAC.WorkloadManagementSystem.Client.VMClient import VMClient
    from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
    from DIRAC.Core.Utilities.PrettyPrint import printTable

    siteList = None
    if site is not None:
        siteList = [s.strip() for s in site.split(",")]

    ceList = None
    if ce is not None:
        ceList = [c.strip() for c in ce.split(",")]

    voName = vo
    if voName is None:
        result = getVOfromProxyGroup()
        if result["OK"]:
            voName = result["Value"]

    records = []
    vmClient = VMClient()

    result = vmClient.getCEInstances(siteList, ceList, voName)
    if not result["OK"]:
        gLogger.error(result["Message"])
        DIRACExit(-1)

    for nodeID in result["Value"]:
        nodeDict = result["Value"][nodeID]
        record = [
            nodeDict["Site"],
            nodeDict["CEName"],
            nodeID,
            nodeDict["NodeName"],
            nodeDict["PublicIP"],
            nodeDict["State"],
        ]
        records.append(record)

    fields = ["Site", "Endpoint", "ID", "Name", "PublicIP", "State"]
    printTable(fields, records)
    DIRACExit(0)


if __name__ == "__main__":
    main()
