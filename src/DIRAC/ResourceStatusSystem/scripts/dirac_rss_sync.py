#!/usr/bin/env python
"""
Script that synchronizes the resources described on the CS with the RSS.
By default, it sets their Status to `Unknown`, StatusType to `all` and
reason to `Synchronized`. However, it can copy over the status on the CS to
the RSS. Important: If the StatusType is not defined on the CS, it will set
it to Active !
"""
from DIRAC import S_OK
from DIRAC import exit as DIRACExit
from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script

switchDict = {}
DEFAULT_STATUS = ""


def registerSwitches():
    """
    Registers all switches that can be used while calling the script from the
    command line interface.
    """

    Script.registerSwitch(
        "", "init", "Initialize the element to the status in the CS ( applicable for StorageElements )"
    )
    Script.registerSwitch("", "element=", "Element family to be Synchronized ( Site, Resource or Node ) or `all`")
    Script.registerSwitch("", "defaultStatus=", "Default element status if not given in the CS")


def parseSwitches():
    """
    Parses the arguments passed by the user
    """

    Script.parseCommandLine(ignoreErrors=True)
    args = Script.getPositionalArgs()
    if args:
        gLogger.error(f"Found the following positional args '{args}', but we only accept switches")
        gLogger.error("Please, check documentation below")
        Script.showHelp(exitCode=1)

    switches = dict(Script.getUnprocessedSwitches())

    # Default values
    switches.setdefault("element", None)
    switches.setdefault("defaultStatus", "Active")
    if not switches["element"] in ("all", "Site", "Resource", "Node", None):
        gLogger.error(f"Found {switches['element']} as element switch")
        gLogger.error("Please, check documentation below")
        Script.showHelp(exitCode=1)

    gLogger.debug("The switches used are:")
    map(gLogger.debug, switches.items())

    return switches


def synchronize():
    """
    Given the element switch, adds rows to the <element>Status tables with Status
    `Unknown` and Reason `Synchronized`.
    """
    global DEFAULT_STATUS

    from DIRAC.ResourceStatusSystem.Utilities import Synchronizer

    synchronizer = Synchronizer.Synchronizer(defaultStatus=DEFAULT_STATUS)

    if switchDict["element"] in ("Site", "all"):
        gLogger.info("Synchronizing Sites")
        res = synchronizer._syncSites()
        if not res["OK"]:
            return res

    if switchDict["element"] in ("Resource", "all"):
        gLogger.info("Synchronizing Resource")
        res = synchronizer._syncResources()
        if not res["OK"]:
            return res

    if switchDict["element"] in ("Node", "all"):
        gLogger.info("Synchronizing Nodes")
        res = synchronizer._syncNodes()
        if not res["OK"]:
            return res

    return S_OK()


def initSites():
    """
    Initializes Sites statuses taking their values from the "SiteMask" table of "JobDB" database.
    """
    from DIRAC.ResourceStatusSystem.Client import ResourceStatusClient
    from DIRAC.WorkloadManagementSystem.Client.WMSAdministratorClient import WMSAdministratorClient

    rssClient = ResourceStatusClient.ResourceStatusClient()

    sites = WMSAdministratorClient().getAllSiteMaskStatus()

    if not sites["OK"]:
        gLogger.error(sites["Message"])
        DIRACExit(1)

    for site, elements in sites["Value"].items():
        result = rssClient.addOrModifyStatusElement(
            "Site",
            "Status",
            name=site,
            statusType="all",
            status=elements[0],
            elementType=site.split(".")[0],
            tokenOwner="rs_svc",
            reason="dirac-rss-sync",
        )
        if not result["OK"]:
            gLogger.error(result["Message"])
            DIRACExit(1)

    return S_OK()


def initSEs():
    """
    Initializes SEs statuses taking their values from the CS.
    """
    from DIRAC import gConfig
    from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
    from DIRAC.ResourceStatusSystem.Client import ResourceStatusClient
    from DIRAC.ResourceStatusSystem.PolicySystem import StateMachine
    from DIRAC.ResourceStatusSystem.Utilities import CSHelpers, RssConfiguration

    # WarmUp local copy
    CSHelpers.warmUp()

    gLogger.info("Initializing SEs")

    rssClient = ResourceStatusClient.ResourceStatusClient()

    statuses = StateMachine.RSSMachine(None).getStates()
    statusTypes = RssConfiguration.RssConfiguration().getConfigStatusType("StorageElement")
    reason = "dirac-rss-sync"

    gLogger.debug(statuses)
    gLogger.debug(statusTypes)

    for se in DMSHelpers().getStorageElements():
        gLogger.debug(se)

        opts = gConfig.getOptionsDict(f"/Resources/StorageElements/{se}")
        if not opts["OK"]:
            gLogger.warn(opts["Message"])
            continue
        opts = opts["Value"]

        gLogger.debug(opts)

        # We copy the list into a new object to remove items INSIDE the loop !
        statusTypesList = statusTypes[:]

        for statusType, status in opts.items():
            # Sanity check...
            if statusType not in statusTypesList:
                continue

            # Transforms statuses to RSS terms
            if status in ("NotAllowed", "InActive"):
                status = "Banned"

            if status not in statuses:
                gLogger.error(f"{status} not a valid status for {se} - {statusType}")
                continue

            # We remove from the backtracking
            statusTypesList.remove(statusType)

            gLogger.debug([se, statusType, status, reason])
            result = rssClient.addOrModifyStatusElement(
                "Resource",
                "Status",
                name=se,
                statusType=statusType,
                status=status,
                elementType="StorageElement",
                tokenOwner="rs_svc",
                reason=reason,
            )

            if not result["OK"]:
                gLogger.error("Failed to modify")
                gLogger.error(result["Message"])
                continue

        # Backtracking: statusTypes not present on CS
        for statusType in statusTypesList:
            result = rssClient.addOrModifyStatusElement(
                "Resource",
                "Status",
                name=se,
                statusType=statusType,
                status=DEFAULT_STATUS,
                elementType="StorageElement",
                tokenOwner="rs_svc",
                reason=reason,
            )
            if not result["OK"]:
                gLogger.error(f"Error in backtracking for {se},{statusType},{status}")
                gLogger.error(result["Message"])

    return S_OK()


def run():
    """
    Main function of the script
    """

    result = synchronize()
    if not result["OK"]:
        gLogger.error(result["Message"])
        DIRACExit(1)

    if "init" in switchDict:
        if switchDict.get("element") == "Site":
            result = initSites()
            if not result["OK"]:
                gLogger.error(result["Message"])
                DIRACExit(1)

        if switchDict.get("element") == "Resource":
            result = initSEs()
            if not result["OK"]:
                gLogger.error(result["Message"])
                DIRACExit(1)


@Script()
def main():
    global switchDict
    global DEFAULT_STATUS

    registerSwitches()
    switchDict = parseSwitches()
    DEFAULT_STATUS = switchDict.get("defaultStatus", "Active")

    # Run script
    run()

    # Bye
    DIRACExit(0)


if __name__ == "__main__":
    main()
