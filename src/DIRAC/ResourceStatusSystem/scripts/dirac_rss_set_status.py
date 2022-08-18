#!/usr/bin/env python
"""
Script that facilitates the modification of a element through the command line.
However, the usage of this script will set the element token to the command
issuer with a duration of 1 day.
"""
from datetime import datetime, timedelta

from DIRAC import gLogger, exit as DIRACExit, S_OK, version
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ResourceStatusSystem.Client import ResourceStatusClient
from DIRAC.ResourceStatusSystem.PolicySystem import StateMachine
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations


subLogger = None


def registerSwitches():
    """
    Registers all switches that can be used while calling the script from the
    command line interface.
    """

    switches = (
        ("element=", "Element family to be Synchronized ( Site, Resource or Node )"),
        ("name=", "Name (or comma-separeted list of names) of the element where the change applies"),
        ("statusType=", "StatusType (or comma-separeted list of names), if none applies to all possible statusTypes"),
        ("status=", "Status to be changed"),
        ("reason=", "Reason to set the Status"),
        ("VO=", "VO to change a status for. When omitted, status will be changed for all VOs"),
    )

    for switch in switches:
        Script.registerSwitch("", switch[0], switch[1])


def registerUsageMessage():
    """
    Takes the script __doc__ and adds the DIRAC version to it
    """
    usageMessage = "  DIRAC %s\n" % version
    usageMessage += __doc__

    Script.setUsageMessage(usageMessage)


def parseSwitches():
    """
    Parses the arguments passed by the user
    """

    Script.parseCommandLine(ignoreErrors=True)
    args = Script.getPositionalArgs()
    if args:
        subLogger.error("Found the following positional args '%s', but we only accept switches" % args)
        subLogger.error("Please, check documentation below")
        Script.showHelp(exitCode=1)

    switches = dict(Script.getUnprocessedSwitches())
    switches.setdefault("statusType", None)
    switches.setdefault("VO", None)

    for key in ("element", "name", "status", "reason"):

        if key not in switches:
            subLogger.error("%s Switch missing" % key)
            subLogger.error("Please, check documentation below")
            Script.showHelp(exitCode=1)

    if not switches["element"] in ("Site", "Resource", "Node"):
        subLogger.error("Found %s as element switch" % switches["element"])
        subLogger.error("Please, check documentation below")
        Script.showHelp(exitCode=1)

    statuses = StateMachine.RSSMachine(None).getStates()

    if not switches["status"] in statuses:
        subLogger.error("Found %s as element switch" % switches["element"])
        subLogger.error("Please, check documentation below")
        Script.showHelp(exitCode=1)

    subLogger.debug("The switches used are:")
    map(subLogger.debug, switches.items())

    return switches


def checkStatusTypes(statusTypes):
    """
    To check if values for 'statusType' are valid
    """

    opsH = Operations().getValue("ResourceStatus/Config/StatusTypes/StorageElement")
    acceptableStatusTypes = opsH.replace(",", "").split()

    for statusType in statusTypes:
        if statusType not in acceptableStatusTypes and statusType != "all":
            acceptableStatusTypes.append("all")
            subLogger.error(
                "'%s' is a wrong value for switch 'statusType'.\n\tThe acceptable values are:\n\t%s"
                % (statusType, str(acceptableStatusTypes))
            )

    if "all" in statusType:
        return acceptableStatusTypes
    return statusTypes


def unpack(switchDict):
    """
    To split and process comma-separated list of values for 'name' and 'statusType'
    """

    switchDictSet = []
    names = []
    statusTypes = []

    if switchDict["name"] is not None:
        names = list(filter(None, switchDict["name"].split(",")))

    if switchDict["statusType"] is not None:
        statusTypes = list(filter(None, switchDict["statusType"].split(",")))
        statusTypes = checkStatusTypes(statusTypes)

    if len(names) > 0 and len(statusTypes) > 0:
        combinations = [(a, b) for a in names for b in statusTypes]
        for combination in combinations:
            n, s = combination
            switchDictClone = switchDict.copy()
            switchDictClone["name"] = n
            switchDictClone["statusType"] = s
            switchDictSet.append(switchDictClone)
    elif len(names) > 0 and len(statusTypes) == 0:
        for name in names:
            switchDictClone = switchDict.copy()
            switchDictClone["name"] = name
            switchDictSet.append(switchDictClone)
    elif len(names) == 0 and len(statusTypes) > 0:
        for statusType in statusTypes:
            switchDictClone = switchDict.copy()
            switchDictClone["statusType"] = statusType
            switchDictSet.append(switchDictClone)
    elif len(names) == 0 and len(statusTypes) == 0:
        switchDictClone = switchDict.copy()
        switchDictClone["name"] = None
        switchDictClone["statusType"] = None
        switchDictSet.append(switchDictClone)

    return switchDictSet


def getTokenOwner():
    """
    Function that gets the userName from the proxy
    """
    proxyInfo = getProxyInfo()
    if not proxyInfo["OK"]:
        return proxyInfo

    userName = proxyInfo["Value"]["username"]
    return S_OK(userName)


def setStatus(switchDict, tokenOwner):
    """
    Function that gets the user token, sets the validity for it. Gets the elements
    in the database for a given name and statusType(s). Then updates the status
    of all them adding a reason and the token.
    """

    rssClient = ResourceStatusClient.ResourceStatusClient()

    elements = rssClient.selectStatusElement(
        switchDict["element"],
        "Status",
        name=switchDict["name"],
        statusType=switchDict["statusType"],
        vO=switchDict["VO"],
        meta={"columns": ["Status", "StatusType"]},
    )

    if not elements["OK"]:
        return elements
    elements = elements["Value"]

    if not elements:
        subLogger.warn(
            "Nothing found for %s, %s, %s %s"
            % (switchDict["element"], switchDict["name"], switchDict["VO"], switchDict["statusType"])
        )
        return S_OK()

    tomorrow = datetime.utcnow().replace(microsecond=0) + timedelta(days=1)

    for status, statusType in elements:

        subLogger.debug(f"{status} {statusType}")

        if switchDict["status"] == status:
            subLogger.notice(
                "Status for {} ({}) is already {}. Ignoring..".format(switchDict["name"], statusType, status)
            )
            continue

        subLogger.debug(
            "About to set status %s -> %s for %s, statusType: %s, VO: %s, reason: %s"
            % (status, switchDict["status"], switchDict["name"], statusType, switchDict["VO"], switchDict["reason"])
        )
        result = rssClient.modifyStatusElement(
            switchDict["element"],
            "Status",
            name=switchDict["name"],
            statusType=statusType,
            status=switchDict["status"],
            reason=switchDict["reason"],
            vO=switchDict["VO"],
            tokenOwner=tokenOwner,
            tokenExpiration=tomorrow,
        )
        if not result["OK"]:
            return result

    return S_OK()


def run(switchDict):
    """
    Main function of the script
    """

    tokenOwner = getTokenOwner()
    if not tokenOwner["OK"]:
        subLogger.error(tokenOwner["Message"])
        DIRACExit(1)
    tokenOwner = tokenOwner["Value"]

    subLogger.notice("TokenOwner is %s" % tokenOwner)

    result = setStatus(switchDict, tokenOwner)
    if not result["OK"]:
        subLogger.error(result["Message"])
        DIRACExit(1)


@Script()
def main():
    global subLogger
    global registerUsageMessage

    subLogger = gLogger.getSubLogger(__file__)

    # Script initialization
    registerSwitches()
    registerUsageMessage()
    switchDict = parseSwitches()
    switchDictSets = unpack(switchDict)

    # Run script
    for switchDict in switchDictSets:
        run(switchDict)

    # Bye
    DIRACExit(0)


if __name__ == "__main__":
    main()
