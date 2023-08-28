#!/usr/bin/env python
"""
Script that dumps the DB information for the elements into the standard output.
If returns information concerning the StatusType and Status attributes.
"""
from DIRAC import exit as DIRACExit
from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.ResourceStatusSystem.Client import ResourceStatusClient

switchDict = {}


def registerSwitches():
    """
    Registers all switches that can be used while calling the script from the
    command line interface.
    """

    switches = (
        ("element=", "Element family to be Synchronized ( Site, Resource or Node )"),
        ("elementType=", "ElementType narrows the search; None if default"),
        ("name=", "ElementName; None if default"),
        ("tokenOwner=", "Owner of the token; None if default"),
        ("statusType=", "StatusType; None if default"),
        ("status=", "Status; None if default"),
        ("VO=", "Virtual organisation; None if default"),
    )

    for switch in switches:
        Script.registerSwitch("", switch[0], switch[1])


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
    switches.setdefault("elementType", None)
    switches.setdefault("name", None)
    switches.setdefault("tokenOwner", None)
    switches.setdefault("statusType", None)
    switches.setdefault("status", None)
    switches.setdefault("VO", None)

    if "element" not in switches:
        gLogger.error("element Switch missing")
        gLogger.error("Please, check documentation below")
        Script.showHelp(exitCode=1)

    if not switches["element"] in ("Site", "Resource", "Node"):
        gLogger.error(f"Found {switches['element']} as element switch")
        gLogger.error("Please, check documentation below")
        Script.showHelp(exitCode=1)

    gLogger.debug("The switches used are:")
    map(gLogger.debug, switches.items())

    return switches


def getElements():
    """
    Given the switches, gets a list of elements with their respective statustype
    and status attributes.
    """

    rssClient = ResourceStatusClient.ResourceStatusClient()

    meta = {"columns": []}
    for key in ("Name", "StatusType", "Status", "ElementType", "TokenOwner"):
        # Transforms from upper lower case to lower upper case
        if switchDict[key[0].lower() + key[1:]] is None:
            meta["columns"].append(key)

    elements = rssClient.selectStatusElement(
        switchDict["element"],
        "Status",
        name=switchDict["name"].split(",") if switchDict["name"] else None,
        statusType=switchDict["statusType"].split(",") if switchDict["statusType"] else None,
        status=switchDict["status"].split(",") if switchDict["status"] else None,
        elementType=switchDict["elementType"].split(",") if switchDict["elementType"] else None,
        tokenOwner=switchDict["tokenOwner"].split(",") if switchDict["tokenOwner"] else None,
        meta=meta,
    )

    return elements


def tabularPrint(elementsList):
    """
    Prints the list of elements on a tabular
    """

    gLogger.notice("")
    gLogger.notice("Selection parameters:")
    gLogger.notice(f"  {'element'.ljust(15)}: {switchDict['element']}")
    titles = []
    for key in ("Name", "StatusType", "Status", "ElementType", "TokenOwner"):
        # Transforms from upper lower case to lower upper case
        keyT = key[0].lower() + key[1:]

        if switchDict[keyT] is None:
            titles.append(key)
        else:
            gLogger.notice(f"  {key.ljust(15)}: {switchDict[keyT]}")
    gLogger.notice("")

    gLogger.notice(printTable(titles, elementsList, printOut=False, numbering=False, columnSeparator=" | "))


def run():
    """
    Main function of the script
    """

    elements = getElements()
    if not elements["OK"]:
        gLogger.error(elements)
        DIRACExit(1)
    elements = elements["Value"]

    tabularPrint(elements)


@Script()
def main():
    global switchDict

    # Script initialization
    registerSwitches()
    switchDict = parseSwitches()

    # Run script
    run()

    # Bye
    DIRACExit(0)


if __name__ == "__main__":
    main()
