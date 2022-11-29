#!/usr/bin/env python
"""
Script that helps setting the token of the elements in RSS.
It can acquire or release the token.

If the releaseToken switch is used, no matter what was the previous token, it will be set to rs_svc (RSS owns it).
If not set, the token will be set to whatever username is defined on the proxy loaded while issuing
this command. In the second case, the token lasts one day.
"""
from datetime import datetime, timedelta

# DIRAC
from DIRAC import S_OK
from DIRAC import exit as DIRACExit
from DIRAC import gLogger
from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

switchDict = {}


def registerSwitches():
    """
    Registers all switches that can be used while calling the script from the
    command line interface.
    """

    switches = (
        ("element=", "Element family to be Synchronized ( Site, Resource or Node )"),
        ("name=", "Name, name of the element where the change applies"),
        ("statusType=", "StatusType, if none applies to all possible statusTypes"),
        ("reason=", "Reason to set the Status"),
        ("days=", "Number of days the token is acquired"),
        ("releaseToken", "Release the token and let the RSS take control"),
        ("VO=", "VO to set a token for. When omitted token will be set for all VOs"),
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
        gLogger.error("Found the following positional args '%s', but we only accept switches" % args)
        gLogger.error("Please, check documentation below")
        Script.showHelp(exitCode=1)

    switches = dict(Script.getUnprocessedSwitches())
    switches.setdefault("statusType", None)
    switches.setdefault("VO", None)
    switches.setdefault("days", 1)
    if "releaseToken" in switches:
        switches["releaseToken"] = True
    else:
        switches["releaseToken"] = False

    for key in ("element", "name", "reason"):

        if key not in switches:
            gLogger.error("%s Switch missing" % key)
            gLogger.error("Please, check documentation above")
            Script.showHelp(exitCode=1)

    if not switches["element"] in ("Site", "Resource", "Node"):
        gLogger.error("Found %s as element switch" % switches["element"])
        gLogger.error("Please, check documentation above")
        Script.showHelp(exitCode=1)

    gLogger.debug("The switches used are:")
    map(gLogger.debug, switches.items())

    return switches


def proxyUser():
    """
    Read proxy to get username.
    """

    res = getProxyInfo()
    if not res["OK"]:
        return res

    return S_OK(res["Value"]["username"])


def setToken(user):
    """
    Function that gets the user token, sets the validity for it. Gets the elements
    in the database for a given name and statusType(s). Then updates the status
    of all them adding a reason and the token.
    """

    rssClient = ResourceStatusClient()

    # This is a little bit of a nonsense, and certainly needs to be improved.
    # To modify a list of elements, we have to do it one by one. However, the
    # modify method does not discover the StatusTypes ( which in this script is
    # an optional parameter ). So, we get them from the DB and iterate over them.
    elements = rssClient.selectStatusElement(
        switchDict["element"],
        "Status",
        name=switchDict["name"],
        statusType=switchDict["statusType"],
        vO=switchDict["VO"],
        meta={"columns": ["StatusType", "TokenOwner"]},
    )

    if not elements["OK"]:
        return elements
    elements = elements["Value"]

    # If there list is empty they do not exist on the DB !
    if not elements:
        gLogger.warn(
            "Nothing found for %s, %s, %s %s"
            % (switchDict["element"], switchDict["name"], switchDict["VO"], switchDict["statusType"])
        )
        return S_OK()

    # If we want to release the token
    if switchDict["releaseToken"]:
        tokenExpiration = datetime.max
        newTokenOwner = "rs_svc"
    else:
        tokenExpiration = datetime.utcnow().replace(microsecond=0) + timedelta(days=int(switchDict["days"]))
        newTokenOwner = user

    gLogger.always(f"New token: {newTokenOwner} --- until {tokenExpiration}")

    for statusType, tokenOwner in elements:

        # If a user different than the one issuing the command and RSS
        if tokenOwner != user and tokenOwner != "rs_svc":
            gLogger.info("{}({}) belongs to the user: {}".format(switchDict["name"], statusType, tokenOwner))

        # does the job
        result = rssClient.modifyStatusElement(
            switchDict["element"],
            "Status",
            name=switchDict["name"],
            statusType=statusType,
            reason=switchDict["reason"],
            tokenOwner=newTokenOwner,
            vO=switchDict["VO"],
            tokenExpiration=tokenExpiration,
        )
        if not result["OK"]:
            return result

        if tokenOwner == newTokenOwner:
            msg = "(extended)"
        elif newTokenOwner == "rs_svc":
            msg = "(released)"
        else:
            msg = "(aquired from %s)" % tokenOwner

        gLogger.info("name:{}, VO:{} statusType:{} {}".format(switchDict["name"], switchDict["VO"], statusType, msg))
    return S_OK()


@Script()
def main():
    """
    Main function of the script. Gets the username from the proxy loaded and sets
    the token taking into account that user and the switchDict parameters.
    """
    global switchDict

    # Script initialization
    registerSwitches()
    switchDict = parseSwitches()

    user = proxyUser()
    if not user["OK"]:
        gLogger.error(user["Message"])
        DIRACExit(1)
    user = user["Value"]

    res = setToken(user)
    if not res["OK"]:
        gLogger.error(res["Message"])
        DIRACExit(1)

    DIRACExit(0)


if __name__ == "__main__":
    main()
