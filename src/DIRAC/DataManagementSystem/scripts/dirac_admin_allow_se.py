#!/usr/bin/env python
"""
Enable using one or more Storage Elements

Example:
  $ dirac-admin-allow-se M3PEC-disk
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    read = False
    write = False
    check = False
    remove = False
    site = ""
    mute = False
    userName = ""

    Script.registerSwitch("r", "AllowRead", "     Allow only reading from the storage element")
    Script.registerSwitch("w", "AllowWrite", "     Allow only writing to the storage element")
    Script.registerSwitch("k", "AllowCheck", "     Allow only check access to the storage element")
    Script.registerSwitch("v", "AllowRemove", "    Allow only remove access to the storage element")
    Script.registerSwitch("a", "All", "    Allow all access to the storage element")
    Script.registerSwitch("m", "Mute", "     Do not send email")
    Script.registerSwitch("S:", "Site=", "     Allow all SEs associated to site")
    Script.registerSwitch("t:", "tokenOwner=", "     Optional Name of the token owner")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["seGroupList: list of SEs or comma-separated SEs"])

    switches, ses = Script.parseCommandLine(ignoreErrors=True)

    for switch in switches:
        if switch[0].lower() in ("r", "allowread"):
            read = True
        if switch[0].lower() in ("w", "allowwrite"):
            write = True
        if switch[0].lower() in ("k", "allowcheck"):
            check = True
        if switch[0].lower() in ("v", "allowremove"):
            remove = True
        if switch[0].lower() in ("a", "all"):
            read = True
            write = True
            check = True
            remove = True
        if switch[0].lower() in ("m", "mute"):
            mute = True
        if switch[0].lower() in ("s", "site"):
            site = switch[1]
        if switch[0] in ("t", "tokenOwner"):
            userName = switch[1]

    # imports
    from DIRAC import gLogger
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
    from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo
    from DIRAC.DataManagementSystem.Utilities.DMSHelpers import resolveSEGroup
    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
    from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus

    if not (read or write or check or remove):
        # No switch was specified, means we need all of them
        gLogger.notice("No option given, all accesses will be allowed if they were not")
        read = True
        write = True
        check = True
        remove = True

    ses = resolveSEGroup(ses)
    diracAdmin = DiracAdmin()

    if not userName:
        res = getProxyInfo()
        if not res["OK"]:
            gLogger.error("Failed to get proxy information", res["Message"])
            DIRAC.exit(2)

        userName = res["Value"].get("username")
        if not userName:
            gLogger.error("Failed to get username for proxy")
            DIRAC.exit(2)

    if site:
        res = getSites()
        if not res["OK"]:
            gLogger.error(res["Message"])
            DIRAC.exit(-1)
        if site not in res["Value"]:
            gLogger.error(f"The provided site ({site}) is not known.")
            DIRAC.exit(-1)
        ses.extend(res["Value"]["SE"].replace(" ", "").split(","))
    if not ses:
        gLogger.error("There were no SEs provided")
        DIRAC.exit()

    STATUS_TYPES = ["ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"]
    ALLOWED_STATUSES = ["Unknown", "InActive", "Banned", "Probing", "Degraded", "Error"]

    statusAllowedDict = {}
    for statusType in STATUS_TYPES:
        statusAllowedDict[statusType] = []

    statusFlagDict = {}
    statusFlagDict["ReadAccess"] = read
    statusFlagDict["WriteAccess"] = write
    statusFlagDict["CheckAccess"] = check
    statusFlagDict["RemoveAccess"] = remove

    resourceStatus = ResourceStatus()

    res = resourceStatus.getElementStatus(ses, "StorageElement")
    if not res["OK"]:
        gLogger.error(f"Storage Element {ses} does not exist")
        DIRAC.exit(-1)

    reason = f"Forced with dirac-admin-allow-se by {userName}"

    for se, seOptions in res["Value"].items():
        # InActive is used on the CS model, Banned is the equivalent in RSS
        for statusType in STATUS_TYPES:
            if statusFlagDict[statusType]:
                if seOptions.get(statusType) == "Active":
                    gLogger.notice(f"{statusType} status of {se} is already Active")
                    continue
                if statusType in seOptions:
                    if not seOptions[statusType] in ALLOWED_STATUSES:
                        gLogger.notice(
                            "%s option for %s is %s, instead of %s"
                            % (statusType, se, seOptions["ReadAccess"], ALLOWED_STATUSES)
                        )
                        gLogger.notice("Try specifying the command switches")
                    else:
                        resR = resourceStatus.setElementStatus(
                            se, "StorageElement", statusType, "Active", reason, userName
                        )
                        if not resR["OK"]:
                            gLogger.fatal(f"Failed to update {se} {statusType} to Active, exit -", resR["Message"])
                            DIRAC.exit(-1)
                        else:
                            gLogger.notice(f"Successfully updated {se} {statusType} to Active")
                            statusAllowedDict[statusType].append(se)

    totalAllowed = 0
    totalAllowedSEs = []
    for statusType in STATUS_TYPES:
        totalAllowed += len(statusAllowedDict[statusType])
        totalAllowedSEs += statusAllowedDict[statusType]
    totalAllowedSEs = list(set(totalAllowedSEs))

    if not totalAllowed:
        gLogger.info("No storage elements were allowed")
        DIRAC.exit(-1)

    if mute:
        gLogger.notice("Email is muted by script switch")
        DIRAC.exit(0)

    subject = f"{len(totalAllowedSEs)} storage elements allowed for use"
    addressPath = "EMail/Production"
    address = Operations().getValue(addressPath, "")
    fromAddress = Operations().getValue("ResourceStatus/Config/FromAddress", "")

    body = ""
    if read:
        body = f"{body}\n\nThe following storage elements were allowed for reading:"
        for se in statusAllowedDict["ReadAccess"]:
            body = f"{body}\n{se}"
    if write:
        body = f"{body}\n\nThe following storage elements were allowed for writing:"
        for se in statusAllowedDict["WriteAccess"]:
            body = f"{body}\n{se}"
    if check:
        body = f"{body}\n\nThe following storage elements were allowed for checking:"
        for se in statusAllowedDict["CheckAccess"]:
            body = f"{body}\n{se}"
    if remove:
        body = f"{body}\n\nThe following storage elements were allowed for removing:"
        for se in statusAllowedDict["RemoveAccess"]:
            body = f"{body}\n{se}"

    if not address:
        gLogger.notice(f"'{addressPath}' not defined in Operations, can not send Mail\n", body)
        DIRAC.exit(0)

    res = diracAdmin.sendMail(address, subject, body, fromAddress=fromAddress)
    gLogger.notice(f"Notifying {address}")
    if res["OK"]:
        gLogger.notice(res["Value"])
    else:
        gLogger.notice(res["Message"])

    DIRAC.exit(0)


if __name__ == "__main__":
    main()
