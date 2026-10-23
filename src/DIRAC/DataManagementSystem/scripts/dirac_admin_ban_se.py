#!/usr/bin/env python

"""
Ban one or more Storage Elements for usage

Example:
  $ dirac-admin-ban-se M3PEC-disk
"""
import DIRAC
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    read = True
    write = True
    check = True
    remove = True
    sites = []
    mute = False
    userName = ""

    Script.registerSwitch("r", "BanRead", "     Ban only reading from the storage element")
    Script.registerSwitch("w", "BanWrite", "     Ban writing to the storage element")
    Script.registerSwitch("k", "BanCheck", "     Ban check access to the storage element")
    Script.registerSwitch("v", "BanRemove", "    Ban remove access to the storage element")
    Script.registerSwitch("a", "All", "    Ban all access to the storage element")
    Script.registerSwitch("m", "Mute", "     Do not send email")
    Script.registerSwitch(
        "S:", "Site=", "     Ban all SEs associate to site (note that if writing is allowed, check is always allowed)"
    )
    Script.registerSwitch("t:", "tokenOwner=", "     Optional Name of the token owner")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(["seGroupList: list of SEs or comma-separated SEs"])

    switches, ses = Script.parseCommandLine(ignoreErrors=True)

    for switch in switches:
        if switch[0].lower() in ("r", "banread"):
            write = False
            check = False
            remove = False
        if switch[0].lower() in ("w", "banwrite"):
            read = False
            check = False
            remove = False
        if switch[0].lower() in ("k", "bancheck"):
            read = False
            write = False
            remove = False
        if switch[0].lower() in ("v", "banremove"):
            read = False
            write = False
            check = False
        if switch[0].lower() in ("a", "all"):
            pass
        if switch[0].lower() in ("m", "mute"):
            mute = True
        if switch[0].lower() in ("s", "site"):
            sites = switch[1].split(",")
        if switch[0] in ("t", "tokenOwner"):
            userName = switch[1]

    # from DIRAC.ConfigurationSystem.Client.CSAPI           import CSAPI
    from DIRAC import gLogger
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo
    from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers, resolveSEGroup
    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
    from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
    from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

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

    for site in sites:
        res = DMSHelpers().getSEsForSite(site)
        if not res["OK"]:
            gLogger.error(res["Message"], site)
            DIRAC.exit(-1)
        ses.extend(res["Value"])

    if not ses:
        gLogger.error("There were no SEs provided")
        DIRAC.exit(-1)

    STATUS_TYPES = ["ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"]

    statusBannedDict = {}
    for statusType in STATUS_TYPES:
        statusBannedDict[statusType] = []

    statusFlagDict = {}
    statusFlagDict["ReadAccess"] = read
    statusFlagDict["WriteAccess"] = write
    statusFlagDict["CheckAccess"] = check
    statusFlagDict["RemoveAccess"] = remove

    resourceStatus = ResourceStatus()

    resDB = ResourceStatusClient().selectStatusElement(
        "Resource", "Status", ses, elementType="StorageElement", meta={"columns": ["Name", "StatusType", "Status"]}
    )
    if not resDB["OK"]:
        gLogger.error("Failed to get the status of the storage elements", resDB["Message"])
        DIRAC.exit(-1)
    dbStatus = {}
    for name, statusType, status in resDB["Value"]:
        dbStatus.setdefault(name, {})[statusType] = status

    reason = f"Forced with dirac-admin-ban-se by {userName}"

    for se, seOptions in dbStatus.items():
        for statusType in (s for s in STATUS_TYPES if statusFlagDict[s]):
            if seOptions.get(statusType) == "Banned":
                gLogger.notice(f"{statusType} status of {se} is already Banned")
                continue
            if statusType in seOptions:
                resR = resourceStatus.setElementStatus(se, "StorageElement", statusType, "Banned", reason, userName)
                if not resR["OK"]:
                    gLogger.fatal(f"Failed to update {se} {statusType} to Banned, exit -", resR["Message"])
                    DIRAC.exit(-1)
                else:
                    gLogger.notice(f"Successfully updated {se} {statusType} to Banned")
                    statusBannedDict[statusType].append(se)

    totalBanned = 0
    totalBannedSEs = []
    for statusType in STATUS_TYPES:
        totalBanned += len(statusBannedDict[statusType])
        totalBannedSEs += statusBannedDict[statusType]
    totalBannedSEs = list(set(totalBannedSEs))

    if not totalBanned:
        gLogger.info("No storage elements were Banned")
        DIRAC.exit(-1)

    if mute:
        gLogger.notice("Email is muted by script switch")
        DIRAC.exit(0)

    subject = f"{len(totalBannedSEs)} storage elements banned for use"
    addressPath = "EMail/Production"
    address = Operations().getValue(addressPath, "")
    fromAddress = Operations().getValue("ResourceStatus/Config/FromAddress", "")

    body = ""
    if read:
        body = f"{body}\n\nThe following storage elements were banned for reading:"
        for se in statusBannedDict["ReadAccess"]:
            body = f"{body}\n{se}"
    if write:
        body = f"{body}\n\nThe following storage elements were banned for writing:"
        for se in statusBannedDict["WriteAccess"]:
            body = f"{body}\n{se}"
    if check:
        body = f"{body}\n\nThe following storage elements were banned for check access:"
        for se in statusBannedDict["CheckAccess"]:
            body = f"{body}\n{se}"
    if remove:
        body = f"{body}\n\nThe following storage elements were banned for remove access:"
        for se in statusBannedDict["RemoveAccess"]:
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
