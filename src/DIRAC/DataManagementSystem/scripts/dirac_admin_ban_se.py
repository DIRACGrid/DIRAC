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

    readBanned = []
    writeBanned = []
    checkBanned = []
    removeBanned = []

    resourceStatus = ResourceStatus()

    res = resourceStatus.getElementStatus(ses, "StorageElement")
    if not res["OK"]:
        gLogger.error(f"Storage Element {ses} does not exist")
        DIRAC.exit(-1)

    reason = f"Forced with dirac-admin-ban-se by {userName}"

    for se, seOptions in res["Value"].items():
        resW = resC = resR = {"OK": False}

        # Eventually, we will get rid of the notion of InActive, as we always write Banned.
        if read and "ReadAccess" in seOptions:
            if seOptions["ReadAccess"] == "Banned":
                gLogger.notice("Read access already banned", se)
                resR["OK"] = True
            elif not seOptions["ReadAccess"] in ["Active", "Degraded", "Probing", "Error"]:
                gLogger.notice(
                    "Read option for %s is %s, instead of %s"
                    % (se, seOptions["ReadAccess"], ["Active", "Degraded", "Probing", "Error"])
                )
                gLogger.notice("Try specifying the command switches")
            else:
                resR = resourceStatus.setElementStatus(se, "StorageElement", "ReadAccess", "Banned", reason, userName)
                # res = csAPI.setOption( "%s/%s/ReadAccess" % ( storageCFGBase, se ), "InActive" )
                if not resR["OK"]:
                    gLogger.error(f"Failed to update {se} read access to Banned")
                else:
                    gLogger.notice(f"Successfully updated {se} read access to Banned")
                    readBanned.append(se)

        # Eventually, we will get rid of the notion of InActive, as we always write Banned.
        if write and "WriteAccess" in seOptions:
            if seOptions["WriteAccess"] == "Banned":
                gLogger.notice("Write access already banned", se)
                resW["OK"] = True
            elif not seOptions["WriteAccess"] in ["Active", "Degraded", "Probing"]:
                gLogger.notice(
                    "Write option for %s is %s, instead of %s"
                    % (se, seOptions["WriteAccess"], ["Active", "Degraded", "Probing"])
                )
                gLogger.notice("Try specifying the command switches")
            else:
                resW = resourceStatus.setElementStatus(se, "StorageElement", "WriteAccess", "Banned", reason, userName)
                # res = csAPI.setOption( "%s/%s/WriteAccess" % ( storageCFGBase, se ), "InActive" )
                if not resW["OK"]:
                    gLogger.error(f"Failed to update {se} write access to Banned")
                else:
                    gLogger.notice(f"Successfully updated {se} write access to Banned")
                    writeBanned.append(se)

        # Eventually, we will get rid of the notion of InActive, as we always write Banned.
        if check and "CheckAccess" in seOptions:
            if seOptions["CheckAccess"] == "Banned":
                gLogger.notice("Check access already banned", se)
                resC["OK"] = True
            elif not seOptions["CheckAccess"] in ["Active", "Degraded", "Probing"]:
                gLogger.notice(
                    "Check option for %s is %s, instead of %s"
                    % (se, seOptions["CheckAccess"], ["Active", "Degraded", "Probing"])
                )
                gLogger.notice("Try specifying the command switches")
            else:
                resC = resourceStatus.setElementStatus(se, "StorageElement", "CheckAccess", "Banned", reason, userName)
                # res = csAPI.setOption( "%s/%s/CheckAccess" % ( storageCFGBase, se ), "InActive" )
                if not resC["OK"]:
                    gLogger.error(f"Failed to update {se} check access to Banned")
                else:
                    gLogger.notice(f"Successfully updated {se} check access to Banned")
                    checkBanned.append(se)

        # Eventually, we will get rid of the notion of InActive, as we always write Banned.
        if remove and "RemoveAccess" in seOptions:
            if seOptions["RemoveAccess"] == "Banned":
                gLogger.notice("Remove access already banned", se)
                resC["OK"] = True
            elif not seOptions["RemoveAccess"] in ["Active", "Degraded", "Probing"]:
                gLogger.notice(
                    "Remove option for %s is %s, instead of %s"
                    % (se, seOptions["RemoveAccess"], ["Active", "Degraded", "Probing"])
                )
                gLogger.notice("Try specifying the command switches")
            else:
                resC = resourceStatus.setElementStatus(se, "StorageElement", "RemoveAccess", "Banned", reason, userName)
                # res = csAPI.setOption( "%s/%s/CheckAccess" % ( storageCFGBase, se ), "InActive" )
                if not resC["OK"]:
                    gLogger.error(f"Failed to update {se} remove access to Banned")
                else:
                    gLogger.notice(f"Successfully updated {se} remove access to Banned")
                    removeBanned.append(se)

        if not (resR["OK"] or resW["OK"] or resC["OK"]):
            DIRAC.exit(-1)

    if not (writeBanned or readBanned or checkBanned or removeBanned):
        gLogger.notice("No storage elements were banned")
        DIRAC.exit(-1)

    if mute:
        gLogger.notice("Email is muted by script switch")
        DIRAC.exit(0)

    subject = f"{len(writeBanned + readBanned + checkBanned + removeBanned)} storage elements banned for use"
    addressPath = "EMail/Production"
    address = Operations().getValue(addressPath, "")
    fromAddress = Operations().getValue("ResourceStatus/Config/FromAddress", "")

    body = ""
    if read:
        body = f"{body}\n\nThe following storage elements were banned for reading:"
        for se in readBanned:
            body = f"{body}\n{se}"
    if write:
        body = f"{body}\n\nThe following storage elements were banned for writing:"
        for se in writeBanned:
            body = f"{body}\n{se}"
    if check:
        body = f"{body}\n\nThe following storage elements were banned for check access:"
        for se in checkBanned:
            body = f"{body}\n{se}"
    if remove:
        body = f"{body}\n\nThe following storage elements were banned for remove access:"
        for se in removeBanned:
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
