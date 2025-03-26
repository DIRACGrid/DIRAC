#!/usr/bin/env python
########################################################################
# File :   dirac-admin-voms-sync
# Author : Andrei Tsaregorodtsev
########################################################################
"""
Synchronize VOMS user data with the DIRAC Registry
"""
from DIRAC import gLogger, exit as DIRACExit, S_OK
from DIRAC.Core.Base.Script import Script
from DIRAC.ConfigurationSystem.Client.VOMS2CSSynchronizer import VOMS2CSSynchronizer
from DIRAC.Core.Utilities.Proxy import executeWithUserProxy
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOOption
from DIRAC.FrameworkSystem.Client.TokenManagerClient import gTokenManager

dryRun = False
voName = None
compareWithIAM = False
useIAM = False


def setDryRun(value):
    global dryRun
    dryRun = True
    return S_OK()


def setVO(value):
    global voName
    voName = value
    return S_OK()


def setCompareWithIAM(value):
    global compareWithIAM
    compareWithIAM = True
    return S_OK()


def setUseIAM(value):
    global useIAM
    useIAM = True
    return S_OK()


@Script()
def main():
    Script.registerSwitch("V:", "vo=", "VO name", setVO)
    Script.registerSwitch("D", "dryRun", "Dry run", setDryRun)
    Script.registerSwitch("C", "compareWithIAM", "Compare user list with IAM", setCompareWithIAM)
    Script.registerSwitch("I", "useIAM", "Use IAM as authoritative source", setUseIAM)

    Script.parseCommandLine(ignoreErrors=True)

    @executeWithUserProxy
    def syncCSWithVOMS(vomsSync):
        return vomsSync.syncCSWithVOMS()

    voAdminUser = getVOOption(voName, "VOAdmin")
    voAdminGroup = getVOOption(voName, "VOAdminGroup", getVOOption(voName, "DefaultGroup"))
    accessToken = None
    if compareWithIAM or useIAM:
        res = gTokenManager.getToken(
            userGroup=voAdminGroup,
            requiredTimeLeft=3600,
            scope=["scim:read"],
        )
        if not res["OK"]:
            return res

        accessToken = res["Value"]["access_token"]

    vomsSync = VOMS2CSSynchronizer(voName, compareWithIAM=compareWithIAM, useIAM=useIAM, accessToken=accessToken)
    result = syncCSWithVOMS(  # pylint: disable=unexpected-keyword-arg
        vomsSync, proxyUserName=voAdminUser, proxyUserGroup=voAdminGroup
    )
    if not result["OK"]:
        gLogger.error("Failed to synchronize user data")
        DIRACExit(-1)

    resultDict = result["Value"]
    newUsers = resultDict.get("NewUsers", [])
    modUsers = resultDict.get("ModifiedUsers", [])
    delUsers = resultDict.get("DeletedUsers", [])
    susUsers = resultDict.get("SuspendedUsers", [])
    gLogger.notice(
        "\nUser results: new %d, modified %d, deleted %d, new/suspended %d"
        % (len(newUsers), len(modUsers), len(delUsers), len(susUsers))
    )

    for msg in resultDict["AdminMessages"]["Info"]:
        gLogger.notice(msg)

    csapi = resultDict.get("CSAPI")
    if csapi and csapi.csModified:
        if dryRun:
            gLogger.notice("There are changes to Registry ready to commit, skipped because of dry run")
            csapi.showDiff()
        else:
            yn = input("There are changes to Registry ready to commit, do you want to proceed ? [Y|n]:")
            if yn == "" or yn[0].lower() == "y":
                result = csapi.commitChanges()
                if not result["OK"]:
                    gLogger.error("Could not commit configuration changes", result["Message"])
                else:
                    gLogger.notice(f"Registry changes committed for VO {voName}")
            else:
                gLogger.notice("Registry changes are not committed")
    else:
        gLogger.notice(f"No changes to Registry for VO {voName}")

    result = vomsSync.getVOUserReport()
    if not result["OK"]:
        gLogger.error("Failed to generate user data report")
        DIRACExit(-1)

    gLogger.notice("\n" + result["Value"])


if __name__ == "__main__":
    main()
