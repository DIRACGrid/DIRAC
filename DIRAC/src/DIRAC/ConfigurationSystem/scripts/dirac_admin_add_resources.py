#!/usr/bin/env python
########################################################################
# File :   dirac_admin_add_resources
# Author : Andrei Tsaregorodtsev
########################################################################
"""
Add resources from the BDII database for a given VO
"""
import signal
import shlex

from DIRAC.Core.Base.Script import Script
from DIRAC import gLogger, exit as DIRACExit
from DIRAC.ConfigurationSystem.Client.Utilities import getGridCEs, getSiteUpdates
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueues, getDIRACSiteName
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOOption


def processScriptSwitches():
    global vo, dry, doCEs, hostURL, onecore

    Script.registerSwitch("V:", "vo=", "Virtual Organization")
    Script.registerSwitch("D", "dry", "Dry run")
    Script.registerSwitch("C", "ce", "Process Computing Elements")
    Script.registerSwitch("H:", "host=", "use this url for information querying")
    Script.registerSwitch(
        "", "onecore", "Add Single Core Queues for each MultiCore Queue, set RequiredTag for those Queues"
    )
    Script.parseCommandLine(ignoreErrors=True)

    vo = ""
    dry = False
    doCEs = False
    hostURL = None
    onecore = False

    for sw in Script.getUnprocessedSwitches():
        if sw[0] in ("V", "vo"):
            vo = sw[1]
        if sw[0] in ("D", "dry"):
            dry = True
        if sw[0] in ("C", "ce"):
            doCEs = True
        if sw[0] in ("H", "host"):
            hostURL = sw[1]
        if sw[0] in ("onecore",):
            onecore = True


ceBdiiDict = None


def checkUnusedCEs():
    global vo, dry, ceBdiiDict, hostURL

    gLogger.notice("looking for new computing resources in the BDII database...")

    res = getQueues(community=vo)
    if not res["OK"]:
        gLogger.error("ERROR: failed to get CEs from CS", res["Message"])
        DIRACExit(-1)

    knownCEs = set()
    for _site, ces in res["Value"].items():
        knownCEs.update(ces)

    result = getGridCEs(vo, ceBlackList=knownCEs, hostURL=hostURL)
    if not result["OK"]:
        gLogger.error("ERROR: failed to get CEs from BDII", result["Message"])
        DIRACExit(-1)
    ceBdiiDict = result["BdiiInfo"]

    unknownCEs = result["UnknownCEs"]
    if unknownCEs:
        gLogger.notice(f"There is no (longer) information about the following CEs for the {vo} VO:")
        gLogger.notice("\n".join(sorted(unknownCEs)))

    siteDict = result["Value"]
    if siteDict:
        gLogger.notice("New resources available:")
        for site in siteDict:
            diracSite = "Unknown"
            result = getDIRACSiteName(site)
            if result["OK"]:
                diracSite = ",".join(result["Value"])
            if siteDict[site]:
                gLogger.notice(f"  {site}, DIRAC site {diracSite}")
                for ce in siteDict[site]:
                    gLogger.notice(" " * 4 + ce)
                    gLogger.notice(
                        "      {}, {}".format(siteDict[site][ce]["CEType"], "%s_%s_%s" % siteDict[site][ce]["System"])
                    )
    else:
        gLogger.notice("No new resources available, exiting")
        return

    inp = input("\nDo you want to add sites ? [default=yes] [yes|no]: ")
    inp = inp.strip()
    if inp and inp.lower().startswith("n"):
        return

    gLogger.notice("\nAdding new sites/CEs interactively\n")

    sitesAdded = []

    for site in siteDict:
        # Get the country code:
        country = ""
        for ce in siteDict[site]:
            country = ce.strip().split(".")[-1].lower()
            if len(country) == 2:
                break
            if country == "gov":
                country = "us"
                break
        if not country or len(country) != 2:
            country = "xx"
        result = getDIRACSiteName(site)
        if not result["OK"]:
            gLogger.notice(f"\nThe site {site} is not yet in the CS, give it a name")
            diracSite = input(f"[help|skip|<domain>.<name>.{country}]: ")
            if diracSite.lower() == "skip":
                continue
            if diracSite.lower() == "help":
                gLogger.notice(f"{site} site details:")
                for k, v in ceBdiiDict[site].items():
                    if k != "CEs":
                        gLogger.notice(f"{k}\t{v}")
                gLogger.notice(f"\nEnter DIRAC site name in the form <domain>.<name>.{country}\n")
                diracSite = input(f"[<domain>.<name>.{country}]: ")
            try:
                _, _, _ = diracSite.split(".")
            except ValueError:
                gLogger.error(f"ERROR: DIRAC site name does not follow convention: {diracSite}")
                continue
            diracSites = [diracSite]
        else:
            diracSites = result["Value"]

        if len(diracSites) > 1:
            gLogger.notice(f"Attention! GOC site {site} corresponds to more than one DIRAC sites:")
            gLogger.notice(str(diracSites))
            gLogger.notice("Please, pay attention which DIRAC site the new CEs will join\n")

        newCEs = {}
        addedCEs = []
        for ce in siteDict[site]:
            ceType = siteDict[site][ce]["CEType"]
            for diracSite in diracSites:
                if ce in addedCEs:
                    continue
                yn = input(f"Add CE {ce} of type {ceType} to {diracSite}? [default yes] [yes|no]: ")
                if yn == "" or yn.lower().startswith("y"):
                    newCEs.setdefault(diracSite, [])
                    newCEs[diracSite].append(ce)
                    addedCEs.append(ce)

        for diracSite in diracSites:
            if diracSite in newCEs:
                cmd = f"dirac-admin-add-site {diracSite} {site} {' '.join(newCEs[diracSite])}"
                gLogger.notice(f"\nNew site/CEs will be added with command:\n{cmd}")
                yn = input("Add it ? [default yes] [yes|no]: ")
                if not (yn == "" or yn.lower().startswith("y")):
                    continue

                if dry:
                    gLogger.notice("Command is skipped in the dry run")
                else:
                    result = systemCall(0, shlex.split(cmd))
                    if not result["OK"]:
                        gLogger.error("Error while executing dirac-admin-add-site command")
                        yn = input("Do you want to continue ? [default no] [yes|no]: ")
                        if yn == "" or yn.lower().startswith("n"):
                            if sitesAdded:
                                gLogger.notice("CEs were added at the following sites:")
                                for site, diracSite in sitesAdded:
                                    gLogger.notice(f"{site}\t{diracSite}")
                            DIRACExit(0)
                    else:
                        exitStatus, stdData, errData = result["Value"]
                        if exitStatus:
                            gLogger.error(
                                "Error while executing dirac-admin-add-site command\n", "\n".join([stdData, errData])
                            )
                            yn = input("Do you want to continue ? [default no] [yes|no]: ")
                            if yn == "" or yn.lower().startswith("n"):
                                if sitesAdded:
                                    gLogger.notice("CEs were added at the following sites:")
                                    for site, diracSite in sitesAdded:
                                        gLogger.notice(f"{site}\t{diracSite}")
                                DIRACExit(0)
                        else:
                            sitesAdded.append((site, diracSite))
                            gLogger.notice(stdData)

    if sitesAdded:
        gLogger.notice("CEs were added at the following sites:")
        for site, diracSite in sitesAdded:
            gLogger.notice(f"{site}\t{diracSite}")
    else:
        gLogger.notice("No new CEs were added this time")


def updateCS(changeSet):
    global vo, dry, ceBdiiDict

    changeList = sorted(changeSet)
    if dry:
        gLogger.notice("The following needed changes are detected:\n")
    else:
        gLogger.notice("We are about to make the following changes to CS:\n")
    for entry in changeList:
        gLogger.notice("%s/%s %s -> %s" % entry)

    if not dry:
        csAPI = CSAPI()
        csAPI.initialize()
        result = csAPI.downloadCSData()
        if not result["OK"]:
            gLogger.error("Failed to initialize CSAPI object", result["Message"])
            DIRACExit(-1)
        for section, option, value, new_value in changeSet:
            if value == "Unknown" or not value:
                csAPI.setOption(cfgPath(section, option), new_value)
            else:
                csAPI.modifyValue(cfgPath(section, option), new_value)

        yn = input("Do you want to commit changes to CS ? [default yes] [yes|no]: ")
        if yn == "" or yn.lower().startswith("y"):
            result = csAPI.commit()
            if not result["OK"]:
                gLogger.error("Error while commit to CS", result["Message"])
            else:
                gLogger.notice(f"Successfully committed {len(changeSet)} changes to CS")


def updateSites():
    global vo, dry, ceBdiiDict, onecore

    result = getSiteUpdates(vo, bdiiInfo=ceBdiiDict, onecore=onecore)
    if not result["OK"]:
        gLogger.error("Failed to get site updates", result["Message"])
        DIRACExit(-1)
    changeSet = result["Value"]

    updateCS(changeSet)


def handler(signum, frame):
    gLogger.notice("\nExit is forced, bye...")
    DIRACExit(-1)


@Script()
def main():
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)

    global vo, dry, doCEs, ceBdiiDict

    processScriptSwitches()

    if not vo:
        gLogger.error("No VO specified")
        DIRACExit(-1)

    vo = getVOOption(vo, "VOMSName", vo)

    if doCEs:
        yn = input("Do you want to check/add new sites to CS ? [default yes] [yes|no]: ")
        yn = yn.strip()
        if yn == "" or yn.lower().startswith("y"):
            checkUnusedCEs()

        yn = input("Do you want to update CE details in the CS ? [default yes] [yes|no]: ")
        yn = yn.strip()
        if yn == "" or yn.lower().startswith("y"):
            updateSites()


if __name__ == "__main__":
    vo = ""
    dry = False
    doCEs = False
    ceBdiiDict = None

    main()
