""" The Bdii2CSAgent performs checking BDII for availability of CE
resources for a given or any configured VO. It detects resources not yet
present in the CS and notifies the administrators.
For the CEs already present in the CS, the agent is updating
if necessary settings which were changed in the BDII recently

The following options can be set for the Bdii2CSAgent.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN Bdii2CSAgent
  :end-before: ##END
  :dedent: 2
  :caption: Bdii2CSAgent options
"""
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOs, getVOOption
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueues, getCESiteMapping
from DIRAC.ConfigurationSystem.Client.Utilities import getGridCEs, getSiteUpdates
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.Glue2 import getGlue2CEInfo
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient


class Bdii2CSAgent(AgentModule):
    def __init__(self, *args, **kwargs):
        """Defines default parameters"""

        super().__init__(*args, **kwargs)

        self.addressTo = ""
        self.addressFrom = ""
        self.voName = []
        self.subject = self.am_getModuleParam("fullName")
        self.alternativeBDIIs = []
        self.voBdiiCEDict = {}
        self.voBdiiSEDict = {}
        self.host = "cclcgtopbdii01.in2p3.fr:2170"
        self.injectSingleCoreQueues = False
        self.csAPI = None

        # What to get
        self.processCEs = True
        self.selectedSites = []

        # Update the CS or not?
        self.dryRun = False

    def initialize(self):
        """Gets run paramaters from the configuration"""

        self.addressTo = self.am_getOption("MailTo", self.addressTo)
        self.addressFrom = self.am_getOption("MailFrom", self.addressFrom)
        # Create a list of alternative bdii urls
        self.alternativeBDIIs = self.am_getOption("AlternativeBDIIs", self.alternativeBDIIs)
        self.host = self.am_getOption("Host", self.host)
        self.injectSingleCoreQueues = self.am_getOption("InjectSingleCoreQueues", self.injectSingleCoreQueues)

        # Check if the bdii url is appended by a port number, if not append the default 2170
        for index, url in enumerate(self.alternativeBDIIs):
            if not url.split(":")[-1].isdigit():
                self.alternativeBDIIs[index] += ":2170"
        if self.addressTo and self.addressFrom:
            self.log.info("MailTo", self.addressTo)
            self.log.info("MailFrom", self.addressFrom)
        if self.alternativeBDIIs:
            self.log.info("AlternativeBDII URLs:", self.alternativeBDIIs)

        self.processCEs = self.am_getOption("ProcessCEs", self.processCEs)
        self.selectedSites = self.am_getOption("SelectedSites", [])
        self.dryRun = self.am_getOption("DryRun", self.dryRun)

        self.voName = self.am_getOption("VirtualOrganization", self.voName)
        if not self.voName:
            self.voName = self.am_getOption("VO", [])
        if not self.voName or (len(self.voName) == 1 and self.voName[0].lower() == "all"):
            # Get all VOs defined in the configuration
            self.voName = []
            result = getVOs()
            if result["OK"]:
                vos = result["Value"]
                for vo in vos:
                    vomsVO = getVOOption(vo, "VOMSName")
                    if vomsVO:
                        self.voName.append(vomsVO)

        if self.voName:
            self.log.info(f"Agent will manage VO(s) {self.voName}")
        else:
            self.log.fatal("VirtualOrganization option not defined for agent")
            return S_ERROR()

        self.csAPI = CSAPI()
        return self.csAPI.initialize()

    def execute(self):
        """General agent execution method"""
        self.voBdiiCEDict = {}

        # Get a "fresh" copy of the CS data
        result = self.csAPI.downloadCSData()
        if not result["OK"]:
            self.log.warn("Could not download a fresh copy of the CS data", result["Message"])

        # Refresh the configuration from the controller server
        gConfig.forceRefresh(fromMaster=True)

        if self.processCEs:
            self.__lookForNewCEs()
            self.__updateCEs()
        return S_OK()

    def __lookForNewCEs(self):
        """Look up BDII for CEs not yet present in the DIRAC CS"""

        bannedCEs = self.am_getOption("BannedCEs", [])

        for vo in self.voName:
            # get the known CEs for a given VO, so we can know the unknowns, or no longer supported,
            # for a VO
            res = getQueues(community=vo)
            if not res["OK"]:
                return res

            knownCEs = set()
            for _site, ces in res["Value"].items():
                knownCEs.update(ces)
            knownCEs.update(bannedCEs)

            result = self.__getGlue2CEInfo(vo)
            if not result["OK"]:
                continue
            bdiiInfo = result["Value"]
            result = getGridCEs(vo, bdiiInfo=bdiiInfo, ceBlackList=knownCEs)
            if not result["OK"]:
                self.log.error("Failed to get unused CEs", result["Message"])
                continue  # next VO
            siteDict = result["Value"]
            unknownCEs = set(result["UnknownCEs"]) - set(bannedCEs)

            body = ""
            for site in siteDict:
                newCEs = set(siteDict[site])  # pylint: disable=no-member
                if not newCEs:
                    continue

                ceString = ""
                for ce in newCEs:
                    queueString = ""
                    ceInfo = bdiiInfo[site]["CEs"][ce]
                    newCEString = f"CE: {ce}, GOCDB Site Name: {site}"
                    systemTuple = siteDict[site][ce]["System"]
                    osString = "%s_%s_%s" % (systemTuple)
                    newCEString = f"\n{newCEString}\n{osString}\n"
                    for queue in ceInfo["Queues"]:
                        queueStatus = ceInfo["Queues"][queue].get("GlueCEStateStatus", "UnknownStatus")
                        if "production" in queueStatus.lower():
                            ceType = ceInfo["Queues"][queue].get("GlueCEImplementationName", "")
                            queueString += f"   {queue} {queueStatus} {ceType}\n"
                    if queueString:
                        ceString += newCEString
                        ceString += "Queues:\n"
                        ceString += queueString

                if ceString:
                    body += ceString

            if siteDict:
                body = f"\nWe are glad to inform You about new CE(s) possibly suitable for {vo}:\n" + body
                body += "\n\nTo suppress information about CE add its name to BannedCEs list.\n"
                body += f"Add new Sites/CEs for vo {vo} with the command:\n"
                body += f"dirac-admin-add-resources --vo {vo} --ce\n"

            if unknownCEs:
                body += "\n\n"
                body += f"There is no (longer) information about the following CEs for the {vo} VO.\n"
                body += "\n".join(sorted(unknownCEs))
                body += "\n\n"

            if body:
                self.log.info(body)
                if self.addressTo and self.addressFrom:
                    notification = NotificationClient()
                    result = notification.sendMail(
                        self.addressTo, self.subject, body, self.addressFrom, localAttempt=False
                    )
                    if not result["OK"]:
                        self.log.error("Can not send new site notification mail", result["Message"])

        return S_OK()

    def __getGlue2CEInfo(self, vo):
        if vo in self.voBdiiCEDict:
            return S_OK(self.voBdiiCEDict[vo])
        self.log.info("Check for available CEs for VO", vo)
        totalResult = S_OK({})
        message = ""

        mainResult = getGlue2CEInfo(vo, host=self.host)
        if not mainResult["OK"]:
            self.log.error("Failed getting information from default bdii", mainResult["Message"])
            message = mainResult["Message"]

        for bdii in reversed(self.alternativeBDIIs):
            resultAlt = getGlue2CEInfo(vo, host=bdii)
            if resultAlt["OK"]:
                totalResult["Value"].update(resultAlt["Value"])
            else:
                self.log.error(f"Failed getting information from {bdii} ", resultAlt["Message"])
                message = (message + "\n" + resultAlt["Message"]).strip()

        if mainResult["OK"]:
            totalResult["Value"].update(mainResult["Value"])

        if not totalResult["Value"] and message:  # Dict is empty and we have an error message
            self.log.error("Error during BDII request", message)
            totalResult = S_ERROR(message)
        else:
            self.voBdiiCEDict[vo] = totalResult["Value"]
            self.__purgeSites(totalResult["Value"])

        return totalResult

    def __updateCEs(self):
        """Update the Site/CE/queue settings in the CS if they were changed in the BDII"""

        bdiiChangeSet = set()
        bannedCEs = self.am_getOption("BannedCEs", [])

        for vo in self.voName:
            result = self.__getGlue2CEInfo(vo)
            if not result["OK"]:
                continue
            ceBdiiDict = result["Value"]

            for _siteName, ceDict in ceBdiiDict.items():
                for bannedCE in bannedCEs:
                    ceDict["CEs"].pop(bannedCE, None)

            result = getSiteUpdates(vo, bdiiInfo=ceBdiiDict, log=self.log, onecore=self.injectSingleCoreQueues)

            if not result["OK"]:
                continue
            bdiiChangeSet = bdiiChangeSet.union(result["Value"])

        # We have collected all the changes, consolidate VO settings
        result = self.__updateCS(bdiiChangeSet)
        return result

    def __purgeSites(self, ceBdiiDict):
        """Remove all sites that are not in self.selectedSites.

        Modifies the ceBdiiDict!
        """
        if not self.selectedSites:
            return
        for site in list(ceBdiiDict):
            ces = list(ceBdiiDict[site]["CEs"])
            if not ces:
                self.log.error("No CE information for site:", site)
                continue
            siteInCS = "Not_In_CS"
            for ce in ces:
                res = getCESiteMapping(ce)
                if not res["OK"]:
                    self.log.error("Failed to get DIRAC site name for ce", f"{ce}: {res['Message']}")
                    continue
                # if the ce is not in the CS the returned value will be empty
                if ce in res["Value"]:
                    siteInCS = res["Value"][ce]
                    break
            self.log.debug(f"Checking site {site} ({ces}), aka {siteInCS}")
            if siteInCS in self.selectedSites:
                continue
            self.log.info(f"Dropping site {site}, aka {siteInCS}")
            ceBdiiDict.pop(site)
        return

    def __updateCS(self, bdiiChangeSet):
        queueVODict = {}
        changeSet = set()
        for entry in bdiiChangeSet:
            section, option, _value, new_value = entry
            if option == "VO":
                queueVODict.setdefault(section, set())
                queueVODict[section] = queueVODict[section].union(set(new_value.split(",")))
            else:
                changeSet.add(entry)
        for section, VOs in queueVODict.items():  # can be an iterator
            changeSet.add((section, "VO", "", ",".join(VOs)))

        if changeSet:
            changeList = sorted(changeSet)
            body = "\n".join(["%s/%s %s -> %s" % entry for entry in changeList])
            if body and self.addressTo and self.addressFrom:
                notification = NotificationClient()
                result = notification.sendMail(self.addressTo, self.subject, body, self.addressFrom, localAttempt=False)

            if body:
                self.log.info("The following configuration changes were detected:")
                self.log.info(body)

            for section, option, value, new_value in changeSet:
                if value == "Unknown" or not value:
                    self.csAPI.setOption(cfgPath(section, option), new_value)
                else:
                    self.csAPI.modifyValue(cfgPath(section, option), new_value)

            if self.dryRun:
                self.log.info("Dry Run: CS won't be updated")
                self.csAPI.showDiff()
            else:
                result = self.csAPI.commit()
                if not result["OK"]:
                    self.log.error("Error while committing to CS", result["Message"])
                else:
                    self.log.info(f"Successfully committed {len(changeList)} changes to CS")
                return result
        else:
            self.log.info("No changes found")
            return S_OK()
