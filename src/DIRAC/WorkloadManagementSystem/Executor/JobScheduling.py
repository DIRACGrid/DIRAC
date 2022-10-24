"""   The Job Scheduling Executor takes the information gained from all previous
      optimizers and makes a scheduling decision for the jobs.

      Subsequent to this jobs are added into a Task Queue and pilot agents can be submitted.

      All issues preventing the successful resolution of a site candidate are discovered
      here where all information is available.

      This Executor will fail affected jobs meaningfully.
"""

import random

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities.ClassAd import ClassAd

from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.Core.Utilities.TimeUtilities import fromString, toEpoch
from DIRAC.Core.Security import Properties
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import getFilesToStage
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor import OptimizerExecutor


class JobScheduling(OptimizerExecutor):
    """
    The specific Optimizer must provide the following methods:
    - optimizeJob() - the main method called for each job
    and it can provide:
    - initializeOptimizer() before each execution cycle
    """

    @classmethod
    def initializeOptimizer(cls):
        """Initialization of the optimizer."""
        cls.siteClient = SiteStatus()
        return S_OK()

    def optimizeJob(self, jid, jobState: JobState):
        """1. Banned sites are removed from the destination list.
        2. Get input files
        3. Production jobs are sent directly to TQ
        4. Check if staging is necessary
        """
        # Reschedule delay
        result = jobState.getAttributes(["RescheduleCounter", "RescheduleTime", "ApplicationStatus"])
        if not result["OK"]:
            return result
        attDict = result["Value"]
        try:
            reschedules = int(attDict["RescheduleCounter"])
        except (ValueError, KeyError):
            return S_ERROR("RescheduleCounter has to be an integer")
        if reschedules != 0:
            delays = self.ex_getOption("RescheduleDelays", [60, 180, 300, 600])
            delay = delays[min(reschedules - 1, len(delays) - 1)]
            waited = toEpoch() - toEpoch(fromString(attDict["RescheduleTime"]))
            if waited < delay:
                return self.__holdJob(jobState, "On Hold: after rescheduling %s" % reschedules, delay)

        # Get the job manifest for the later checks
        result = jobState.getManifest()
        if not result["OK"]:
            self.jobLog.error("Could not retrieve job manifest", result["Message"])
            return result
        jobDescription = result["Value"]

        # Get site requirements
        result = self.__getSitesRequired(jobDescription)
        if not result["OK"]:
            return result
        userSites, userBannedSites = result["Value"]

        # Get job type
        jobType = jobDescription.getAttributeString("JobType")

        # Get banned sites from DIRAC
        result = self.siteClient.getSites("Banned")
        if not result["OK"]:
            self.jobLog.error("Cannot retrieve banned sites", result["Message"])
            return result
        wmsBannedSites = result["Value"]

        # If the user has selected any site, filter them and hold the job if not able to run
        if userSites:
            if jobType not in self.ex_getOption("ExcludedOnHoldJobTypes", []):

                result = self.siteClient.getUsableSites(userSites)
                if not result["OK"]:
                    self.jobLog.error(
                        "Problem checking userSites for tuple of active/banned/invalid sites", result["Message"]
                    )
                    return result
                usableSites = set(result["Value"])
                bannedSites = []
                invalidSites = []
                for site in userSites:
                    if site in wmsBannedSites:
                        bannedSites.append(site)
                    elif site not in usableSites:
                        invalidSites.append(site)

                if invalidSites:
                    self.jobLog.debug("Invalid site(s) requested: %s" % ",".join(invalidSites))
                    if not self.ex_getOption("AllowInvalidSites", True):
                        return self.__holdJob(jobState, "Requested site(s) %s are invalid" % ",".join(invalidSites))
                if bannedSites:
                    self.jobLog.debug("Banned site(s) %s ignored" % ",".join(bannedSites))
                    if not usableSites:
                        return self.__holdJob(jobState, "Requested site(s) %s are inactive" % ",".join(bannedSites))

                if not usableSites:
                    return self.__holdJob(jobState, "No requested site(s) are active/valid")
                userSites = list(usableSites)

        checkPlatform = self.ex_getOption("CheckPlatform", False)
        jobPlatform = jobDescription.getOption("Platform", None)

        # Check that the platform is valid (in OSCompatibility list)
        if checkPlatform and jobPlatform:
            result = gConfig.getOptionsDict("/Resources/Computing/OSCompatibility")
            if not result["OK"]:
                self.jobLog.error("Unable to get OSCompatibility list", result["Message"])
                return result
            allPlatforms = result["Value"]
            if jobPlatform not in allPlatforms:
                self.jobLog.error("Platform not supported", jobPlatform)
                return S_ERROR("Platform is not supported")

        # Check if there is input data
        result = jobState.getInputData()
        if not result["OK"]:
            self.jobLog.error("Failed to get input data from JobDB", result["Message"])
            return result

        if not result["Value"]:
            # No input data? Just send to next optimizer
            return self.setNextOptimizer(jobState)

        self.jobLog.verbose("Has an input data requirement")
        inputData = result["Value"]

        # ===================================================================================
        # Production jobs are sent to TQ, but first we have to verify if staging is necessary
        # ===================================================================================
        if jobType in Operations().getValue("Transformations/DataProcessing", []):
            self.jobLog.info("Production job: sending to TQ, but first checking if staging is requested")

            res = getFilesToStage(
                inputData,
                jobState=jobState,
                checkOnlyTapeSEs=self.ex_getOption("CheckOnlyTapeSEs", True),
                jobLog=self.jobLog,
            )

            if not res["OK"]:
                return self.__holdJob(jobState, res["Message"])
            if res["Value"]["absentLFNs"]:
                # Some files do not exist at all... set the job Failed
                # Reverse errors
                reasons = {}
                for lfn, reason in res["Value"]["absentLFNs"].items():
                    reasons.setdefault(reason, []).append(lfn)
                for reason, lfns in reasons.items():
                    # Some files are missing in the FC or in SEs, fail the job
                    self.jobLog.error(reason, ",".join(lfns))
                error = ",".join(reasons)
                return S_ERROR(error)

            if res["Value"]["failedLFNs"]:
                return self.__holdJob(jobState, "Couldn't get storage metadata of some files")
            stageLFNs = res["Value"]["offlineLFNs"]
            if stageLFNs:
                if not self.isStageAllowed(jobDescription):
                    return S_ERROR("Stage not allowed")
                self.__requestStaging(jobDescription, stageLFNs)
                return self.setNextOptimizer(jobState)

            # No staging required
            onlineSites = res["Value"]["onlineSites"]
            jobDescription.insertAttributeVectorString("OnlineSites", onlineSites)
            return self.setNextOptimizer(jobState)

        # ===================================================
        # From now on we know it's a user job with input data
        # ===================================================

        idAgent = self.ex_getOption("InputDataAgent", "InputData")
        result = self.retrieveOptimizerParam(idAgent)
        if not result["OK"]:
            self.jobLog.error("Could not retrieve input data info", result["Message"])
            return result
        opData = result["Value"]

        if "SiteCandidates" not in opData:
            return S_ERROR("No possible site candidates")

        # Filter input data sites with user requirement
        siteCandidates = list(opData["SiteCandidates"])
        self.jobLog.info("Site candidates are", siteCandidates)

        if userSites:
            siteCandidates = list(set(siteCandidates) & set(userSites))

        siteCandidates = self._applySiteFilter(siteCandidates, banned=userBannedSites)
        if not siteCandidates:
            return S_ERROR("Impossible InputData * Site requirements")

        idSites = {}
        for site in siteCandidates:
            idSites[site] = opData["SiteCandidates"][site]

        # Check if sites have correct count of disk+tape replicas
        numData = len(inputData)
        errorSites = set()
        for site in idSites:
            if numData != idSites[site]["disk"] + idSites[site]["tape"]:
                self.jobLog.error("Site candidate does not have all the input data", "(%s)" % site)
                errorSites.add(site)
        for site in errorSites:
            idSites.pop(site)
        if not idSites:
            return S_ERROR("Site candidates do not have all the input data")

        # Check if staging is required
        onlineSites = self.getOnlineSites(inputData, idSites)
        if onlineSites:
            self.jobLog.verbose("No staging required")
            # No filtering because active and banned sites
            # will be taken into account on matching time
            jobDescription.insertAttributeVectorString("OnlineSites", onlineSites)
            return self.setNextOptimizer(jobState)

        self.jobLog.verbose("Staging required")

        # Check if the user is allowed to stage
        if self.ex_getOption("RestrictDataStage", False):
            if not self.isStageAllowed(jobDescription):
                return S_ERROR("Stage not allowed")

        bestStagingSites = self.getBestStagingSites(idSites)
        if not bestStagingSites:
            return S_ERROR("No destination sites available")

        # Is any site active?
        bestStagingSites = self._applySiteFilter(bestStagingSites, banned=wmsBannedSites)
        if not bestStagingSites:
            # TODO: There is other staging sites available, but not as good as the banned ones. What should we do ?
            return self.__holdJob(jobState, f"Sites {', '.join(bestStagingSites)} are inactive or banned")

        # We choose a stage site between the best options we got
        stageSite = random.choice(bestStagingSites)
        self.jobLog.verbose(" Staging site will be", stageSite)
        stageData = idSites[stageSite]
        # Set as if everything has already been staged
        stageData["disk"] += stageData["tape"]
        stageData["tape"] = 0
        # Set the site info back to the original dict to save afterwards
        opData["SiteCandidates"][stageSite] = stageData

        vo = jobDescription.getAttributeString("VirtualOrganization")
        inputDataPolicy = jobDescription.getAttributeString("InputDataPolicy")

        stageRequest = self.__preRequestStaging(vo, inputDataPolicy, stageSite, opData)
        if not stageRequest["OK"]:
            return stageRequest
        stageLFNs = stageRequest["Value"]

        jobDescription.insertAttributeVectorString("OnlineSites", [stageSite])

        self.__updateSharedSESites(vo, stageSite, stageLFNs, opData)
        # Save the optimizer data again
        self.jobLog.verbose("Updating Optimizer Info", f": {idAgent} for {opData}")
        result = self.storeOptimizerParam(idAgent, opData)
        if not result["OK"]:
            return result

        self.__requestStaging(jobDescription, stageLFNs)

        return self.setNextOptimizer(jobState)

    def _applySiteFilter(self, sites, banned=False):
        """Filters out banned sites"""
        if not sites:
            return sites

        filtered = set(sites)
        if banned and isinstance(banned, (list, set, dict)):
            filtered -= set(banned)
        return list(filtered)

    def __holdJob(self, jobState: JobState, holdMsg, delay=0):
        if delay:
            self.freezeTask(delay)
        else:
            self.freezeTask(self.ex_getOption("HoldTime", 300))
        self.jobLog.info("On hold", holdMsg)
        return jobState.setStatus(appStatus=holdMsg, source=self.ex_optimizerName())

    def __getSitesRequired(self, jobDescription: ClassAd):
        """Returns any candidate sites specified by the job or sites that have been
        banned and could affect the scheduling decision.
        """

        bannedSites = jobDescription.getListFromExpression("BannedSites")
        if bannedSites:
            self.jobLog.info("Banned sites", ", ".join(bannedSites))

        sites = jobDescription.getListFromExpression("Sites")
        if sites:
            if len(sites) == 1:
                self.jobLog.info("Single chosen site", ": %s specified" % (sites[0]))
            else:
                self.jobLog.info("Multiple sites requested", ": %s" % ",".join(sites))
            sites = self._applySiteFilter(sites, banned=bannedSites)
            if not sites:
                return S_ERROR("Impossible site requirement")

        return S_OK((sites, bannedSites))

    def getOnlineSites(self, inputData, idSites: dict):
        """Get the list of sites that contains all the data on disk"""
        diskSites = []
        for site in idSites:
            if idSites[site]["disk"] == len(inputData):
                diskSites.append(site)
        return diskSites

    def getBestStagingSites(self, idSites: dict):
        maxOnDisk = 0
        bestSites = []

        for site in idSites:
            nTape = idSites[site]["tape"]
            nDisk = idSites[site]["disk"]
            if nTape > 0:
                self.jobLog.debug(f"{nTape} tape replicas on site {site}")
            if nDisk > 0:
                self.jobLog.debug(f"{nDisk} disk replicas on site {site}")
            if nDisk > maxOnDisk:
                maxOnDisk = nDisk
                bestSites = [site]
            elif nDisk == maxOnDisk:
                bestSites.append(site)

        return bestSites

    def __preRequestStaging(self, vo: str, inputDataPolicy: str, stageSite, opData):

        tapeSEs = []
        diskSEs = []
        connectionLevel = "DOWNLOAD" if "download" in inputDataPolicy.lower() else "PROTOCOL"
        # Allow staging from SEs accessible by protocol
        result = DMSHelpers(vo=vo).getSEsForSite(stageSite, connectionLevel=connectionLevel)
        if not result["OK"]:
            return S_ERROR("Could not determine SEs for site %s" % stageSite)
        siteSEs = result["Value"]

        for seName in siteSEs:
            se = StorageElement(seName, vo=vo)
            seStatus = se.getStatus()
            if not seStatus["OK"]:
                return seStatus
            seStatus = seStatus["Value"]
            if seStatus["Read"] and seStatus["TapeSE"]:
                tapeSEs.append(seName)
            if seStatus["Read"] and seStatus["DiskSE"]:
                diskSEs.append(seName)

        if not tapeSEs:
            return S_ERROR("No Local SEs for site %s" % stageSite)

        self.jobLog.debug("Tape SEs are %s" % (", ".join(tapeSEs)))

        # I swear this is horrible DM code it's not mine.
        # Eternity of hell to the inventor of the Value of Value of Success of...
        inputData = opData["Value"]["Value"]["Successful"]
        stageLFNs = {}
        lfnToStage = []
        for lfn in inputData:
            replicas = inputData[lfn]
            # Check SEs
            seStage = []
            for seName in replicas:
                if seName in diskSEs:
                    # This lfn is in disk. Skip it
                    seStage = []
                    break
                if seName not in tapeSEs:
                    # This lfn is not in this tape SE. Check next SE
                    continue
                seStage.append(seName)
            for seName in seStage:
                if seName not in stageLFNs:
                    stageLFNs[seName] = []
                stageLFNs[seName].append(lfn)
                if lfn not in lfnToStage:
                    lfnToStage.append(lfn)

        if not stageLFNs:
            return S_ERROR("Cannot find tape replicas")

        # Check if any LFN is in more than one SE
        # If that's the case, try to stage from the SE that has more LFNs to stage to group the request
        # 1.- Get the SEs ordered by ascending replicas
        sortedSEs = reversed(sorted((len(stageLFNs[seName]), seName) for seName in stageLFNs))
        for lfn in lfnToStage:
            found = False
            # 2.- Traverse the SEs
            for _stageCount, seName in sortedSEs:
                if lfn in stageLFNs[seName]:
                    # 3.- If first time found, just mark as found. Next time delete the replica from the request
                    if found:
                        stageLFNs[seName].remove(lfn)
                    else:
                        found = True
                # 4.-If empty SE, remove
                if not stageLFNs[seName]:
                    stageLFNs.pop(seName)

        return S_OK(stageLFNs)

    def __requestStaging(self, jobDescription: ClassAd, stageLFNs: dict[str, list[str]]) -> None:
        """Actual request for staging LFNs through the StorageManagerClient"""
        self.jobLog.debug(
            "Stage request will be \n\t%s" % "\n\t".join([f"{lfn}:{stageLFNs[lfn]}" for lfn in stageLFNs])
        )

        classAd = ClassAd()
        for key, value in stageLFNs.items():
            classAd.insertAttributeVectorString(key, value)

        jobDescription.insertAttributeSubsection("StageLFNs", classAd)

    def __updateSharedSESites(self, vo: str, stageSite, stagedLFNs, opData):
        siteCandidates = opData["SiteCandidates"]

        seStatus = {}
        for siteName in siteCandidates:
            if siteName == stageSite:
                continue
            self.jobLog.debug("Checking %s for shared SEs" % siteName)
            siteData = siteCandidates[siteName]
            result = getSEsForSite(siteName)
            if not result["OK"]:
                continue
            closeSEs = result["Value"]
            diskSEs = []
            for seName in closeSEs:
                # If we don't have the SE status get it and store it
                if seName not in seStatus:
                    seStatus[seName] = StorageElement(seName, vo=vo).status()
                # get the SE status from mem and add it if its disk
                status = seStatus[seName]
                if status["Read"] and status["DiskSE"]:
                    diskSEs.append(seName)
            self.jobLog.debug("Disk SEs for {} are {}".format(siteName, ", ".join(diskSEs)))

            # Hell again to the dev of this crappy value of value of successful of ...
            lfnData = opData["Value"]["Value"]["Successful"]
            for seName in stagedLFNs:
                # If the SE is not close then skip it
                if seName not in closeSEs:
                    continue
                for lfn in stagedLFNs[seName]:
                    self.jobLog.debug(f"Checking {seName} for {lfn}")
                    # I'm pretty sure that this cannot happen :P
                    if lfn not in lfnData:
                        continue
                    # Check if it's already on disk at the site
                    onDisk = False
                    for siteSE in lfnData[lfn]:
                        if siteSE in diskSEs:
                            self.jobLog.verbose("lfn on disk", f": {lfn} at {siteSE}")
                            onDisk = True
                    # If not on disk, then update!
                    if not onDisk:
                        self.jobLog.verbose("Setting LFN to disk", "for %s" % seName)
                        siteData["disk"] += 1
                        siteData["tape"] -= 1

    def isStageAllowed(self, jobDescription: ClassAd):
        """Check if the job credentials allow to stage date"""
        group = jobDescription.getAttributeString("OwnerGroup")
        return Properties.STAGE_ALLOWED in Registry.getPropertiesForGroup(group)
