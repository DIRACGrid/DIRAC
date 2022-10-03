"""   The Job Scheduling Executor takes the information gained from all previous
      optimizers and makes a scheduling decision for the jobs.

      Subsequent to this jobs are added into a Task Queue and pilot agents can be submitted.

      All issues preventing the successful resolution of a site candidate are discovered
      here where all information is available.

      This Executor will fail affected jobs meaningfully.
"""

import random

from DIRAC import S_OK, S_ERROR, gConfig

from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.Core.Utilities.TimeUtilities import fromString, toEpoch
from DIRAC.Core.Security import Properties
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient, getFilesToStage
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor import OptimizerExecutor
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.Client import JobStatus


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
        cls.__jobDB = JobDB()
        return S_OK()

    def optimizeJob(self, jid, jobState):
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
        jobManifest = result["Value"]

        # Get site requirements
        result = self.__getSitesRequired(jobManifest)
        if not result["OK"]:
            return result
        userSites, userBannedSites = result["Value"]

        # Get job type
        result = jobState.getAttribute("JobType")
        if not result["OK"]:
            self.jobLog.error("Could not retrieve job type", result["Message"])
            return result
        jobType = result["Value"]

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
        jobPlatform = jobManifest.getOption("Platform", None)

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
            # No input data? Just send to TQ
            return self.__sendToTQ(jobState, jobManifest, userSites, userBannedSites)

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
                res = self.__checkStageAllowed(jobState)
                if not res["OK"]:
                    return res
                if not res["Value"]:
                    return S_ERROR("Stage not allowed")
                return self.__requestStaging(jobState, stageLFNs)
            else:
                # No staging required
                onlineSites = res["Value"]["onlineSites"]
                if onlineSites:
                    # Set the online site(s) first
                    userSites = set(userSites)
                    onlineSites &= userSites
                    userSites = list(onlineSites) + list(userSites - onlineSites)
                return self.__sendToTQ(jobState, jobManifest, userSites, userBannedSites, onlineSites=onlineSites)

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
        stageRequired, siteCandidates = self.__resolveStaging(inputData, idSites)
        if not siteCandidates:
            return S_ERROR("No destination sites available")

        # Is any site active?
        stageSites = self._applySiteFilter(siteCandidates, banned=wmsBannedSites)
        if not stageSites:
            return self.__holdJob(jobState, "Sites %s are inactive or banned" % ", ".join(siteCandidates))

        # If no staging is required send to TQ
        if not stageRequired:
            # Use siteCandidates and not stageSites because active and banned sites
            # will be taken into account on matching time
            return self.__sendToTQ(jobState, jobManifest, siteCandidates, userBannedSites)

        # Check if the user is allowed to stage
        if self.ex_getOption("RestrictDataStage", False):
            res = self.__checkStageAllowed(jobState)
            if not res["OK"]:
                return res
            if not res["Value"]:
                return S_ERROR("Stage not allowed")

        # Get stageSites[0] because it has already been randomized and it's as good as any in stageSites
        stageSite = stageSites[0]
        self.jobLog.verbose(" Staging site will be", stageSite)
        stageData = idSites[stageSite]
        # Set as if everything has already been staged
        stageData["disk"] += stageData["tape"]
        stageData["tape"] = 0
        # Set the site info back to the original dict to save afterwards
        opData["SiteCandidates"][stageSite] = stageData

        stageRequest = self.__preRequestStaging(jobManifest, stageSite, opData)
        if not stageRequest["OK"]:
            return stageRequest
        stageLFNs = stageRequest["Value"]
        result = self.__requestStaging(jobState, stageLFNs)
        if not result["OK"]:
            return result
        stageLFNs = result["Value"]
        self.__updateSharedSESites(jobManifest, stageSite, stageLFNs, opData)
        # Save the optimizer data again
        self.jobLog.verbose("Updating Optimizer Info", f": {idAgent} for {opData}")
        result = self.storeOptimizerParam(idAgent, opData)
        if not result["OK"]:
            return result

        return self.__setJobSite(jobState, stageSites)

    def _applySiteFilter(self, sites, banned=False):
        """Filters out banned sites"""
        if not sites:
            return sites

        filtered = set(sites)
        if banned and isinstance(banned, (list, set, dict)):
            filtered -= set(banned)
        return list(filtered)

    def __holdJob(self, jobState, holdMsg, delay=0):
        if delay:
            self.freezeTask(delay)
        else:
            self.freezeTask(self.ex_getOption("HoldTime", 300))
        self.jobLog.info("On hold", holdMsg)
        return jobState.setAppStatus(holdMsg, source=self.ex_optimizerName())

    def __getSitesRequired(self, jobManifest):
        """Returns any candidate sites specified by the job or sites that have been
        banned and could affect the scheduling decision.
        """

        bannedSites = jobManifest.getOption("BannedSites", [])
        if not bannedSites:
            bannedSites = jobManifest.getOption("BannedSite", [])
        if bannedSites:
            self.jobLog.info("Banned sites", ", ".join(bannedSites))

        sites = jobManifest.getOption("Site", [])
        # TODO: Only accept known sites after removing crap like ANY set in the original manifest
        sites = [site for site in sites if site.strip().lower() not in ("any", "")]

        if sites:
            if len(sites) == 1:
                self.jobLog.info("Single chosen site", ": %s specified" % (sites[0]))
            else:
                self.jobLog.info("Multiple sites requested", ": %s" % ",".join(sites))
            sites = self._applySiteFilter(sites, banned=bannedSites)
            if not sites:
                return S_ERROR("Impossible site requirement")

        return S_OK((sites, bannedSites))

    def __filterByPlatform(self, jobPlatform, userSites):
        """Filters out sites that have no CE with a matching platform."""
        basePath = "/Resources/Sites"
        filteredSites = set()

        # FIXME: can use Resources().getSiteCEMapping()
        for site in userSites:
            if "." not in site:
                # Invalid site name: Doesn't contain a dot!
                self.jobLog.warn("Skipped invalid site name", site)
                continue
            grid = site.split(".")[0]
            sitePath = cfgPath(basePath, grid, site, "CEs")
            result = gConfig.getSections(sitePath)
            if not result["OK"]:
                self.jobLog.info("Failed to get CEs", "at site %s" % site)
                continue
            siteCEs = result["Value"]

            for CEName in siteCEs:
                CEPlatform = gConfig.getValue(cfgPath(sitePath, CEName, "OS"))
                if jobPlatform == CEPlatform:
                    # Site has a CE with a matchin platform
                    filteredSites.add(site)

        return S_OK(list(filteredSites))

    def _getTagsFromManifest(self, jobManifest):
        """helper method to add a list of tags to the TQ from the job manifest content"""

        # Generate Tags from specific requirements
        tagList = []

        # sorting out the number of processors
        nProcessors = 1
        maxProcessors = 1

        if "NumberOfProcessors" in jobManifest:  # this should be the exact number
            nProcessors = jobManifest.getOption("NumberOfProcessors", 0)

        else:  # is there a min? and in that case, is there a max?
            if "MinNumberOfProcessors" in jobManifest:
                nProcessors = jobManifest.getOption("MinNumberOfProcessors", 0)

                if "MaxNumberOfProcessors" in jobManifest:
                    maxProcessors = jobManifest.getOption("MaxNumberOfProcessors", 0)
                else:
                    maxProcessors = -1

        if nProcessors and nProcessors > 1:
            tagList.append("%dProcessors" % nProcessors)
            tagList.append("MultiProcessor")
        if maxProcessors == -1 or maxProcessors > 1:
            tagList.append("MultiProcessor")

        if "WholeNode" in jobManifest:
            if jobManifest.getOption("WholeNode", "").lower() in ["1", "yes", "true", "y"]:
                tagList.append("WholeNode")
                tagList.append("MultiProcessor")

        # sorting out the RAM (this should be probably coded ~same as number of processors)
        if "MaxRAM" in jobManifest:
            maxRAM = jobManifest.getOption("MaxRAM", 0)
            if maxRAM:
                tagList.append("%dGB" % maxRAM)

        # other tags? Just add them
        if "Tags" in jobManifest:
            tagList.extend(jobManifest.getOption("Tags", []))
        if "Tag" in jobManifest:
            tagList.extend(jobManifest.getOption("Tag", []))

        return tagList

    def __sendToTQ(self, jobState, jobManifest, sites, bannedSites, onlineSites=None):
        """This method sends jobs to the task queue agent and if candidate sites
        are defined, updates job JDL accordingly.
        """

        tagList = self._getTagsFromManifest(jobManifest)
        if tagList:
            jobManifest.setOption("Tags", ", ".join(tagList))

        reqSection = "JobRequirements"
        if reqSection in jobManifest:
            result = jobManifest.getSection(reqSection)
        else:
            result = jobManifest.createSection(reqSection)
        if not result["OK"]:
            self.jobLog.error("Cannot create jobManifest section", "(%s: %s)" % reqSection, result["Message"])
            return result
        reqCfg = result["Value"]

        if sites:
            reqCfg.setOption("Sites", ", ".join(sites))
        if bannedSites:
            reqCfg.setOption("BannedSites", ", ".join(bannedSites))

        # Job multivalue requirement keys are specified as singles in the job descriptions
        # but for backward compatibility can be also plurals
        for key in ("JobType", "GridRequiredCEs", "GridCE", "Tags"):
            reqKey = key
            if key == "JobType":
                reqKey = "JobTypes"
            elif key == "GridRequiredCEs" or key == "GridCE":  # Remove obsolete GridRequiredCEs
                reqKey = "GridCEs"
            if key in jobManifest:
                reqCfg.setOption(reqKey, ", ".join(jobManifest.getOption(key, [])))

        result = self.__setJobSite(jobState, sites, onlineSites=onlineSites)
        if not result["OK"]:
            return result

        self.jobLog.verbose("Done")
        return self.setNextOptimizer(jobState)

    def __resolveStaging(self, inputData, idSites):
        diskSites = []
        maxOnDisk = 0
        bestSites = []

        for site in idSites:
            nTape = idSites[site]["tape"]
            nDisk = idSites[site]["disk"]
            if nTape > 0:
                self.jobLog.debug(f"{nTape} tape replicas on site {site}")
            if nDisk > 0:
                self.jobLog.debug(f"{nDisk} disk replicas on site {site}")
                if nDisk == len(inputData):
                    diskSites.append(site)
            if nDisk > maxOnDisk:
                maxOnDisk = nDisk
                bestSites = [site]
            elif nDisk == maxOnDisk:
                bestSites.append(site)

        # If there are selected sites, those are disk only sites
        if diskSites:
            self.jobLog.verbose("No staging required")
            return (False, diskSites)

        self.jobLog.verbose("Staging required")
        if len(bestSites) > 1:
            random.shuffle(bestSites)
        return (True, bestSites)

    def __preRequestStaging(self, jobManifest, stageSite, opData):

        tapeSEs = []
        diskSEs = []
        vo = jobManifest.getOption("VirtualOrganization")
        inputDataPolicy = jobManifest.getOption("InputDataPolicy", "Protocol")
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

    def __requestStaging(self, jobState, stageLFNs):
        """Actual request for staging LFNs through the StorageManagerClient"""
        self.jobLog.debug(
            "Stage request will be \n\t%s" % "\n\t".join([f"{lfn}:{stageLFNs[lfn]}" for lfn in stageLFNs])
        )

        stagerClient = StorageManagerClient()
        result = jobState.setStatus(
            JobStatus.STAGING,
            self.ex_getOption("StagingMinorStatus", "Request To Be Sent"),
            appStatus="",
            source=self.ex_optimizerName(),
        )
        if not result["OK"]:
            return result

        result = stagerClient.setRequest(
            stageLFNs, "WorkloadManagement", "updateJobFromStager@WorkloadManagement/JobStateUpdate", int(jobState.jid)
        )
        if not result["OK"]:
            self.jobLog.error("Could not send stage request", ": %s" % result["Message"])
            return result

        rid = str(result["Value"])
        self.jobLog.info("Stage request sent", "(%s)" % rid)
        self.storeOptimizerParam("StageRequest", rid)

        result = jobState.setStatus(
            JobStatus.STAGING,
            self.ex_getOption("StagingMinorStatus", "Request Sent"),
            appStatus="",
            source=self.ex_optimizerName(),
        )
        if not result["OK"]:
            return result

        return S_OK(stageLFNs)

    def __updateSharedSESites(self, jobManifest, stageSite, stagedLFNs, opData):
        siteCandidates = opData["SiteCandidates"]

        seStatus = {}
        vo = jobManifest.getOption("VirtualOrganization")
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

    def __setJobSite(self, jobState, siteList, onlineSites=None):
        """Set the site attribute"""
        if onlineSites is None:
            onlineSites = []
        numSites = len(siteList)
        if numSites == 0:
            self.jobLog.info("Any site is candidate")
            return jobState.setAttribute("Site", "ANY")
        elif numSites == 1:
            self.jobLog.info("Only 1 site is candidate", ": %s" % siteList[0])
            return jobState.setAttribute("Site", siteList[0])

        # If the job has input data, the online sites are hosting the data
        if len(onlineSites) == 1:
            siteName = "Group.%s" % ".".join(list(onlineSites)[0].split(".")[1:])
            self.jobLog.info("Group %s is candidate" % siteName)
        elif onlineSites:
            # More than one site with input
            siteName = "MultipleInput"
            self.jobLog.info("Several input sites are candidate", ": %s" % ",".join(onlineSites))
        else:
            # No input site reported (could be a user job)
            siteName = "Multiple"
            self.jobLog.info("Multiple sites are candidate")

        return jobState.setAttribute("Site", siteName)

    def __checkStageAllowed(self, jobState):
        """Check if the job credentials allow to stage date"""
        result = jobState.getAttribute("OwnerGroup")
        if not result["OK"]:
            self.jobLog.error("Cannot retrieve OwnerGroup from DB", ": %s" % result["Message"])
            return result
        group = result["Value"]
        return S_OK(Properties.STAGE_ALLOWED in Registry.getPropertiesForGroup(group))
