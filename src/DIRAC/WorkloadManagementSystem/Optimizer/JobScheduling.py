"""
The Job Scheduling Optimizer takes the information gained from all previous
optimizers and makes a scheduling decision for the jobs.

Subsequent to this jobs are added into a Task Queue and pilot agents can be submitted.

All issues preventing the successful resolution of a site candidate are discovered
here where all information is available.

This optimizer will fail affected jobs meaningfully.
"""

import random

from DIRAC import S_OK, S_ERROR, gConfig, gLogger

from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.Core.Security import Properties
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient, getFilesToStage
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.OptimizerAdministrator.Optimizer import Optimizer


class JobScheduling(Optimizer):
    """
    The specific Optimizer must provide the following methods:
    - optimize() - the main method called for each job
    """

    def __init__(self, jobState: JobState):
        """Constructor"""
        super().__init__(jobState)
        self.log = gLogger.getSubLogger(f"[jid={jobState.jid}]{__name__}")
        self.__operations = Operations()
        self.siteClient = SiteStatus()

    def optimize(self):
        """1. Banned sites are removed from the destination list.
        2. Get input files
        3. Production jobs are sent directly to TQ
        4. Check if staging is necessary
        """

        # Get the job manifest for the later checks
        result = self.jobState.getManifest()
        if not result["OK"]:
            self.log.error("Could not retrieve job manifest", result["Message"])
            return result
        jobManifest = result["Value"]

        # Get site requirements
        result = self.__getSitesRequired(jobManifest)
        if not result["OK"]:
            return result
        userSites, userBannedSites = result["Value"]

        # Get job type
        result = self.jobState.getAttribute("JobType")
        if not result["OK"]:
            self.log.error("Could not retrieve job type", result["Message"])
            return result
        jobType = result["Value"]

        # Get banned sites from DIRAC
        result = self.siteClient.getSites("Banned")
        if not result["OK"]:
            self.log.error("Cannot retrieve banned sites", result["Message"])
            return result
        wmsBannedSites = result["Value"]

        # If the user has selected any site, filter them and hold the job if not able to run
        if userSites:
            if jobType not in self.__operations.getValue("ExcludedOnHoldJobTypes", []):

                result = self.siteClient.getUsableSites(userSites)
                if not result["OK"]:
                    self.log.error(
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
                    self.log.debug("Invalid site(s) requested: %s" % ",".join(invalidSites))
                    if not self.__operations.getValue("AllowInvalidSites", True):
                        raise ValueError(f"Requested site(s) {','.join(invalidSites)} are invalid")

                if bannedSites:
                    self.log.debug("Banned site(s) %s ignored" % ",".join(bannedSites))
                    if not usableSites:
                        raise ValueError(f"Requested site(s) {','.join(invalidSites)} are inactive")

                if not usableSites:
                    raise ValueError("No requested site(s) are active/valid")

                userSites = list(usableSites)

        checkPlatform = self.__operations.getValue("CheckPlatform", False)
        jobPlatform = jobManifest.getOption("Platform", None)
        # First check that the platform is valid (in OSCompatibility list)
        if checkPlatform and jobPlatform:
            result = gConfig.getOptionsDict("/Resources/Computing/OSCompatibility")
            if not result["OK"]:
                self.log.error("Unable to get OSCompatibility list", result["Message"])
                return result
            allPlatforms = result["Value"]
            if jobPlatform not in allPlatforms:
                self.log.error("Platform not supported", jobPlatform)
                return S_ERROR("Platform is not supported")

        # Filter the userSites by the platform selection (if there is one)
        if checkPlatform and userSites:
            if jobPlatform:
                result = self.__filterByPlatform(jobPlatform, userSites)
                if not result["OK"]:
                    self.log.error("Failed to filter job sites by platform", result["Message"])
                    return result
                userSites = result["Value"]
                if not userSites:
                    # No sites left after filtering -> Invalid platform/sites combination
                    self.log.error("No selected sites match platform", jobPlatform)
                    return S_ERROR("No selected sites match platform '%s'" % jobPlatform)

        # Check if there is input data
        result = self.jobState.getInputData()
        if not result["OK"]:
            self.log.error("Failed to get input data from JobDB", result["Message"])
            return result

        if not result["Value"]:
            # No input data? Just send to TQ
            return self.__sendToTQ(jobManifest, userSites, userBannedSites)

        self.log.verbose("Has an input data requirement")
        inputData = result["Value"]

        # ===================================================================================
        # Production jobs are sent to TQ, but first we have to verify if staging is necessary
        # ===================================================================================
        if jobType in self.__operations.getValue("Transformations/DataProcessing", []):
            self.log.info("Production job: sending to TQ, but first checking if staging is requested")

            res = getFilesToStage(
                inputData,
                jobState=self.jobState,
                checkOnlyTapeSEs=self.__operations.getValue("CheckOnlyTapeSEs", True),
                jobLog=self.log,
            )

            if not res["OK"]:
                raise ValueError(res["Message"])

            if res["Value"]["absentLFNs"]:
                # Some files do not exist at all... set the job Failed
                # Reverse errors
                reasons = {}
                for lfn, reason in res["Value"]["absentLFNs"].items():
                    reasons.setdefault(reason, []).append(lfn)
                for reason, lfns in reasons.items():
                    # Some files are missing in the FC or in SEs, fail the job
                    self.log.error(reason, ",".join(lfns))
                error = ",".join(reasons)
                return S_ERROR(error)

            if res["Value"]["failedLFNs"]:
                raise ValueError("Couldn't get storage metadata of some files")

            stageLFNs = res["Value"]["offlineLFNs"]
            if stageLFNs:
                res = self.__checkStageAllowed()
                if not res["OK"]:
                    return res
                if not res["Value"]:
                    return S_ERROR("Stage not allowed")
                self.__requestStaging(stageLFNs)
                return S_OK()

            # No staging required
            onlineSites = res["Value"]["onlineSites"]
            if onlineSites:
                # Set the online site(s) first
                userSites = set(userSites)
                onlineSites &= userSites
                userSites = list(onlineSites) + list(userSites - onlineSites)
            return self.__sendToTQ(jobManifest, userSites, userBannedSites, onlineSites=onlineSites)

        # ===================================================
        # From now on we know it's a user job with input data
        # ===================================================

        idAgent = self.__operations.getValue("InputDataAgent", "InputData")
        result = self.retrieveOptimizerParam(idAgent)
        if not result["OK"]:
            self.log.error("Could not retrieve input data info", result["Message"])
            return result
        opData = result["Value"]

        if "SiteCandidates" not in opData:
            return S_ERROR("No possible site candidates")

        # Filter input data sites with user requirement
        siteCandidates = list(opData["SiteCandidates"])
        self.log.info("Site candidates are", siteCandidates)

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
                self.log.error("Site candidate does not have all the input data", "(%s)" % site)
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
            raise ValueError(f"Sites {', '.join(siteCandidates)} are inactive or banned")

        # If no staging is required send to TQ
        if not stageRequired:
            # Use siteCandidates and not stageSites because active and banned sites
            # will be taken into account on matching time
            return self.__sendToTQ(jobManifest, siteCandidates, userBannedSites)

        # Check if the user is allowed to stage
        if self.__operations.getValue("RestrictDataStage", False):
            res = self.__checkStageAllowed()
            if not res["OK"]:
                return res
            if not res["Value"]:
                return S_ERROR("Stage not allowed")

        # Get stageSites[0] because it has already been randomized and it's as good as any in stageSites
        stageSite = stageSites[0]
        self.log.verbose(" Staging site will be", stageSite)
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
        result = self.__requestStaging(stageLFNs)
        if not result["OK"]:
            return result
        stageLFNs = result["Value"]
        self.__updateSharedSESites(jobManifest, stageSite, stageLFNs, opData)
        # Save the optimizer data again
        self.log.verbose("Updating Optimizer Info", ": %s for %s" % (idAgent, opData))
        result = self.storeOptimizerParam(idAgent, opData)
        if not result["OK"]:
            return result

        return self.__setJobSite(stageSites)

    def _applySiteFilter(self, sites, banned=False):
        """Filters out banned sites"""
        if not sites:
            return sites

        filtered = set(sites)
        if banned and isinstance(banned, (list, set, dict)):
            filtered -= set(banned)
        return list(filtered)

    def __getSitesRequired(self, jobManifest):
        """Returns any candidate sites specified by the job or sites that have been
        banned and could affect the scheduling decision.
        """

        bannedSites = jobManifest.getOption("BannedSites", [])
        if not bannedSites:
            bannedSites = jobManifest.getOption("BannedSite", [])
        if bannedSites:
            self.log.info("Banned sites", ", ".join(bannedSites))

        sites = jobManifest.getOption("Site", [])
        # TODO: Only accept known sites after removing crap like ANY set in the original manifest
        sites = [site for site in sites if site.strip().lower() not in ("any", "")]

        if sites:
            if len(sites) == 1:
                self.log.info("Single chosen site", ": %s specified" % (sites[0]))
            else:
                self.log.info("Multiple sites requested", ": %s" % ",".join(sites))
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
                self.log.warn("Skipped invalid site name", site)
                continue
            grid = site.split(".")[0]
            sitePath = cfgPath(basePath, grid, site, "CEs")
            result = gConfig.getSections(sitePath)
            if not result["OK"]:
                self.log.info("Failed to get CEs", "at site %s" % site)
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

    def __sendToTQ(self, jobManifest, sites, bannedSites, onlineSites=None):
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
            self.log.error("Cannot create jobManifest section", "(%s: %s)" % reqSection, result["Message"])
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
            elif key == "GridRequiredCEs" or key == "GridCE":  # TODO: Remove obsolete GridRequiredCEs
                reqKey = "GridCEs"
            if key in jobManifest:
                reqCfg.setOption(reqKey, ", ".join(jobManifest.getOption(key, [])))

        result = self.__setJobSite(sites, onlineSites=onlineSites)
        if not result["OK"]:
            return result

        self.log.verbose("Done")
        return S_OK()

    def __resolveStaging(self, inputData, idSites):
        diskSites = []
        maxOnDisk = 0
        bestSites = []

        for site in idSites:
            nTape = idSites[site]["tape"]
            nDisk = idSites[site]["disk"]
            if nTape > 0:
                self.log.debug("%s tape replicas on site %s" % (nTape, site))
            if nDisk > 0:
                self.log.debug("%s disk replicas on site %s" % (nDisk, site))
                if nDisk == len(inputData):
                    diskSites.append(site)
            if nDisk > maxOnDisk:
                maxOnDisk = nDisk
                bestSites = [site]
            elif nDisk == maxOnDisk:
                bestSites.append(site)

        # If there are selected sites, those are disk only sites
        if diskSites:
            self.log.verbose("No staging required")
            return (False, diskSites)

        self.log.verbose("Staging required")
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

        self.log.debug("Tape SEs are %s" % (", ".join(tapeSEs)))

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
        sortedSEs = reversed(sorted([(len(stageLFNs[seName]), seName) for seName in stageLFNs]))
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

    def __requestStaging(self, stageLFNs):
        """Actual request for staging LFNs through the StorageManagerClient"""
        self.log.debug(
            "Stage request will be \n\t%s" % "\n\t".join(["%s:%s" % (lfn, stageLFNs[lfn]) for lfn in stageLFNs])
        )

        stagerClient = StorageManagerClient()
        result = self.jobState.setStatus(
            JobStatus.STAGING,
            self.__operations.getValue("StagingMinorStatus", "Request To Be Sent"),
            appStatus="",
            source=self.__class__.__name__,
        )
        if not result["OK"]:
            return result

        result = stagerClient.setRequest(
            stageLFNs,
            "WorkloadManagement",
            "updateJobFromStager@WorkloadManagement/JobStateUpdate",
            int(self.jobState.jid),
        )
        if not result["OK"]:
            self.log.error("Could not send stage request", ": %s" % result["Message"])
            return result

        rid = str(result["Value"])
        self.log.info("Stage request sent", "(%s)" % rid)
        self.storeOptimizerParam("", rid)

        result = self.jobState.setStatus(
            JobStatus.STAGING,
            self.__operations.getValue("StagingMinorStatus", "Request Sent"),
            appStatus="",
            source=self.__class__.__name__,
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
            self.log.debug("Checking %s for shared SEs" % siteName)
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
            self.log.debug("Disk SEs for %s are %s" % (siteName, ", ".join(diskSEs)))

            # Hell again to the dev of this crappy value of value of successful of ...
            lfnData = opData["Value"]["Value"]["Successful"]
            for seName in stagedLFNs:
                # If the SE is not close then skip it
                if seName not in closeSEs:
                    continue
                for lfn in stagedLFNs[seName]:
                    self.log.debug("Checking %s for %s" % (seName, lfn))
                    # I'm pretty sure that this cannot happen :P
                    if lfn not in lfnData:
                        continue
                    # Check if it's already on disk at the site
                    onDisk = False
                    for siteSE in lfnData[lfn]:
                        if siteSE in diskSEs:
                            self.log.verbose("lfn on disk", ": %s at %s" % (lfn, siteSE))
                            onDisk = True
                    # If not on disk, then update!
                    if not onDisk:
                        self.log.verbose("Setting LFN to disk", "for %s" % seName)
                        siteData["disk"] += 1
                        siteData["tape"] -= 1

    def __setJobSite(self, siteList, onlineSites=None):
        """Set the site attribute"""
        if onlineSites is None:
            onlineSites = []
        numSites = len(siteList)
        if numSites == 0:
            self.log.info("Any site is candidate")
            return self.jobState.setAttribute("Site", "ANY")

        if numSites == 1:
            self.log.info("Only 1 site is candidate", ": %s" % siteList[0])
            return self.jobState.setAttribute("Site", siteList[0])

        # If the job has input data, the online sites are hosting the data
        if len(onlineSites) == 1:
            siteName = "Group.%s" % ".".join(list(onlineSites)[0].split(".")[1:])
            self.log.info("Group %s is candidate" % siteName)
        elif onlineSites:
            # More than one site with input
            siteName = "MultipleInput"
            self.log.info("Several input sites are candidate", ": %s" % ",".join(onlineSites))
        else:
            # No input site reported (could be a user job)
            siteName = "Multiple"
            self.log.info("Multiple sites are candidate")

        return self.jobState.setAttribute("Site", siteName)

    def __checkStageAllowed(self):
        """Check if the job credentials allow to stage date"""
        result = self.jobState.getAttribute("OwnerGroup")
        if not result["OK"]:
            self.log.error("Cannot retrieve OwnerGroup from DB", ": %s" % result["Message"])
            return result
        group = result["Value"]
        return S_OK(Properties.STAGE_ALLOWED in Registry.getPropertiesForGroup(group))
