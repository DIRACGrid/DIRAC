from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSiteCEMapping
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.OptimizerAdministrator.Optimizer import Optimizer

from DIRAC import S_OK, S_ERROR, gConfig, gLogger


class SiteFilter(Optimizer):
    """
    Filter the user sites and stores the result
    """

    def __init__(self, jobState: JobState) -> None:
        """Constructor"""
        super().__init__(jobState)
        self.__log = gLogger.getSubLogger(f"[jid={jobState.jid}]{__name__}")
        self.__operations = Operations()
        self.__siteStatus = SiteStatus()

    def optimize(self):
        """
        If a userSites is set, it will filter out user banned sites and save only the usable sites
        If all the user selected sites are filtered out, it will return S_ERROR or ValueError

        :raises: ValueError if the optimizer needs to be retried
        :return: S_OK() / S_ERROR()
        """

        # Get job manifest
        result = self.jobState.getManifest()
        if not result["OK"]:
            self.__log.error("Could not retrieve job manifest", result["Message"])
            return result
        jobManifest = result["Value"]

        # Get job type
        result = self.jobState.getAttribute("JobType")
        if not result["OK"]:
            self.__log.error("Could not retrieve job type", result["Message"])
            return result
        jobType = result["Value"]

        # If it's not a production job
        if jobType not in self.__operations.getValue("Transformations/DataProcessing", []):

            # Get site candidates
            idAgent = self.__operations.getValue("InputDataAgent", "InputData")
            result = self.retrieveOptimizerParam(idAgent)
            if not result["OK"]:
                self.__log.error("Could not retrieve input data info", result["Message"])
                return result
            opData = result["Value"]
            if "SiteCandidates" not in opData:
                return S_ERROR("No possible site candidates")
            siteCandidates = set(opData["SiteCandidates"])
            self.__log.info("Site candidates are", siteCandidates)

        # Get user site from manifest
        userSites = jobManifest.getOption("Site", [])
        # Removing crap like ANY in the original manifest
        userSites = {site for site in userSites if site.strip().lower() not in ("any", "")}
        if not userSites:
            self.__log.info("No site specified by the user")
            if siteCandidates:
                userSites = siteCandidates
            else:
                return S_ERROR("No user sites nor site candidates")
        else:
            if len(userSites) == 1:
                self.__log.info("Single chosen site", f": {userSites[0]} specified")
            else:
                self.__log.info("Multiple sites requested", ",".join(userSites))

            # Filter input data sites with user requirement
            rejectedSitesCandidates = userSites - siteCandidates
            if rejectedSitesCandidates:
                self.__log.debug("Rejected site candidate(s)", ",".join(rejectedSitesCandidates))
                userSites -= rejectedSitesCandidates
                if not userSites:
                    self.__log.error("Impossible InputData * Site requirements")
                    return S_ERROR("Impossible InputData * Site requirements")

        # Get user banned sites from manifest
        userBannedSites = jobManifest.getOption("BannedSites", [])
        if not userBannedSites:
            userBannedSites = jobManifest.getOption("BannedSite", [])
        if userBannedSites:
            self.__log.info("User banned sites", ", ".join(userBannedSites))

        # Filter user banned sites
        userBannedSites = userSites & userBannedSites  # intersection
        if userBannedSites:  # if user sites containes wms banned sites
            self.__log.debug("User banned site(s) ignored", ", ".join(userBannedSites))
            userSites -= userBannedSites
            if not userSites:
                self.__log.error("Impossible site requirement: no site available after filtering user banned sites")
                return S_ERROR("Impossible site requirement", "no site available after filtering user banned sites")

        # Check that the platform is valid (in OSCompatibility list)
        checkPlatform = self.__operations.getValue("CheckPlatform", False)
        jobPlatform = jobManifest.getOption("Platform", None)
        if checkPlatform and jobPlatform:
            result = gConfig.getOptionsDict("/Resources/Computing/OSCompatibility")
            if not result["OK"]:
                self.__log.error("Unable to get OSCompatibility list", result["Message"])
                return result
            allPlatforms = result["Value"]
            if jobPlatform not in allPlatforms:
                self.__log.error("Platform not supported", jobPlatform)
                return S_ERROR(f"Platform {jobPlatform} is not supported")

        # Filter the userSites by the platform selection (if there is one)
        if userSites and checkPlatform and jobPlatform:
            result = self.__filterByPlatform(jobPlatform, userSites)
            if not result["OK"]:
                self.__log.error("Failed to filter job sites by platform", result["Message"])
                return result
            userSites = result["Value"]
            if not userSites:
                # No sites left after filtering -> Invalid platform/sites combination
                self.__log.error("No selected sites match platform", jobPlatform)
                return S_ERROR(f"No selected sites match platform '{jobPlatform}'")

        # Get usable sites from SiteStatus
        result = self.__siteStatus.getUsableSites(userSites)
        if not result["OK"]:
            self.__log.error("Problem checking userSites for tuple of active/banned/invalid sites", result["Message"])
            return result
        usableSites = set(result["Value"])
        if not usableSites:
            self.__log.error("No usable site available at all")
            raise ValueError("No usable site available at all")

        # Check if invalid sites are allowed
        invalidSites = userSites - usableSites
        if invalidSites:
            self.__log.debug("Invalid requested site(s)", ",".join(invalidSites))
            if not self.__operations.getValue("AllowInvalidSites", True):
                raise ValueError(f"Requested site(s) {','.join(invalidSites)} are invalid")

        # Get input data
        result = self.jobState.getInputData()
        if not result["OK"]:
            self.log.error("Cannot retrieve input data", result["Message"])
            return result
        inputData = result["Value"]

        # If the job has input data
        if inputData:
            # Get online sites from StorageSiteResolver
            idAgent = self.__operations.getValue("StorageSiteResolverAgent", "StorageSiteResolver")
            result = self.retrieveOptimizerParam(idAgent)
            if not result["OK"]:
                self.__log.error("Could not retrieve storage site resolver info", result["Message"])
                return result
            opData = result["Value"]

            # If online sites are available for the job
            if "onlineSites" in opData:
                # Filtering the online sites
                onlineSites = result["Value"]["onlineSites"]

                bannedOnlineSites = onlineSites - userSites
                if bannedOnlineSites:
                    self.__log.debug("Online banned site(s) ignored", ", ".join(bannedOnlineSites))
                    onlineSites -= bannedOnlineSites

                    # If all the online sites have been banned
                    if not onlineSites:
                        self.__log.info("All the online sites have been banned")
                        return S_ERROR("All the online sites have been banned")

                # Set the online site(s) first in the list of user sites
                userSites = list(onlineSites) + list(userSites - onlineSites)

        # Storing the filtered job requrements
        # reqSection = "JobRequirements"
        # if reqSection in jobManifest:
        #     result = jobManifest.getSection(reqSection)
        # else:
        #     result = jobManifest.createSection(reqSection)
        # if not result["OK"]:
        #     self.__log.error("Cannot create jobManifest section", f"({reqSection}: {result['Message']})")
        #     return result
        # reqCfg = result["Value"]

        # if userSites:
        #     reqCfg.setOption("Sites", ", ".join(userSites))
        # if userBannedSites:
        #     reqCfg.setOption("BannedSites", ", ".join(userBannedSites))
        # if invalidSites:
        #     reqCfg.setOption("InvalidSites", ", ".join(invalidSites))
        # if rejectedSitesCandidates:
        #     reqCfg.setOption("RejectedSitesCandidates", ", ".join(rejectedSitesCandidates))

        # # TODO: Storing the valid user sites. Necessary ?

        self.log.debug(f"Storing the usable filtered sites:\n{usableSites}")
        result = self.storeOptimizerParam(self.__class__.__name__, usableSites)
        if not result["OK"]:
            self.log.warn(result["Message"])
            return result

        self.__setJobSite(usableSites)

        return S_OK(usableSites)

    def __filterByPlatform(self, jobPlatform, userSites):
        """Filters out sites that have no CE with a matching platform."""
        basePath = "/Resources/Sites"
        filteredSites = set()

        # TODO: check if this works
        # result = getSiteCEMapping()
        # if not result["OK"]:
        #     return result
        # dicti = result["Value"]

        # for site in userSites:
        #     for CEName in dicti[site]:
        #         CEPlatform = gConfig.getValue(cfgPath(sitePath, CEName, "OS"))
        #         if jobPlatform == CEPlatform:
        #             # Site has a CE with a matching platform
        #             filteredSites.add(site)

        # FIXME: can use Resources().getSiteCEMapping()
        for site in userSites:
            if "." not in site:
                # Invalid site name: Doesn't contain a dot!
                self.__log.warn("Skipped invalid site name", site)
                continue
            grid = site.split(".")[0]
            sitePath = cfgPath(basePath, grid, site, "CEs")
            result = gConfig.getSections(sitePath)
            if not result["OK"]:
                self.__log.info("Failed to get CEs", f"at site {site}")
                continue
            siteCEs = result["Value"]

            for CEName in siteCEs:
                CEPlatform = gConfig.getValue(cfgPath(sitePath, CEName, "OS"))
                if jobPlatform == CEPlatform:
                    # Site has a CE with a matchin platform
                    filteredSites.add(site)

        return S_OK(list(filteredSites))

    def __setJobSite(self, siteList, onlineSites=None):
        """Set the site attribute"""
        if onlineSites is None:
            onlineSites = []
        numSites = len(siteList)
        if numSites == 0:
            self.__log.info("Any site is candidate")
            return self.jobState.setAttribute("Site", "ANY")

        if numSites == 1:
            self.__log.info(f"Only 1 site is candidate", siteList[0])
            return self.jobState.setAttribute("Site", siteList[0])

        # If the job has input data, the online sites are hosting the data
        if len(onlineSites) == 1:
            siteName = "Group.%s" % ".".join(list(onlineSites)[0].split(".")[1:])
            self.__log.info("Group %s is candidate" % siteName)
        elif onlineSites:
            # More than one site with input
            siteName = "MultipleInput"
            self.__log.info("Several input sites are candidate", ": %s" % ",".join(onlineSites))
        else:
            # No input site reported (could be a user job)
            siteName = "Multiple"
            self.__log.info("Multiple sites are candidate")

        return self.jobState.setAttribute("Site", siteName)
