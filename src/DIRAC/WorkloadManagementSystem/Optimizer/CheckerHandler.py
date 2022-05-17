from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.OptimizerAdministrator.Optimizer import Optimizer


class CheckerHandler(Optimizer):
    def __init__(self, jobState: JobState) -> None:
        """Constructor"""
        super().__init__(jobState)
        self.__log = gLogger.getSubLogger(f"[jid={jobState.jid}]{__name__}")
        self.__operations = Operations()

    def optimize(self):

        # Get job manifest
        result = self.jobState.getManifest()
        if not result["OK"]:
            self.__log.error("Could not retrieve job manifest", result["Message"])
            return result
        jobManifest = result["Value"]

        tagList = self._getTagsFromManifest(jobManifest)
        if tagList:
            jobManifest.setOption("Tags", ", ".join(tagList))

        reqSection = "JobRequirements"
        if reqSection in jobManifest:
            result = jobManifest.getSection(reqSection)
        else:
            result = jobManifest.createSection(reqSection)
        if not result["OK"]:
            self.__log.error("Cannot create jobManifest section", f"({reqSection}: {result['Message']})")
            return result
        reqCfg = result["Value"]

        # Job multivalue requirement keys are specified as singles in the job descriptions
        # but for backward compatibility can be also plurals
        for key in ("JobType", "GridCE", "Tags"):
            reqKey = key
            if key == "JobType":
                reqKey = "JobTypes"
            elif key == "GridCE":
                reqKey = "GridCEs"
            if key in jobManifest:
                reqCfg.setOption(reqKey, ", ".join(jobManifest.getOption(key, [])))

        # Check if there is input data
        result = self.jobState.getInputData()
        if result["OK"]:
            inputData = result["Value"]

        if inputData:

            # Get online LFNs sites status
            idAgent = self.__operations.getValue("OnlineSiteHandlerAgent", "OnlineSiteHandler")
            result = self.retrieveOptimizerParam(idAgent)
            if not result["OK"]:
                self.__log.error("Could not retrieve online site handler info", result["Message"])
                return result
            LFNs = result["Value"]

            # If there is oneline LFNs, no need for staging
            if not "onlineLFNs" in LFNs:
                return S_ERROR("No online LFNs site, the job should have been staged")

            result = self.__setJobSite(sites, LFNs["onlineLFNs"])
            if not result["OK"]:
                return result
        else:
            result = self.__setJobSite(sites)
            if not result["OK"]:
                return result

        self.__log.verbose("Done")
        return S_OK()

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

    def __setJobSite(self, computeSites, onlineStorageSites=None):
        """Set the site attribute"""
        if onlineStorageSites is None:
            onlineStorageSites = []
        numSites = len(computeSites)
        if numSites == 0:
            self.__log.info("Any site is candidate")
            return self.jobState.setAttribute("Site", "ANY")
        if numSites == 1:
            self.__log.info("Only 1 site is candidate", computeSites[0])
            return self.jobState.setAttribute("Site", computeSites[0])

        # If the job has input data, the online sites are hosting the data
        if onlineStorageSites:
            if len(onlineStorageSites) == 1:
                siteName = f"Group.{'.'.join(list(onlineStorageSites)[0].split('.')[1:])}"
                self.__log.info(f"Group {siteName} is candidate")
            else:
                # More than one site with input
                # TODO: why don't we store the specific sites here ?
                siteName = "MultipleInput"
                self.__log.info("Several input sites are candidate", ": %s" % ",".join(onlineStorageSites))
        else:
            # No input site reported (could be a user job)
            # TODO: why is it set to "Multiple" if no storage site is necessary
            siteName = "Multiple"
            self.__log.info("Multiple sites are candidate")

        return self.jobState.setAttribute("Site", siteName)
