from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus, JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.OptimizerAdministrator.Optimizer import Optimizer


class WaitingHandler(Optimizer):
    def __init__(self, jobState: JobState) -> None:
        super().__init__(jobState)
        self.__log = gLogger.getSubLogger(f"[jid={jobState.jid}]{self.__class__.__name__}")
        self.__operations = Operations()

    def optimize(self):

        # Check if there is input data
        result = self.jobState.getInputData()
        if result["OK"]:
            inputData = result["Value"]

        # TODO: get filtered compute sites

        if inputData:
            # Get online LFNs sites status
            # TODO: get storage sites

            result = self.__setJobSite(sites, LFNs["onlineLFNs"])
            if not result["OK"]:
                return result
        else:
            result = self.__setJobSite(sites)
            if not result["OK"]:
                return result

        self.__log.verbose("Done")
        return self.jobState.setStatus(
            JobStatus.WAITING,
            minorStatus=JobMinorStatus.PILOT_AGENT_SUBMISSION,
            appStatus="Unknown",
            source=self.__class__.__name__,
        )

    def __setJobSite(self, computeSites, onlineStorageSites=None):
        """Set the site attribute"""
        if onlineStorageSites is None:
            onlineStorageSites = []

        numSites = len(computeSites)
        if numSites == 0:
            self.__log.info("Any site is candidate")
            siteName = "ANY"
        elif numSites == 1:
            self.__log.info("Only 1 site is candidate", computeSites[0])
            siteName = computeSites[0]
        else:
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
                siteName = "Multiple"
                self.__log.info("Multiple sites are candidate")

        return self.jobState.setAttribute("Site", siteName)
