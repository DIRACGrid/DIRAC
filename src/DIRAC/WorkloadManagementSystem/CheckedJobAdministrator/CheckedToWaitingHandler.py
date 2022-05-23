from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.CheckedJobAdministrator.CheckedJobHandler import CheckedJobHandler
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus, JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState


class CheckedToWaitingHandler(CheckedJobHandler):
    def __init__(self, jobState: JobState) -> None:
        super().__init__(jobState)
        self.log = gLogger.getSubLogger(f"[jid={jobState.jid}]{self.__class__.__name__}")

    def handle(self):

        # Check if there is input data
        result = self.jobState.getInputData()
        if result["OK"]:
            inputData = result["Value"]

        # TODO: get filtered compute sites

        if inputData:
            # Get online LFNs sites status
            # TODO: get storage sites
            computeSites = []
            storageSites = []

            result = self.__setJobSite(computeSites, storageSites)
            if not result["OK"]:
                return result
        else:
            result = self.__setJobSite(computeSites)
            if not result["OK"]:
                return result

        self.log.verbose("Done")
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
            self.log.info("Any site is candidate")
            siteName = "ANY"
        elif numSites == 1:
            self.log.info("Only 1 site is candidate", computeSites[0])
            siteName = computeSites[0]
        else:
            # If the job has input data, the online sites are hosting the data
            if onlineStorageSites:
                if len(onlineStorageSites) == 1:
                    siteName = f"Group.{'.'.join(list(onlineStorageSites)[0].split('.')[1:])}"
                    self.log.info(f"Group {siteName} is candidate")
                else:
                    # More than one site with input
                    # TODO: why don't we store the specific sites here ?
                    siteName = "MultipleInput"
                    self.log.info("Several input sites are candidate", ",".join(onlineStorageSites))
            else:
                # No input site reported (could be a user job)
                siteName = "Multiple"
                self.log.info("Multiple sites are candidate")

        return self.jobState.setAttribute("Site", siteName)
