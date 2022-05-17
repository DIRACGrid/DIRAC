from DIRAC import S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.OptimizerAdministrator.Optimizer import Optimizer


class StagerHandler(Optimizer):

    """
    The StagerHandler will get the list of job sites available, and if it
    founds out that all the job sites have the input data on tape, it will select
    a job site where the input data will be put on disk and it will set the
    job to the staged status
    """

    def __init__(self, jobState: JobState):
        super().__init__(jobState)
        self.__log = gLogger.getSubLogger(f"[jid={jobState.jid}]{__name__}")
        self.__operations = Operations()
        self.storageManagerClient = StorageManagerClient()

    def optimize(self):

        # Get input data
        result = self.jobState.getInputData()
        if not result["OK"]:
            self.__log.error("Failed to get input data from JobDB", result["Message"])
            return result

        # No input data? No staging required
        if not result["Value"]:
            self.__log.info("No input data. Skipping")
            return super().optimize()

        # Get input data LFNs state
        idAgent = self.__operations.getValue("OnlineSiteHandlerAgent", "OnlineSiteHandler")
        result = self.retrieveOptimizerParam(idAgent)
        if not result["OK"]:
            self.__log.error("Could not retrieve online site handler info", result["Message"])
            return result
        LFNs = result["Value"]

        # No offline LFNs? No staging required
        if "offlineLFNs" not in LFNs:
            return super().optimize()

        offlineLFNs = LFNs["offlineLFNs"]

        self.__log.debug(
            "Stage request will be \n\t%s" % "\n\t".join(["%s:%s" % (lfn, offlineLFNs[lfn]) for lfn in offlineLFNs])
        )

        result = self.jobState.setStatus(
            JobStatus.STAGING,
            self.__operations.getValue("StagingMinorStatus", "Request To Be Sent"),
            appStatus="",
            source=self.__class__.__name__,
        )
        if not result["OK"]:
            return result

        result = StorageManagerClient().setRequest(
            offlineLFNs,
            "WorkloadManagement",
            "updateJobFromStager@WorkloadManagement/JobStateUpdate",
            int(self.jobState.jid),
        )
        if not result["OK"]:
            self.__log.error("Could not send stage request", f": {result['Message']}")
            return result

        rid = str(result["Value"])
        self.__log.info("Stage request sent", rid)

        result = self.jobState.setStatus(
            JobStatus.STAGING,
            self.__operations.getValue("StagingMinorStatus", "Request Sent"),
            appStatus="",
            source=self.__class__.__name__,
        )
        if not result["OK"]:
            return result

        return S_OK()
