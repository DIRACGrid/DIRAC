""""""

from DIRAC import S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import getFilesToStage
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.OptimizerAdministrator.Optimizer import Optimizer


class StorageSiteResolver(Optimizer):
    def __init__(self, jobState: JobState) -> None:
        super().__init__(jobState)
        self.__log = gLogger.getSubLogger(f"[jid={jobState.jid}]{__name__}")
        self.__operations = Operations()

    def optimize(self):
        """
        Raises S_ERROR if there is absent of failed LFNs,
        and stores offline and online LFNs.
        """

        # Check if there is input data
        result = self.jobState.getInputData()
        if not result["OK"]:
            self.__log.error("Failed to get input data from JobDB", result["Message"])
            return result

        if not result["Value"]:
            # No input data? Go to next optimizer
            return super().optimize()

        self.__log.verbose("Has an input data requirement")
        inputData = result["Value"]

        result = getFilesToStage(
            inputData,
            jobState=self.jobState,
            checkOnlyTapeSEs=self.__operations.getValue("CheckOnlyTapeSEs", True),
            jobLog=self.__log,
        )
        if not result["OK"]:
            raise ValueError(result["Message"])
        absentLFNs = result["Value"]["absentLFNs"]
        failedLFNs = result["Value"]["failedLFNs"]
        offlineLFNs = result["Value"]["offlineLFNs"]
        onlineLFNs = result["Value"]["onlineLFNs"]
        onlineSites = result["Value"]["onlineSites"]

        if absentLFNs:
            # Some files do not exist at all... set the job Failed
            # Reverse errors
            reasons = {}
            for lfn, reason in absentLFNs.items():
                reasons.setdefault(reason, []).append(lfn)
            for reason, lfns in reasons.items():
                # Some files are missing in the FC or in SEs, fail the job
                self.__log.error(reason, ",".join(lfns))
            error = ",".join(reasons)
            return S_ERROR(error)

        if failedLFNs:
            raise ValueError("Couldn't get storage metadata of some files")

        result = self.storeOptimizerParam(
            self.__class__.__name__, {"onlineLFNs": onlineLFNs, "offlineLFNs": offlineLFNs, "onlineSites": onlineSites}
        )
        if not result["OK"]:
            return result

        return super().optimize()
