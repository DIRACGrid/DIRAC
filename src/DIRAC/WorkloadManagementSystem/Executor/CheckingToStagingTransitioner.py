"""
Set the job to the STAGING state if the jobDescription contains
a StageLFNs subsection and sets the staging request
Otherwise, it will launch the next optimizer.
"""

from DIRAC import S_OK
from DIRAC.Core.Utilities.ClassAd import ClassAd
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor import OptimizerExecutor


class CheckingToStagingTransitioner(OptimizerExecutor):
    """Transition from checking to staging"""

    @classmethod
    def initializeOptimizer(cls):
        """Initialization of the optimizer."""
        return S_OK()

    def optimizeJob(self, jid, jobState: JobState):
        """Check if there are files to stage in the job description, and if so
        sent a stage request"""

        result = jobState.getManifest()
        if not result["OK"]:
            return result
        jobDescription = result["Value"]
        jobDescription = ClassAd()

        if not jobDescription.lookupAttribute("StageLFNs"):
            # No file to stage, goto next transitioner
            return self.setNextOptimizer(jobState)

        stageLFNs = jobDescription.getAttributeSubsection("StageLFNs")

        stageLFNsDict = {}
        for key in stageLFNs:
            stageLFNsDict[key] = stageLFNs.getListFromExpression(key)

        # Send the actual staging request
        result = StorageManagerClient().setRequest(
            stageLFNsDict,
            "WorkloadManagement",
            "updateJobFromStager@WorkloadManagement/JobStateUpdate",
            int(jobState.jid),
        )
        if not result["OK"]:
            self.jobLog.error("Could not send stage request", result["Message"])
            return result

        # Remove the StageLFNs section of the manifest, and update the manifest in db
        # TODO: not mandatory, but should we do it ?
        jobDescription.deleteAttribute("StageLFNs")
        jobState.setManifest(jobDescription)

        # Store the request id (TODO: Necessary ?)
        rid = result["Value"]
        self.jobLog.info("Stage request sent", rid)
        self.storeOptimizerParam("StageRequest", rid)

        # Set the job status to staging
        return jobState.setStatus(
            JobStatus.STAGING,
            self.ex_getOption("StagingMinorStatus", "Request Sent"),
            appStatus="",
            source=self.ex_optimizerName(),
        )
