"""Transition from CHECKING to WAITING if STAGING is not required"""

from DIRAC.Core.Utilities.ReturnValues import S_OK
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus, JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import singleValueDefFields, multiValueDefFields
from DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor import OptimizerExecutor


class CheckingToWaitingTransitioner(OptimizerExecutor):
    """Transition from CHECKING to WAITING"""

    @classmethod
    def initializeOptimizer(cls):
        """Initialization of the optimizer."""
        return S_OK()

    def optimizeJob(self, jid, jobState: JobState):
        """Insert the jobState into the task queue and set the status to WAITING"""

        result = jobState.getManifest()
        if not result["OK"]:
            return result
        manifest = result["Value"]

        result = jobState.insertIntoTQ(manifest)
        if not result["OK"]:
            return result

        return jobState.setStatus(
            JobStatus.WAITING,
            minorStatus=JobMinorStatus.PILOT_AGENT_SUBMISSION,
            appStatus="Unknown",
            source=self.__class__.__name__,
        )
