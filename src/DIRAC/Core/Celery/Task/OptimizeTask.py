"""Celery task to treat the job from RECEIVED to WAITING or STAGING"""

from DIRAC.Core.Celery.CeleryApp import celeryApp
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.OptimizerAdministrator.OptimizerAdministrator import OptimizerAdministrator


@celeryApp.task(retry_backoff=True, retry_backoff_max=600)
def optimize(self, jobState: JobState):
    """Treat the job from RECEIVED to WAITING or STAGING"""
    try:
        result = OptimizerAdministrator(jobState).optimize()
        if not result["OK"]:
            pass
    except ValueError as exc:
        # Get job type
        result = self.jobState.getAttribute("JobType")
        if not result["OK"]:
            self.__log.error("Could not retrieve job type", result["Message"])
            return result
        jobType = result["Value"]

        if jobType not in self.__operations.getValue("ExcludedOnHoldJobTypes", []):
            raise self.retry(exc=exc)
