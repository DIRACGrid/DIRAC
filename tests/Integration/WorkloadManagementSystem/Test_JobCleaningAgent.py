# pylint: disable=protected-access,wrong-import-position,missing-docstring

import time

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent import JobCleaningAgent
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus, JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobManagerClient import JobManagerClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.tests.Utilities.WMS import helloWorldJob


def test_removeDeletedJobs():
    """Test that JobCleaningAgent method to remove the jobs in status Deleted and Killed"""

    # Arrange
    job = helloWorldJob()
    job.setType("User")

    result = Dirac().submitJob(job)
    assert result["OK"], result["Message"]
    jobID = result["Value"]

    # Wait for the optimizers to run otherwise the status of the job could change back to WAITING or CHECKING
    startingTime = time.time()
    while time.time() - startingTime < 7 * 60:
	result = JobMonitoringClient().getJobsStates(jobID)
	assert result["OK"], result["Message"]
	if result["Value"][jobID]["Status"] in (JobStatus.WAITING, JobStatus.FAILED):
	    break
	time.sleep(1)
    print(f"Lookup time: {time.time() - startingTime}s")
    # Check if the optimizers ran correctly after rescheduling
    assert result["Value"][jobID]["Status"] == JobStatus.WAITING
    assert result["Value"][jobID]["MinorStatus"] == JobMinorStatus.PILOT_AGENT_SUBMISSION
    assert result["Value"][jobID]["ApplicationStatus"] == "Unknown"
    # Delete the job
    res = JobManagerClient().deleteJob(jobID)
    assert res["OK"], res["Message"]
    res = JobMonitoringClient().getJobsStatus([jobID])
    assert res["OK"], res["Message"]
    assert res["Value"][jobID]["Status"] == JobStatus.DELETED

    # Act
    jca = JobCleaningAgent("WorkloadManagement/JobCleaningAgent", "WorkloadManagement/JobCleaningAgent")
    jca.initialize()
    jca.removeDeletedJobs()

    # Assert
    res = JobMonitoringClient().getJobsStatus([jobID])
    assert res["OK"], res["Message"]
    assert res["Value"] == {}
