"""
This class contains all the async treatments that are done in the WMS after
the job is submitted and before it's sent to the TQ.
"""

from celery import Task

from DIRAC import gLogger, S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Celery.CeleryApp import celery
from DIRAC.Core.Utilities.Proxy import executeWithUserProxy
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient
from DIRAC.WorkloadManagementSystem.Utilities.JobModel import JobDescriptionModel


@celery.task(bind=True, name="resolveInputSandbox")
def resolveInputSandbox(self: Task, jsonJobDescription: str):
    """
    Assign SB:sandboxes to the job and check LFN sandboxes
    """
    jobDescription = JobDescriptionModel.parse_raw(jsonJobDescription)
    jobId = int(self.request.id)
    log = gLogger.getSubLogger(f"AssignSandboxesToJob/{jobId}")

    sbsToAssign = []
    lfns = set()
    for isb in jobDescription.inputSandbox:
        if isb.startswith("SB:"):
            sbsToAssign.append((isb, "Input"))
        elif isb.startswith("LFN:"):
            lfns.add(isb.lstrip("LFN:"))

    # We assign the SB: sandboxes to the job
    if sbsToAssign:
        res = SandboxStoreClient().assignSandboxesToJob(
            jobId, sbsToAssign, jobDescription.owner, jobDescription.ownerGroup
        )
        if not res["OK"]:
            log.error("Could not assign sandboxes in the SandboxStore")
            # We set the jobStatus to Failed
            JobStateUpdateClient().setJobStatus(
                jobId, JobStatus.FAILED, "Could not assign sandboxes in the SandboxStore"
            )
            return

        if res["Value"] != len(sbsToAssign):
            log.error("Could not assign sandboxes in the SandboxStore")
            # We set the jobStatus to Failed
            JobStateUpdateClient().setJobStatus(
                jobId, JobStatus.FAILED, "Could not assign all sandboxes in the SandboxStore"
            )
            return

    # Check if the input sandbox files that start with LFN:/ are available if it's not a transformation job
    if lfns and jobDescription.jobType not in Operations().getValue("Transformations/DataProcessing", []):
        # This will return already active replicas, excluding banned SEs, and
        # removing tape replicas if there are disk replicas
        res = DataManager(vo=jobDescription.vo).getReplicasForJobs(lfns)
        if not res["OK"]:
            log.error(res["Message"])
            JobStateUpdateClient().setJobStatus(jobId, JobStatus.FAILED, "No replica Info available")
            return

        if Operations().getValue("CheckWithUserProxy", False):
            res = checkReplicas(  # pylint: disable=unexpected-keyword-arg
                jobDescription,
                lfns,
                proxyUserName=jobDescription.owner,
                proxyUserGroup=jobDescription.ownerGroup,
                executionLock=True,
            )
        else:
            res = checkReplicas(jobDescription, lfns)

        if not res["OK"]:
            log.error("Could not resolve input sandbox", res["Message"])
            JobStateUpdateClient().setJobStatus(jobId, JobStatus.FAILED, "Could not resolve input sandbox")
            return

    # Go to next celery task
    nextTask = Operations().getValue(f"{self.name}/NextTask", "resolveInputData")
    celery.send_task(nextTask, task_id=str(jobId), args=[jsonJobDescription])


@executeWithUserProxy
def checkReplicas(jobDefinition: JobDescriptionModel, lfns: set[str]):
    """This method checks the file catalog for replica information.

    :param JobState jobState: JobState object
    :param list inputSandbox: list of LFNs for the input sandbox

    :returns: S_OK/S_ERROR structure with resolved input data info
    """

    # This will return already active replicas, excluding banned SEs, and
    # removing tape replicas if there are disk replicas

    result = DataManager(jobDefinition.vo).getReplicasForJobs(lfns)
    if not result["OK"]:
        return result

    replicaDict = result["Value"]

    badLFNs = []

    if "Successful" not in replicaDict:
        return S_ERROR("No replica Info available")

    okReplicas = replicaDict["Successful"]
    for lfn in okReplicas:
        if not okReplicas[lfn]:
            badLFNs.append(f"LFN:{lfn} -> No replicas available")

    if "Failed" in replicaDict:
        errorReplicas = replicaDict["Failed"]
        for lfn in errorReplicas:
            badLFNs.append(f"LFN:{lfn} -> {errorReplicas[lfn]}")

    if badLFNs:
        errorMsg = "\n".join(badLFNs)
        return S_ERROR(f"Some lfn input sandbox files are not available: {errorMsg}")

    return S_OK(okReplicas)
