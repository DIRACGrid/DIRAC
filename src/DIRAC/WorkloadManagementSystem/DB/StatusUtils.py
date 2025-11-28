from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Service.JobPolicy import RIGHT_DELETE, RIGHT_KILL
from DIRAC.WorkloadManagementSystem.Utilities.jobAdministration import _filterJobStateTransition


def _deleteJob(jobID, force=False, *, jobdb, taskqueuedb, pilotagentsdb):
    """Set the job status to "Deleted"
    and remove the pilot that ran and its logging info if the pilot is finished.

    :param int jobID: job ID
    :return: S_OK()/S_ERROR()
    """
    if not (result := jobdb.setJobStatus(jobID, JobStatus.DELETED, "Checking accounting", force=force))["OK"]:
        gLogger.warn("Failed to set job Deleted status", result["Message"])
        return result

    if not (result := taskqueuedb.deleteJob(jobID))["OK"]:
        gLogger.warn("Failed to delete job from the TaskQueue")

    # if it was the last job for the pilot
    result = pilotagentsdb.getPilotsForJobID(jobID)
    if not result["OK"]:
        gLogger.error("Failed to get Pilots for JobID", result["Message"])
        return result
    for pilot in result["Value"]:
        res = pilotagentsdb.getJobsForPilot(pilot)
        if not res["OK"]:
            gLogger.error("Failed to get jobs for pilot", res["Message"])
            return res
        if not res["Value"]:  # if list of jobs for pilot is empty, delete pilot
            result = pilotagentsdb.getPilotInfo(pilotID=pilot)
            if not result["OK"]:
                gLogger.error("Failed to get pilot info", result["Message"])
                return result
            ret = pilotagentsdb.deletePilot(result["Value"]["PilotJobReference"])
            if not ret["OK"]:
                gLogger.error("Failed to delete pilot from PilotAgentsDB", ret["Message"])
                return ret

    return S_OK()


def _killJob(jobID, sendKillCommand=True, force=False, *, jobdb, taskqueuedb):
    """Kill one job

    :param int jobID: job ID
    :param bool sendKillCommand: send kill command

    :return: S_OK()/S_ERROR()
    """
    if sendKillCommand:
        if not (result := jobdb.setJobCommand(jobID, "Kill"))["OK"]:
            gLogger.warn("Failed to set job Kill command", result["Message"])
            return result

    gLogger.info("Job marked for termination", jobID)
    if not (result := jobdb.setJobStatus(jobID, JobStatus.KILLED, "Marked for termination", force=force))["OK"]:
        gLogger.warn("Failed to set job Killed status", result["Message"])
    if not (result := taskqueuedb.deleteJob(jobID))["OK"]:
        gLogger.warn("Failed to delete job from the TaskQueue", result["Message"])

    return S_OK()


def kill_delete_jobs(
    right,
    validJobList,
    nonauthJobList=[],
    force=False,
    *,
    jobdb=None,
    taskqueuedb=None,
    pilotagentsdb=None,
    storagemanagementdb=None,
):
    """Kill (== set the status to "KILLED") or delete (== set the status to "DELETED") jobs as necessary

    :param str right: RIGHT_KILL or RIGHT_DELETE

    :return: S_OK()/S_ERROR()
    """
    if jobdb is None:
        result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobDB", "JobDB")
        if not result["OK"]:
            return result
        jobdb = result["Value"]()
    if taskqueuedb is None:
        result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.TaskQueueDB", "TaskQueueDB")
        if not result["OK"]:
            return result
        taskqueuedb = result["Value"]()
    if pilotagentsdb is None:
        result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.PilotAgentsDB", "PilotAgentsDB")
        if not result["OK"]:
            return result
        pilotagentsdb = result["Value"]()
    if storagemanagementdb is None:
        result = ObjectLoader().loadObject("StorageManagementSystem.DB.StorageManagementDB", "StorageManagementDB")
        if not result["OK"]:
            return result
        storagemanagementdb = result["Value"]()

    badIDs = []

    killJobList = []
    deleteJobList = []
    if validJobList:
        result = jobdb.getJobsAttributes(validJobList, ["Status"])
        if not result["OK"]:
            return result
        jobStates = result["Value"]

        # Get the jobs allowed to transition to the Killed state
        killJobList.extend(_filterJobStateTransition(jobStates, JobStatus.KILLED))

        if right == RIGHT_DELETE:
            # Get the jobs allowed to transition to the Deleted state
            deleteJobList.extend(_filterJobStateTransition(jobStates, JobStatus.DELETED))

        for jobID in killJobList:
            result = _killJob(jobID, force=force, jobdb=jobdb, taskqueuedb=taskqueuedb)
            if not result["OK"]:
                badIDs.append(jobID)

        for jobID in deleteJobList:
            result = _deleteJob(jobID, force=force, jobdb=jobdb, taskqueuedb=taskqueuedb, pilotagentsdb=pilotagentsdb)
            if not result["OK"]:
                badIDs.append(jobID)

        # Look for jobs that are in the Staging state to send kill signal to the stager
        stagingJobList = [jobID for jobID, sDict in jobStates.items() if sDict["Status"] == JobStatus.STAGING]

        if stagingJobList:
            gLogger.info("Going to send killing signal to stager as well!")
            result = storagemanagementdb.killTasksBySourceTaskID(stagingJobList)
            if not result["OK"]:
                gLogger.warn("Failed to kill some Stager tasks", result["Message"])

    if nonauthJobList or badIDs:
        result = S_ERROR("Some jobs failed deletion")
        if nonauthJobList:
            gLogger.warn("Non-authorized JobIDs won't be deleted", str(nonauthJobList))
            result["NonauthorizedJobIDs"] = nonauthJobList
        if badIDs:
            gLogger.warn("JobIDs failed to be deleted", str(badIDs))
            result["FailedJobIDs"] = badIDs
        return result

    jobsList = killJobList if right == RIGHT_KILL else deleteJobList
    return S_OK(jobsList)
