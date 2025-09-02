"""Stateless job status utility functions"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from DIRACCommon.Core.Utilities.ReturnValues import S_OK
from DIRACCommon.Core.Utilities.TimeUtilities import toEpoch, fromString
from DIRACCommon.WorkloadManagementSystem.Client.JobStatus import RUNNING, JOB_FINAL_STATES, JobsStateMachine


def getStartAndEndTime(startTime, endTime, updateTimes, timeStamps, statusDict):
    """Get start and end times from job status updates

    :param startTime: current start time
    :param endTime: current end time
    :param updateTimes: list of update times
    :param timeStamps: list of (timestamp, status) tuples
    :param statusDict: dictionary mapping update times to status dictionaries
    :return: tuple of (newStartTime, newEndTime)
    """
    newStat = ""
    firstUpdate = toEpoch(fromString(updateTimes[0]))
    for ts, st in timeStamps:
        if firstUpdate >= ts:
            newStat = st
    # Pick up start and end times from all updates
    for updTime in updateTimes:
        sDict = statusDict[updTime]
        newStat = sDict.get("Status", newStat)

        if not startTime and newStat == RUNNING:
            # Pick up the start date when the job starts running if not existing
            startTime = updTime
        elif not endTime and newStat in JOB_FINAL_STATES:
            # Pick up the end time when the job is in a final status
            endTime = updTime

    return startTime, endTime


def getNewStatus(
    jobID: int,
    updateTimes: list[datetime],
    lastTime: datetime,
    statusDict: dict[datetime, Any],
    currentStatus,
    force: bool,
    log,
):
    """Get new job status from status updates

    :param jobID: job ID
    :param updateTimes: list of update times
    :param lastTime: last update time
    :param statusDict: dictionary mapping update times to status dictionaries
    :param currentStatus: current job status
    :param force: whether to force status update without state machine validation
    :param log: logger object
    :return: S_OK((status, minor, application)) or S_ERROR
    """
    status = ""
    minor = ""
    application = ""
    # Get the last status values looping on the most recent upupdateTimes in chronological order
    for updTime in [dt for dt in updateTimes if dt >= lastTime]:
        sDict = statusDict[updTime]
        log.debug(f"\tTime {updTime} - Statuses {str(sDict)}")
        status = sDict.get("Status", currentStatus)
        # evaluate the state machine if the status is changing
        if not force and status != currentStatus:
            res = JobsStateMachine(currentStatus).getNextState(status)
            if not res["OK"]:
                return res
            newStat = res["Value"]
            # If the JobsStateMachine does not accept the candidate, don't update
            if newStat != status:
                # keeping the same status
                log.error(
                    f"Job Status Error: {jobID} can't move from {currentStatus} to {status}: using {newStat}",
                )
                status = newStat
                sDict["Status"] = newStat
                # Change the source to indicate this is not what was requested
                source = sDict.get("Source", "")
                sDict["Source"] = source + "(SM)"
            # at this stage status == newStat. Set currentStatus to this new status
            currentStatus = newStat

        minor = sDict.get("MinorStatus", minor)
        application = sDict.get("ApplicationStatus", application)
    return S_OK((status, minor, application))
