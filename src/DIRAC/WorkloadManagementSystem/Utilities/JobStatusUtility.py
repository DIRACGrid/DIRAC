"""Utility to set the job status in the jobDB"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.Core.Utilities import TimeUtilities
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRACCommon.WorkloadManagementSystem.Utilities.JobStatusUtility import getStartAndEndTime, getNewStatus

if TYPE_CHECKING:
    from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
    from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB


class JobStatusUtility:
    def __init__(
        self,
        jobDB: JobDB = None,
        jobLoggingDB: JobLoggingDB = None,
    ) -> None:
        """
        :raises: RuntimeError, AttributeError
        """

        self.log = gLogger.getSubLogger(self.__class__.__name__)

        self.jobDB = jobDB
        self.jobLoggingDB = jobLoggingDB

        if not self.jobDB:
            try:
                result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobDB", "JobDB")
                if not result["OK"]:
                    raise AttributeError(result["Message"])
                self.jobDB = result["Value"](parentLogger=self.log)
            except RuntimeError:
                self.log.error("Can't connect to the jobDB")
                raise

        if not self.jobLoggingDB:
            try:
                result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobLoggingDB", "JobLoggingDB")
                if not result["OK"]:
                    raise AttributeError(result["Message"])
                self.jobLoggingDB = result["Value"](parentLogger=self.log)
            except RuntimeError:
                self.log.error("Can't connect to the JobLoggingDB")
                raise

    def setJobStatus(
        self, jobID: int, status=None, minorStatus=None, appStatus=None, source=None, dateTime=None, force=False
    ):
        """
        Update the job provided statuses (major, minor and application)
        If sets also the source and the time stamp (or current time)
        This method calls the bulk method internally
        """
        sDict = {}
        if status:
            sDict["Status"] = status
        if minorStatus:
            sDict["MinorStatus"] = minorStatus
        if appStatus:
            sDict["ApplicationStatus"] = appStatus
        if sDict:
            if source:
                sDict["Source"] = source
            if not dateTime:
                dateTime = str(datetime.utcnow())
            return self.setJobStatusBulk(jobID, {dateTime: sDict}, force=force)
        return S_OK()

    def setJobStatusBulk(self, jobID: int, statusDict: dict, force: bool = False):
        """Set various status fields for job specified by its jobId.
        Set only the last status in the JobDB, updating all the status
        logging information in the JobLoggingDB. The statusDict has dateTime
        as a key and status information dictionary as values
        """
        jobID = int(jobID)
        log = self.log.getLocalSubLogger("JobStatusBulk/Job-%d" % jobID)

        result = self.jobDB.getJobAttributes(jobID, ["Status", "StartExecTime", "EndExecTime"])
        if not result["OK"]:
            return result
        if not result["Value"]:
            # if there is no matching Job it returns an empty dictionary
            return S_ERROR("No Matching Job")

        # If the current status is Stalled and we get an update, it should probably be "Running"
        currentStatus = result["Value"]["Status"]
        if currentStatus == JobStatus.STALLED:
            currentStatus = JobStatus.RUNNING
        startTime = result["Value"].get("StartExecTime")
        endTime = result["Value"].get("EndExecTime")
        # getJobAttributes only returns strings :(
        if startTime == "None":
            startTime = None
        if endTime == "None":
            endTime = None

        # Remove useless items in order to make it simpler later, although there should not be any
        for sDict in statusDict.values():
            for item in sorted(sDict):
                if not sDict[item]:
                    sDict.pop(item, None)

        # Get the latest time stamps of major status updates
        result = self.jobLoggingDB.getWMSTimeStamps(int(jobID))
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR("No registered WMS timeStamps")
        # This is more precise than "LastTime". timeStamps is a sorted list of tuples...
        timeStamps = sorted((float(t), s) for s, t in result["Value"].items() if s != "LastTime")
        lastTime = TimeUtilities.toString(TimeUtilities.fromEpoch(timeStamps[-1][0]))

        # Get chronological order of new updates
        updateTimes = sorted(statusDict)
        log.debug("*** New call ***", f"Last update time {lastTime} - Sorted new times {updateTimes}")
        # Get the status (if any) at the time of the first update
        newStartTime, newEndTime = getStartAndEndTime(startTime, endTime, updateTimes, timeStamps, statusDict)

        # We should only update the status to the last one if its time stamp is more recent than the last update
        attrNames = []
        attrValues = []
        if updateTimes[-1] >= lastTime:
            res = getNewStatus(jobID, updateTimes, lastTime, statusDict, currentStatus, force, log)
            if not res["OK"]:
                return res
            status, minor, application = res["Value"]
            log.debug("Final statuses:", f"status '{status}', minor '{minor}', application '{application}'")
            if status:
                attrNames.append("Status")
                attrValues.append(status)
            if minor:
                attrNames.append("MinorStatus")
                attrValues.append(minor)
            if application:
                attrNames.append("ApplicationStatus")
                attrValues.append(application)
            # Here we are forcing the update as it's always updating to the last status
            result = self.jobDB.setJobAttributes(jobID, attrNames, attrValues, update=True, force=True)
            if not result["OK"]:
                return result

        # Update start and end time if needed
        if not endTime and newEndTime:
            log.debug("Set job end time", endTime)
            result = self.jobDB.setEndExecTime(jobID, endTime)
            if not result["OK"]:
                return result
        if not startTime and newStartTime:
            log.debug("Set job start time", startTime)
            result = self.jobDB.setStartExecTime(jobID, startTime)
            if not result["OK"]:
                return result

        # Update the JobLoggingDB records
        heartBeatTime = None
        for updTime in updateTimes:
            sDict = statusDict[updTime]
            status = sDict.get("Status", "idem")
            minor = sDict.get("MinorStatus", "idem")
            application = sDict.get("ApplicationStatus", "idem")
            source = sDict.get("Source", "Unknown")
            result = self.jobLoggingDB.addLoggingRecord(
                jobID, status=status, minorStatus=minor, applicationStatus=application, date=updTime, source=source
            )
            if not result["OK"]:
                return result
            # If the update comes from a job, update the heart beat time stamp with this item's stamp
            if source.startswith("Job"):
                heartBeatTime = updTime
        if heartBeatTime is not None:
            result = self.jobDB.setHeartBeatData(jobID, {"HeartBeatTime": heartBeatTime})
            if not result["OK"]:
                return result

        return S_OK((attrNames, attrValues))
