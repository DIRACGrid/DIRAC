""" JobStateUpdateHandler is the implementation of the Job State updating
    service in the DISET framework

    The following methods are available in the Service interface

    setJobStatus()

"""
import time
import datetime as dateTime

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import TimeUtilities
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.Client import JobStatus


class JobStateUpdateHandlerMixin:
    @classmethod
    def initializeHandler(cls, svcInfoDict):
        """
        Determines the switching of ElasticSearch and MySQL backends
        """
        try:
            result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobDB", "JobDB")
            if not result["OK"]:
                return result
            cls.jobDB = result["Value"](parentLogger=cls.log)

            result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobLoggingDB", "JobLoggingDB")
            if not result["OK"]:
                return result
            cls.jobLoggingDB = result["Value"](parentLogger=cls.log)

        except RuntimeError as excp:
            return S_ERROR("Can't connect to DB: %s" % excp)

        cls.elasticJobParametersDB = None
        useESForJobParametersFlag = Operations().getValue("/Services/JobMonitoring/useESForJobParametersFlag", False)
        if useESForJobParametersFlag:
            try:
                result = ObjectLoader().loadObject(
                    "WorkloadManagementSystem.DB.ElasticJobParametersDB", "ElasticJobParametersDB"
                )
                if not result["OK"]:
                    return result
                cls.elasticJobParametersDB = result["Value"]()
            except RuntimeError as excp:
                return S_ERROR("Can't connect to DB: %s" % excp)
        return S_OK()

    ###########################################################################
    types_updateJobFromStager = [[str, int], str]

    @classmethod
    def export_updateJobFromStager(cls, jobID, status):
        """Simple call back method to be used by the stager."""
        if status == "Done":
            jobStatus = JobStatus.CHECKING
            minorStatus = "JobScheduling"
        else:
            jobStatus = None
            minorStatus = "Staging input files failed"

        infoStr = None
        trials = 10
        for i in range(trials):
            result = cls.jobDB.getJobAttributes(jobID, ["Status"])
            if not result["OK"]:
                return result
            if not result["Value"]:
                # if there is no matching Job it returns an empty dictionary
                return S_OK("No Matching Job")
            status = result["Value"]["Status"]
            if status == JobStatus.STAGING:
                if i:
                    infoStr = "Found job in Staging after %d seconds" % i
                break
            time.sleep(1)
        if status != JobStatus.STAGING:
            return S_OK("Job is not in Staging after %d seconds" % trials)

        result = cls.__setJobStatus(int(jobID), status=jobStatus, minorStatus=minorStatus, source="StagerSystem")
        if not result["OK"]:
            if result["Message"].find("does not exist") != -1:
                return S_OK()
        if infoStr:
            return S_OK(infoStr)
        return result

    ###########################################################################
    types_setJobStatus = [[str, int], str, str, str]

    @classmethod
    def export_setJobStatus(cls, jobID, status="", minorStatus="", source="Unknown", datetime=None, force=False):
        """
        Sets the major and minor status for job specified by its JobId.
        Sets optionally the status date and source component which sends the status information.
        The "force" flag will override the WMS state machine decision.
        """
        return cls.__setJobStatus(
            int(jobID), status=status, minorStatus=minorStatus, source=source, datetime=datetime, force=force
        )

    @classmethod
    def __setJobStatus(
        cls, jobID, status=None, minorStatus=None, appStatus=None, source=None, datetime=None, force=False
    ):
        """update the job provided statuses (major, minor and application)
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
            if not datetime:
                datetime = str(dateTime.datetime.utcnow())
            return cls._setJobStatusBulk(jobID, {datetime: sDict}, force=force)
        return S_OK()

    ###########################################################################
    types_setJobStatusBulk = [[str, int], dict]

    @classmethod
    def export_setJobStatusBulk(cls, jobID, statusDict, force=False):
        """Set various job status fields with a time stamp and a source"""
        return cls._setJobStatusBulk(jobID, statusDict, force=force)

    @classmethod
    def _setJobStatusBulk(cls, jobID, statusDict, force=False):
        """Set various status fields for job specified by its jobId.
        Set only the last status in the JobDB, updating all the status
        logging information in the JobLoggingDB. The statusDict has datetime
        as a key and status information dictionary as values
        """
        jobID = int(jobID)
        log = cls.log.getLocalSubLogger("JobStatusBulk/Job-%d" % jobID)

        result = cls.jobDB.getJobAttributes(jobID, ["Status", "StartExecTime", "EndExecTime"])
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
        result = cls.jobLoggingDB.getWMSTimeStamps(int(jobID))
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
        newStat = ""
        firstUpdate = TimeUtilities.toEpoch(TimeUtilities.fromString(updateTimes[0]))
        for ts, st in timeStamps:
            if firstUpdate >= ts:
                newStat = st
        # Pick up start and end times from all updates
        for updTime in updateTimes:
            sDict = statusDict[updTime]
            newStat = sDict.get("Status", newStat)

            if not startTime and newStat == JobStatus.RUNNING:
                # Pick up the start date when the job starts running if not existing
                startTime = updTime
                log.debug("Set job start time", startTime)
            elif not endTime and newStat in JobStatus.JOB_FINAL_STATES:
                # Pick up the end time when the job is in a final status
                endTime = updTime
                log.debug("Set job end time", endTime)

        # We should only update the status to the last one if its time stamp is more recent than the last update
        attrNames = []
        attrValues = []
        if updateTimes[-1] >= lastTime:
            minor = ""
            application = ""
            # Get the last status values looping on the most recent upupdateTimes in chronological order
            for updTime in [dt for dt in updateTimes if dt >= lastTime]:
                sDict = statusDict[updTime]
                log.debug("\t", f"Time {updTime} - Statuses {str(sDict)}")
                status = sDict.get("Status", currentStatus)
                # evaluate the state machine if the status is changing
                if not force and status != currentStatus:
                    res = JobStatus.JobsStateMachine(currentStatus).getNextState(status)
                    if not res["OK"]:
                        return res
                    newStat = res["Value"]
                    # If the JobsStateMachine does not accept the candidate, don't update
                    if newStat != status:
                        # keeping the same status
                        log.error(
                            "Job Status Error",
                            f"{jobID} can't move from {currentStatus} to {status}: using {newStat}",
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
            result = cls.jobDB.setJobAttributes(jobID, attrNames, attrValues, update=True, force=True)
            if not result["OK"]:
                return result
            if cls.elasticJobParametersDB:
                result = cls.elasticJobParametersDB.setJobParameter(int(jobID), "Status", status)
                if not result["OK"]:
                    return result
        # Update start and end time if needed
        if endTime:
            result = cls.jobDB.setEndExecTime(jobID, endTime)
            if not result["OK"]:
                return result
        if startTime:
            result = cls.jobDB.setStartExecTime(jobID, startTime)
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
            result = cls.jobLoggingDB.addLoggingRecord(
                jobID, status=status, minorStatus=minor, applicationStatus=application, date=updTime, source=source
            )
            if not result["OK"]:
                return result
            # If the update comes from a job, update the heart beat time stamp with this item's stamp
            if source.startswith("Job"):
                heartBeatTime = updTime
        if heartBeatTime is not None:
            result = cls.jobDB.setHeartBeatData(jobID, {"HeartBeatTime": heartBeatTime})
            if not result["OK"]:
                return result

        return S_OK((attrNames, attrValues))

    ###########################################################################
    types_setJobAttribute = [[str, int], str, str]

    @classmethod
    def export_setJobAttribute(cls, jobID, attribute, value):
        """Set a job attribute"""
        return cls.jobDB.setJobAttribute(int(jobID), attribute, value)

    ###########################################################################
    types_setJobSite = [[str, int], str]

    @classmethod
    def export_setJobSite(cls, jobID, site):
        """Allows the site attribute to be set for a job specified by its jobID."""
        return cls.jobDB.setJobAttribute(int(jobID), "Site", site)

    ###########################################################################
    types_setJobFlag = [[str, int], str]

    @classmethod
    def export_setJobFlag(cls, jobID, flag):
        """Set job flag for job with jobID"""
        return cls.jobDB.setJobAttribute(int(jobID), flag, "True")

    ###########################################################################
    types_unsetJobFlag = [[str, int], str]

    @classmethod
    def export_unsetJobFlag(cls, jobID, flag):
        """Unset job flag for job with jobID"""
        return cls.jobDB.setJobAttribute(int(jobID), flag, "False")

    ###########################################################################
    types_setJobApplicationStatus = [[str, int], str, str]

    @classmethod
    def export_setJobApplicationStatus(cls, jobID, appStatus, source="Unknown"):
        """Set the application status for job specified by its JobId.
        Internally calling the bulk method
        """
        return cls.__setJobStatus(jobID, appStatus=appStatus, source=source)

    ###########################################################################
    types_setJobParameter = [[str, int], str, str]

    @classmethod
    def export_setJobParameter(cls, jobID, name, value):
        """Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
        """

        if cls.elasticJobParametersDB:
            return cls.elasticJobParametersDB.setJobParameter(int(jobID), name, value)  # pylint: disable=no-member

        return cls.jobDB.setJobParameter(int(jobID), name, value)

    ###########################################################################
    types_setJobsParameter = [dict]

    @classmethod
    @ignoreEncodeWarning
    def export_setJobsParameter(cls, jobsParameterDict):
        """Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
        """
        failed = False

        for jobID in jobsParameterDict:

            if cls.elasticJobParametersDB:
                res = cls.elasticJobParametersDB.setJobParameter(
                    int(jobID), str(jobsParameterDict[jobID][0]), str(jobsParameterDict[jobID][1])
                )
                if not res["OK"]:
                    cls.log.error("Failed to add Job Parameter to elasticJobParametersDB", res["Message"])
                    failed = True
                    message = res["Message"]

            else:
                res = cls.jobDB.setJobParameter(
                    jobID, str(jobsParameterDict[jobID][0]), str(jobsParameterDict[jobID][1])
                )
                if not res["OK"]:
                    cls.log.error("Failed to add Job Parameter to MySQL", res["Message"])
                    failed = True
                    message = res["Message"]

        if failed:
            return S_ERROR(message)
        return S_OK()

    ###########################################################################
    types_setJobParameters = [[str, int], list]

    @classmethod
    @ignoreEncodeWarning
    def export_setJobParameters(cls, jobID, parameters):
        """Set arbitrary parameters specified by a list of name/value pairs
        for job specified by its JobId
        """
        if cls.elasticJobParametersDB:
            result = cls.elasticJobParametersDB.setJobParameters(int(jobID), parameters)
            if not result["OK"]:
                cls.log.error("Failed to add Job Parameters to ElasticJobParametersDB", result["Message"])
        else:
            result = cls.jobDB.setJobParameters(int(jobID), parameters)
            if not result["OK"]:
                cls.log.error("Failed to add Job Parameters to MySQL", result["Message"])

        return result

    ###########################################################################
    types_sendHeartBeat = [[str, int], dict, dict]

    @classmethod
    def export_sendHeartBeat(cls, jobID, dynamicData, staticData):
        """Send a heart beat sign of life for a job jobID"""

        result = cls.jobDB.setHeartBeatData(int(jobID), dynamicData)
        if not result["OK"]:
            cls.log.warn("Failed to set the heart beat data", f"for job {jobID} ")

        if cls.elasticJobParametersDB:
            for key, value in staticData.items():
                result = cls.elasticJobParametersDB.setJobParameter(int(jobID), key, value)
                if not result["OK"]:
                    cls.log.error("Failed to add Job Parameters to ElasticSearch", result["Message"])
        else:
            result = cls.jobDB.setJobParameters(int(jobID), list(staticData.items()))
            if not result["OK"]:
                cls.log.error("Failed to add Job Parameters to MySQL", result["Message"])

        # Restore the Running status if necessary
        result = cls.jobDB.getJobAttributes(jobID, ["Status"])
        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_ERROR(f"Job {jobID} not found")

        status = result["Value"]["Status"]
        if status in (JobStatus.STALLED, JobStatus.MATCHED):
            result = cls.jobDB.setJobAttribute(jobID=jobID, attrName="Status", attrValue=JobStatus.RUNNING, update=True)
            if not result["OK"]:
                cls.log.warn("Failed to restore the job status to Running")

        jobMessageDict = {}
        result = cls.jobDB.getJobCommand(int(jobID))
        if result["OK"]:
            jobMessageDict = result["Value"]

        if jobMessageDict:
            for key in jobMessageDict:
                result = cls.jobDB.setJobCommandStatus(int(jobID), key, "Sent")

        return S_OK(jobMessageDict)


class JobStateUpdateHandler(JobStateUpdateHandlerMixin, RequestHandler):
    pass
