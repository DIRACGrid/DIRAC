""" JobStateUpdateHandler is the implementation of the Job State updating
    service in the DISET framework

    The following methods are available in the Service interface

    setJobStatus()

"""
import time

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import Time
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
            cls.jobDB = result["Value"]()

            result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobLoggingDB", "JobLoggingDB")
            if not result["OK"]:
                return result
            cls.jobLoggingDB = result["Value"]()

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
                datetime = Time.toString()
            return cls.__setJobStatusBulk(jobID, {datetime: sDict}, force=force)
        return S_OK()

    ###########################################################################
    types_setJobStatusBulk = [[str, int], dict]

    @classmethod
    def export_setJobStatusBulk(cls, jobID, statusDict, force=False):
        """Set various job status fields with a time stamp and a source"""
        return cls.__setJobStatusBulk(jobID, statusDict, force=force)

    @classmethod
    def __setJobStatusBulk(cls, jobID, statusDict, force=False):
        """Set various status fields for job specified by its JobId.
        Set only the last status in the JobDB, updating all the status
        logging information in the JobLoggingDB. The statusDict has datetime
        as a key and status information dictionary as values
        """
        status = ""
        minor = ""
        application = ""
        jobID = int(jobID)

        result = cls.jobDB.getJobAttributes(jobID, ["Status", "StartExecTime", "EndExecTime"])
        if not result["OK"]:
            return result

        if not result["Value"]:
            # if there is no matching Job it returns an empty dictionary
            return S_ERROR("No Matching Job")
        # If the current status is Stalled and we get an update, it should probably be "Running"
        currentStatus = result["Value"]["Status"]
        if currentStatus == JobStatus.STALLED:
            status = JobStatus.RUNNING
        startTime = result["Value"].get("StartExecTime")
        endTime = result["Value"].get("EndExecTime")
        # getJobAttributes only returns strings :(
        if startTime == "None":
            startTime = None
        if endTime == "None":
            endTime = None

        # Get the latest WN time stamps of status updates
        result = cls.jobLoggingDB.getWMSTimeStamps(int(jobID))
        if not result["OK"]:
            return result
        lastTime = max([float(t) for s, t in result["Value"].items() if s != "LastTime"])
        lastTime = Time.toString(Time.fromEpoch(lastTime))

        dates = sorted(statusDict)
        # If real updates, start from the current status
        if dates[0] >= lastTime and not status:
            status = currentStatus
        log = cls.log.getLocalSubLogger("JobStatusBulk/Job-%s" % jobID)
        log.debug("*** New call ***", "Last update time %s - Sorted new times %s" % (lastTime, dates))
        # Remove useless items in order to make it simpler later, although there should not be any
        for sDict in statusDict.values():
            for item in sorted(sDict):
                if not sDict[item]:
                    sDict.pop(item, None)
        # Pick up start and end times from all updates, if they don't exist
        newStat = status
        for date in dates:
            sDict = statusDict[date]
            # This is to recover Matched jobs that set the application status: they are running!
            if sDict.get("ApplicationStatus") and newStat == JobStatus.MATCHED:
                sDict["Status"] = JobStatus.RUNNING
            newStat = sDict.get("Status", newStat)

            # evaluate the state machine
            if not force and newStat:
                res = JobStatus.JobsStateMachine(currentStatus).getNextState(newStat)
                if not res["OK"]:
                    return res
                nextState = res["Value"]

                # If the JobsStateMachine does not accept the candidate, don't update
                if newStat != nextState:
                    log.error(
                        "Job Status Error",
                        "%s can't move from %s to %s: using %s" % (jobID, currentStatus, newStat, nextState),
                    )
                    newStat = nextState
                sDict["Status"] = newStat
                currentStatus = newStat

            if newStat == JobStatus.RUNNING and not startTime:
                # Pick up the start date when the job starts running if not existing
                startTime = date
                log.debug("Set job start time", startTime)
            elif newStat in JobStatus.JOB_FINAL_STATES and not endTime:
                # Pick up the end time when the job is in a final status
                endTime = date
                log.debug("Set job end time", endTime)

        # We should only update the status if its time stamp is more recent than the last update
        if dates[-1] >= lastTime:
            # Get the last status values
            for date in [dt for dt in dates if dt >= lastTime]:
                sDict = statusDict[date]
                log.debug("\t", "Time %s - Statuses %s" % (date, str(sDict)))
                status = sDict.get("Status", status)
                minor = sDict.get("MinorStatus", minor)
                application = sDict.get("ApplicationStatus", application)

            log.debug("Final statuses:", "status '%s', minor '%s', application '%s'" % (status, minor, application))
            attrNames = []
            attrValues = []
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
        for date in dates:
            sDict = statusDict[date]
            status = sDict.get("Status", "idem")
            minor = sDict.get("MinorStatus", "idem")
            application = sDict.get("ApplicationStatus", "idem")
            source = sDict.get("Source", "Unknown")
            result = cls.jobLoggingDB.addLoggingRecord(
                jobID, status=status, minorStatus=minor, applicationStatus=application, date=date, source=source
            )
            if not result["OK"]:
                return result

        return S_OK()

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
                    jobID, str(jobsParameterDict[jobID][0]), str(jobsParameterDict[jobID][1])
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
            result = cls.elasticJobParametersDB.setJobParameters(jobID, parameters)
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
            cls.log.warn("Failed to set the heart beat data", "for job %d " % int(jobID))

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
            return S_ERROR("Job %d not found" % jobID)

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
            for key, _value in jobMessageDict.items():
                result = cls.jobDB.setJobCommandStatus(int(jobID), key, "Sent")

        return S_OK(jobMessageDict)


class JobStateUpdateHandler(JobStateUpdateHandlerMixin, RequestHandler):
    pass
