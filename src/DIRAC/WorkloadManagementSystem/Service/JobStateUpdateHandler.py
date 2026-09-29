""" JobStateUpdateHandler is the implementation of the Job State updating
    service in the DISET framework

    The following methods are available in the Service interface

    setJobStatus()

"""

import time
import datetime as dateTime

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Security import Properties
from DIRAC.Core.Utilities import TimeUtilities
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Service.JobPolicy import RIGHT_CHANGE_STATUS, JobPolicy

# Job attributes that may be modified through this service (default-deny: any other attribute,
# including a newly added Jobs column, is immutable here)
MUTABLE_JOB_ATTRIBUTES = frozenset(
    {
        "Status",
        "MinorStatus",
        "ApplicationStatus",
        "Site",
        "StartExecTime",
        "EndExecTime",
        "HeartBeatTime",
    }
)

# Callers holding any of these properties may act on any job
PRIVILEGED_JOB_PROPERTIES = frozenset({Properties.JOB_ADMINISTRATOR, Properties.TRUSTED_HOST, Properties.OPERATOR})


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
            return S_ERROR(f"Can't connect to DB: {excp}")

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
                return S_ERROR(f"Can't connect to DB: {excp}")
        return S_OK()

    def initializeRequest(self):
        credDict = self.getRemoteCredentials()
        self.userProperties = credDict.get("properties", [])
        self.jobPolicy = JobPolicy(credDict.get("DN", ""), credDict.get("group", ""))
        self.jobPolicy.jobDB = self.jobDB

    def _authorizedJobs(self, jobIDs):
        """Return the subset of ``jobIDs`` (as ``int``) whose state the caller may change,
        evaluating the rights of all the jobs in a single ``JobPolicy`` pass.
        """
        jobIDs = [int(jobID) for jobID in jobIDs]
        if PRIVILEGED_JOB_PROPERTIES.intersection(self.userProperties):
            return set(jobIDs)
        return set(self.jobPolicy.evaluateJobRights(jobIDs, RIGHT_CHANGE_STATUS)[0])

    def _checkJobAccess(self, jobID):
        """Check that the caller may change the state of the given job: it holds a privileged
        property or is granted ``RIGHT_CHANGE_STATUS`` by ``JobPolicy`` (owner, JobSharing member
        of the owner's group, pilot).
        """
        if int(jobID) not in self._authorizedJobs([jobID]):
            return S_ERROR(f"Not authorized to modify job {jobID}")
        return S_OK()

    def _authorizeForce(self, force, jobID):
        """The ``force`` flag bypasses the job state machine: it is only honoured for callers
        holding the JobAdministrator property, and ignored for any other caller.
        """
        if force and Properties.JOB_ADMINISTRATOR not in self.userProperties:
            self.log.warn("Ignoring 'force' flag from non-administrator caller", f"for job {jobID}")
            return False
        return force

    ###########################################################################
    types_updateJobFromStager = [[str, int], str]

    def export_updateJobFromStager(self, jobID, status):
        """Simple call back method to be used by the stager."""
        result = self._checkJobAccess(jobID)
        if not result["OK"]:
            return result
        if status == "Done":
            jobStatus = JobStatus.CHECKING
            minorStatus = "JobScheduling"
        else:
            jobStatus = None
            minorStatus = "Staging input files failed"

        infoStr = None
        trials = 10
        for i in range(trials):
            result = self.jobDB.getJobAttributes(int(jobID), ["Status"])
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

        result = self.__setJobStatus(int(jobID), status=jobStatus, minorStatus=minorStatus, source="StagerSystem")
        if not result["OK"]:
            if result["Message"].find("does not exist") != -1:
                return S_OK()
        if infoStr:
            return S_OK(infoStr)
        return result

    ###########################################################################
    types_setJobStatus = [[str, int], str, str, str]

    def export_setJobStatus(self, jobID, status="", minorStatus="", source="Unknown", datetime=None, force=False):
        """
        Sets the major and minor status for job specified by its JobId.
        Sets optionally the status date and source component which sends the status information.
        The "force" flag will override the WMS state machine decision (JobAdministrator only).
        """
        result = self._checkJobAccess(jobID)
        if not result["OK"]:
            return result
        force = self._authorizeForce(force, jobID)
        return self.__setJobStatus(
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

    def export_setJobStatusBulk(self, jobID, statusDict, force=False):
        """Set various job status fields with a time stamp and a source"""
        result = self._checkJobAccess(jobID)
        if not result["OK"]:
            return result
        force = self._authorizeForce(force, jobID)
        return self._setJobStatusBulk(jobID, statusDict, force=force)

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

    def export_setJobAttribute(self, jobID, attribute, value):
        """Set a job attribute. Only ``MUTABLE_JOB_ATTRIBUTES`` may be set."""
        if attribute not in MUTABLE_JOB_ATTRIBUTES:
            return S_ERROR(f"Job attribute '{attribute}' cannot be modified through JobStateUpdate")
        result = self._checkJobAccess(jobID)
        if not result["OK"]:
            return result
        return self.jobDB.setJobAttribute(int(jobID), attribute, value)

    ###########################################################################
    types_setJobSite = [[str, int], str]

    def export_setJobSite(self, jobID, site):
        """Allows the site attribute to be set for a job specified by its jobID."""
        result = self._checkJobAccess(jobID)
        if not result["OK"]:
            return result
        return self.jobDB.setJobAttribute(int(jobID), "Site", site)

    ###########################################################################
    types_setJobFlag = [[str, int], str]

    def export_setJobFlag(self, jobID, flag):
        """Set job flag for job with jobID. Only ``MUTABLE_JOB_ATTRIBUTES`` may be set."""
        if flag not in MUTABLE_JOB_ATTRIBUTES:
            return S_ERROR(f"Job flag '{flag}' cannot be modified through JobStateUpdate")
        result = self._checkJobAccess(jobID)
        if not result["OK"]:
            return result
        return self.jobDB.setJobAttribute(int(jobID), flag, "True")

    ###########################################################################
    types_unsetJobFlag = [[str, int], str]

    def export_unsetJobFlag(self, jobID, flag):
        """Unset job flag for job with jobID. Only ``MUTABLE_JOB_ATTRIBUTES`` may be unset."""
        if flag not in MUTABLE_JOB_ATTRIBUTES:
            return S_ERROR(f"Job flag '{flag}' cannot be modified through JobStateUpdate")
        result = self._checkJobAccess(jobID)
        if not result["OK"]:
            return result
        return self.jobDB.setJobAttribute(int(jobID), flag, "False")

    ###########################################################################
    types_setJobApplicationStatus = [[str, int], str, str]

    def export_setJobApplicationStatus(self, jobID, appStatus, source="Unknown"):
        """Set the application status for job specified by its JobId.
        Internally calling the bulk method
        """
        result = self._checkJobAccess(jobID)
        if not result["OK"]:
            return result
        return self.__setJobStatus(jobID, appStatus=appStatus, source=source)

    ###########################################################################
    types_setJobParameter = [[str, int], str, str]

    def export_setJobParameter(self, jobID, name, value):
        """Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
        """
        result = self._checkJobAccess(jobID)
        if not result["OK"]:
            return result

        if self.elasticJobParametersDB:
            return self.elasticJobParametersDB.setJobParameter(int(jobID), name, value)  # pylint: disable=no-member

        return self.jobDB.setJobParameter(int(jobID), name, value)

    ###########################################################################
    types_setJobsParameter = [dict]

    @ignoreEncodeWarning
    def export_setJobsParameter(self, jobsParameterDict):
        """Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
        """
        failed = False
        message = ""

        authorizedJobs = self._authorizedJobs(jobsParameterDict)
        for jobID in jobsParameterDict:
            if int(jobID) not in authorizedJobs:
                self.log.error("Not authorized to set job parameter", f"for job {jobID}")
                failed = True
                message = f"Not authorized to modify job {jobID}"
                continue
            if self.elasticJobParametersDB:
                res = self.elasticJobParametersDB.setJobParameter(
                    int(jobID), str(jobsParameterDict[jobID][0]), str(jobsParameterDict[jobID][1])
                )
                if not res["OK"]:
                    self.log.error("Failed to add Job Parameter to elasticJobParametersDB", res["Message"])
                    failed = True
                    message = res["Message"]

            else:
                res = self.jobDB.setJobParameter(
                    jobID, str(jobsParameterDict[jobID][0]), str(jobsParameterDict[jobID][1])
                )
                if not res["OK"]:
                    self.log.error("Failed to add Job Parameter to MySQL", res["Message"])
                    failed = True
                    message = res["Message"]

        if failed:
            return S_ERROR(message)
        return S_OK()

    ###########################################################################
    types_setJobParameters = [[str, int], list]

    @ignoreEncodeWarning
    def export_setJobParameters(self, jobID, parameters):
        """Set arbitrary parameters specified by a list of name/value pairs
        for job specified by its JobId
        """
        result = self._checkJobAccess(jobID)
        if not result["OK"]:
            return result
        if self.elasticJobParametersDB:
            result = self.elasticJobParametersDB.setJobParameters(int(jobID), parameters)
            if not result["OK"]:
                self.log.error("Failed to add Job Parameters to ElasticJobParametersDB", result["Message"])
        else:
            result = self.jobDB.setJobParameters(int(jobID), parameters)
            if not result["OK"]:
                self.log.error("Failed to add Job Parameters to MySQL", result["Message"])

        return result

    ###########################################################################
    types_sendHeartBeat = [[str, int], dict, dict]

    def export_sendHeartBeat(self, jobID, dynamicData, staticData):
        """Send a heart beat sign of life for a job jobID"""
        result = self._checkJobAccess(jobID)
        if not result["OK"]:
            return result

        result = self.jobDB.setHeartBeatData(int(jobID), dynamicData)
        if not result["OK"]:
            self.log.warn("Failed to set the heart beat data", f"for job {jobID} ")

        if self.elasticJobParametersDB:
            for key, value in staticData.items():
                result = self.elasticJobParametersDB.setJobParameter(int(jobID), key, value)
                if not result["OK"]:
                    self.log.error("Failed to add Job Parameters to ElasticSearch", result["Message"])
        else:
            result = self.jobDB.setJobParameters(int(jobID), list(staticData.items()))
            if not result["OK"]:
                self.log.error("Failed to add Job Parameters to MySQL", result["Message"])

        # Restore the Running status if necessary
        result = self.jobDB.getJobAttributes(jobID, ["Status"])
        if not result["OK"]:
            return result

        if not result["Value"]:
            return S_ERROR(f"Job {jobID} not found")

        status = result["Value"]["Status"]
        if status in (JobStatus.STALLED, JobStatus.MATCHED):
            result = self.jobDB.setJobAttribute(
                jobID=jobID, attrName="Status", attrValue=JobStatus.RUNNING, update=True
            )
            if not result["OK"]:
                self.log.warn("Failed to restore the job status to Running")

        jobMessageDict = {}
        result = self.jobDB.getJobCommand(int(jobID))
        if result["OK"]:
            jobMessageDict = result["Value"]

        if jobMessageDict:
            for key in jobMessageDict:
                result = self.jobDB.setJobCommandStatus(int(jobID), key, "Sent")

        return S_OK(jobMessageDict)


class JobStateUpdateHandler(JobStateUpdateHandlerMixin, RequestHandler):
    def initialize(self):
        return self.initializeRequest()
