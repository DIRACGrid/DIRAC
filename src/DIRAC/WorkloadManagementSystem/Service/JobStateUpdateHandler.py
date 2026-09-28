""" JobStateUpdateHandler is the implementation of the Job State updating
    service in the DISET framework

    The following methods are available in the Service interface

    setJobStatus()

"""

import time

from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Security import Properties
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Service.JobPolicy import RIGHT_CHANGE_STATUS, JobPolicy
from DIRAC.WorkloadManagementSystem.Utilities.JobStatusUtility import JobStatusUtility

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
        Determines the switching of OpenSearch and MySQL backends
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

        result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobParametersDB", "JobParametersDB")
        if not result["OK"]:
            return result
        cls.elasticJobParametersDB = result["Value"]()

        cls.jsu = JobStatusUtility(cls.jobDB, cls.jobLoggingDB)

        return S_OK()

    def initializeRequest(self):
        credDict = self.getRemoteCredentials()
        self.vo = credDict.get("VO", Registry.getVOForGroup(credDict["group"]))
        self.userProperties = credDict.get("properties", [])
        self.jobPolicy = JobPolicy(credDict.get("username", ""), credDict.get("group", ""))
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
                    infoStr = f"Found job in Staging after {i} seconds"
                break
            time.sleep(1)
        if status != JobStatus.STAGING:
            return S_OK(f"Job is not in Staging after {trials} seconds")

        result = self.jsu.setJobStatus(int(jobID), status=jobStatus, minorStatus=minorStatus, source="StagerSystem")
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
        return self.jsu.setJobStatus(
            int(jobID), status=status, minorStatus=minorStatus, source=source, dateTime=datetime, force=force
        )

    ###########################################################################
    types_setJobStatusBulk = [[str, int], dict]

    def export_setJobStatusBulk(self, jobID, statusDict, force=False):
        """Set various job status fields with a time stamp and a source"""
        result = self._checkJobAccess(jobID)
        if not result["OK"]:
            return result
        force = self._authorizeForce(force, jobID)
        return self.jsu.setJobStatusBulk(int(jobID), statusDict, force=force)

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
    types_setJobApplicationStatus = [[str, int], str, str]

    def export_setJobApplicationStatus(self, jobID, appStatus, source="Unknown"):
        """Set the application status for job specified by its JobId.
        Internally calling the bulk method
        """
        result = self._checkJobAccess(jobID)
        if not result["OK"]:
            return result
        return self.jsu.setJobStatus(jobID, appStatus=appStatus, source=source)

    ###########################################################################
    types_setJobParameter = [[str, int], str, str]

    def export_setJobParameter(self, jobID, name, value):
        """Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
        """
        result = self._checkJobAccess(jobID)
        if not result["OK"]:
            return result
        return self.elasticJobParametersDB.setJobParameter(int(jobID), name, value, vo=self.vo)

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
            res = self.elasticJobParametersDB.setJobParameter(
                int(jobID), key=str(jobsParameterDict[jobID][0]), value=str(jobsParameterDict[jobID][1]), vo=self.vo
            )
            if not res["OK"]:
                self.log.error("Failed to add Job Parameter to elasticJobParametersDB", res["Message"])
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
        result = self.elasticJobParametersDB.setJobParameters(int(jobID), parameters=parameters, vo=self.vo)
        if not result["OK"]:
            self.log.error("Failed to add Job Parameters to JobParametersDB", result["Message"])

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

        for key, value in staticData.items():
            result = self.elasticJobParametersDB.setJobParameter(int(jobID), key, value, vo=self.vo)
            if not result["OK"]:
                self.log.error("Failed to add Job Parameters to OpenSearch", result["Message"])

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
