""" JobStateUpdateHandler is the implementation of the Job State updating
    service in the DISET framework

    The following methods are available in the Service interface

    setJobStatus()

"""
import time

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Utilities.JobStatusUtility import JobStatusUtility


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
        if Operations().getValue("/Services/JobMonitoring/useESForJobParametersFlag", False):
            try:
                result = ObjectLoader().loadObject(
                    "WorkloadManagementSystem.DB.ElasticJobParametersDB", "ElasticJobParametersDB"
                )
                if not result["OK"]:
                    return result
                cls.elasticJobParametersDB = result["Value"]()
            except RuntimeError as excp:
                return S_ERROR(f"Can't connect to DB: {excp}")

        cls.jsu = JobStatusUtility(cls.jobDB, cls.jobLoggingDB, cls.elasticJobParametersDB)

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
                    infoStr = f"Found job in Staging after {i} seconds"
                break
            time.sleep(1)
        if status != JobStatus.STAGING:
            return S_OK(f"Job is not in Staging after {trials} seconds")

        result = cls.jsu.setJobStatus(int(jobID), status=jobStatus, minorStatus=minorStatus, source="StagerSystem")
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
        return cls.jsu.setJobStatus(
            int(jobID), status=status, minorStatus=minorStatus, source=source, dateTime=datetime, force=force
        )

    ###########################################################################
    types_setJobStatusBulk = [[str, int], dict]

    @classmethod
    def export_setJobStatusBulk(cls, jobID, statusDict, force=False):
        """Set various job status fields with a time stamp and a source"""
        return cls.jsu.setJobStatusBulk(int(jobID), statusDict, force=force)

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
        return cls.jsu.setJobStatus(jobID, appStatus=appStatus, source=source)

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
