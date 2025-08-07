""" JobStateUpdateHandler is the implementation of the Job State updating
    service in the DISET framework

    The following methods are available in the Service interface

    setJobStatus()

"""

import time

from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Utilities.JobStatusUtility import JobStatusUtility


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
            result = cls.jobDB.getJobAttributes(int(jobID), ["Status"])
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
    types_setJobApplicationStatus = [[str, int], str, str]

    @classmethod
    def export_setJobApplicationStatus(cls, jobID, appStatus, source="Unknown"):
        """Set the application status for job specified by its JobId.
        Internally calling the bulk method
        """
        return cls.jsu.setJobStatus(jobID, appStatus=appStatus, source=source)

    ###########################################################################
    types_setJobParameter = [[str, int], str, str]

    def export_setJobParameter(self, jobID, name, value):
        """Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
        """
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

        for jobID in jobsParameterDict:
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
        result = self.elasticJobParametersDB.setJobParameters(int(jobID), parameters=parameters, vo=self.vo)
        if not result["OK"]:
            self.log.error("Failed to add Job Parameters to JobParametersDB", result["Message"])

        return result

    ###########################################################################
    types_sendHeartBeat = [[str, int], dict, dict]

    def export_sendHeartBeat(self, jobID, dynamicData, staticData):
        """Send a heart beat sign of life for a job jobID"""

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
