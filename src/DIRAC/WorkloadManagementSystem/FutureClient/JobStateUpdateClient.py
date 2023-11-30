import functools
from datetime import datetime, timezone


from DIRAC.Core.Security.DiracX import DiracXClient
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue
from DIRAC.Core.Utilities.TimeUtilities import fromString


def stripValueIfOK(func):
    """Decorator to remove S_OK["Value"] from the return value of a function if it is OK.

    This is done as some update functions return the number of modified rows in
    the database. This likely not actually useful so it isn't supported in
    DiracX. Stripping the "Value" key of the dictionary means that we should
    get a fairly straight forward error if the assumption is incorrect.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if result.get("OK"):
            assert result.pop("Value") is None, "Value should be None if OK"
        return result

    return wrapper


class JobStateUpdateClient:
    @stripValueIfOK
    @convertToReturnValue
    def sendHeartBeat(self, jobID: str | int, dynamicData: dict, staticData: dict):
        print("HACK: This is a no-op until we decide what to do")

    @stripValueIfOK
    @convertToReturnValue
    def setJobApplicationStatus(self, jobID: str | int, appStatus: str, source: str = "Unknown"):
        statusDict = {
            "application_status": appStatus,
        }
        if source:
            statusDict["Source"] = source
        with DiracXClient() as api:
            api.jobs.set_single_job_status(
                jobID,
                {datetime.now(tz=timezone.utc): statusDict},
            )

    @stripValueIfOK
    @convertToReturnValue
    def setJobAttribute(self, jobID: str | int, attribute: str, value: str):
        with DiracXClient() as api:
            if attribute == "Status":
                api.jobs.set_single_job_status(
                    jobID,
                    {datetime.now(tz=timezone.utc): {"status": value}},
                )
            else:
                api.jobs.set_single_job_properties(jobID, {attribute: value})

    @stripValueIfOK
    @convertToReturnValue
    def setJobFlag(self, jobID: str | int, flag: str):
        with DiracXClient() as api:
            api.jobs.set_single_job_properties(jobID, {flag: True})

    @stripValueIfOK
    @convertToReturnValue
    def setJobParameter(self, jobID: str | int, name: str, value: str):
        print("HACK: This is a no-op until we decide what to do")

    @stripValueIfOK
    @convertToReturnValue
    def setJobParameters(self, jobID: str | int, parameters: list):
        print("HACK: This is a no-op until we decide what to do")

    @stripValueIfOK
    @convertToReturnValue
    def setJobSite(self, jobID: str | int, site: str):
        with DiracXClient() as api:
            api.jobs.set_single_job_properties(jobID, {"Site": site})

    @stripValueIfOK
    @convertToReturnValue
    def setJobStatus(
        self,
        jobID: str | int,
        status: str = "",
        minorStatus: str = "",
        source: str = "Unknown",
        datetime_=None,
        force=False,
    ):
        statusDict = {}
        if status:
            statusDict["Status"] = status
        if minorStatus:
            statusDict["MinorStatus"] = minorStatus
        if source:
            statusDict["Source"] = source
        if datetime_ is None:
            datetime_ = datetime.utcnow()
        with DiracXClient() as api:
            api.jobs.set_single_job_status(
                jobID,
                {fromString(datetime_).replace(tzinfo=timezone.utc): statusDict},
                force=force,
            )

    @stripValueIfOK
    @convertToReturnValue
    def setJobStatusBulk(self, jobID: str | int, statusDict: dict, force=False):
        statusDict = {fromString(k).replace(tzinfo=timezone.utc): v for k, v in statusDict.items()}
        with DiracXClient() as api:
            api.jobs.set_job_status_bulk(
                {jobID: statusDict},
                force=force,
            )

    @stripValueIfOK
    @convertToReturnValue
    def setJobsParameter(self, jobsParameterDict: dict):
        print("HACK: This is a no-op until we decide what to do")

    @stripValueIfOK
    @convertToReturnValue
    def unsetJobFlag(self, jobID: str | int, flag: str):
        with DiracXClient() as api:
            api.jobs.set_single_job_properties(jobID, {flag: False})

    def updateJobFromStager(self, jobID: str | int, status: str):
        raise NotImplementedError("TODO")
