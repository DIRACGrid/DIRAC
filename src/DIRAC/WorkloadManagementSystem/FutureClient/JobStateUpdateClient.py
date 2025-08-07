from __future__ import annotations

import functools
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from DIRAC.Core.Security.DiracX import DiracXClient, FutureClient, addRPCStub
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue, returnValueOrRaise
from DIRAC.Core.Utilities.TimeUtilities import fromString
from DIRAC.WorkloadManagementSystem.Client import JobStatus

if TYPE_CHECKING:
    from diracx.client.models import JobCommand


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
            if result.get("Value") is None:
                result.pop("Value")
        return result

    return wrapper


class JobStateUpdateClient(FutureClient):
    @convertToReturnValue
    def sendHeartBeat(self, jobID: str | int, dynamicData: dict, staticData: dict):
        """Send a heartbeat from a Job.

        The behaviour of this function is not strictly the same as in legacy
        DIRAC. Most notably, in legacy DIRAC the heartbeat always overrides the
        job status to Running whereas in DiracX the job state machine is still
        respected. Additionally, DiracX updates the job logging information
        when status transitions occur as a result of the heartbeat.
        """

        with DiracXClient() as api:
            body = {jobID: dynamicData | staticData}
            if len(body[jobID]) != len(dynamicData) + len(staticData):
                raise NotImplementedError(f"Duplicate keys: {dynamicData=} {staticData=}")
            commands: list[JobCommand] = api.jobs.add_heartbeat(body)
        # Legacy DIRAC returns a dictionary of {command: arguments}
        result = {}
        for command in commands:
            if command.job_id != jobID:
                raise NotImplementedError(f"Job ID mismatch: {jobID=} {command.job_id=}")
            result[command.command] = command.arguments
        return result

    @stripValueIfOK
    @convertToReturnValue
    def setJobApplicationStatus(self, jobID: str | int, appStatus: str, source: str = "Unknown"):
        statusDict = {
            "ApplicationStatus": appStatus,
        }
        if source:
            statusDict["Source"] = source
        with DiracXClient() as api:
            api.jobs.set_job_statuses(
                {jobID: {datetime.now(tz=timezone.utc): statusDict}},
            )

    @stripValueIfOK
    @convertToReturnValue
    def setJobAttribute(self, jobID: str | int, attribute: str, value: str):
        with DiracXClient() as api:
            if attribute == "Status":
                return api.jobs.set_job_statuses(
                    {jobID: {datetime.now(tz=timezone.utc): {"Status": value}}},
                )
            else:
                return api.jobs.patch_metadata({jobID: {attribute: value}})

    @stripValueIfOK
    @convertToReturnValue
    def setJobParameter(self, jobID: str | int, name: str, value: str):
        with DiracXClient() as api:
            api.jobs.patch_metadata({jobID: {name: value}})

    @stripValueIfOK
    @convertToReturnValue
    def setJobParameters(self, jobID: str | int, parameters: list):
        with DiracXClient() as api:
            api.jobs.patch_metadata({jobID: {k: v for k, v in parameters}})

    @stripValueIfOK
    @convertToReturnValue
    def setJobSite(self, jobID: str | int, site: str):
        with DiracXClient() as api:
            api.jobs.patch_metadata({jobID: {"Site": site}})

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
            api.jobs.set_job_statuses(
                {jobID: {fromString(datetime_).replace(tzinfo=timezone.utc): statusDict}},
                force=force,
            )

    @addRPCStub
    @stripValueIfOK
    @convertToReturnValue
    def setJobStatusBulk(self, jobID: str | int, statusDict: dict, force=False):
        statusDict = {fromString(k).replace(tzinfo=timezone.utc): v for k, v in statusDict.items()}
        with DiracXClient() as api:
            api.jobs.set_job_statuses(
                {jobID: statusDict},
                force=force,
            )

    @stripValueIfOK
    @convertToReturnValue
    def setJobsParameter(self, jobsParameterDict: dict):
        with DiracXClient() as api:
            updates = {job_id: {k: v} for job_id, (k, v) in jobsParameterDict.items()}
            api.jobs.patch_metadata(updates)

    @stripValueIfOK
    @convertToReturnValue
    def updateJobFromStager(self, jobID: str | int, status: str):
        if status == "Done":
            jobStatus = JobStatus.CHECKING
            minorStatus = "JobScheduling"
        else:
            jobStatus = None
            minorStatus = "Staging input files failed"

        trials = 10
        query = [{"parameter": "JobID", "operator": "eq", "value": jobID}]
        with DiracXClient() as api:
            for i in range(trials):
                result = api.jobs.search(parameters=["Status"], search=query)
                if not result:
                    return None
                if result[0]["Status"] == JobStatus.STAGING:
                    break
                time.sleep(1)
            else:
                return f"Job is not in Staging after {trials} seconds"

            retVal = self.setJobStatus(jobID, status=jobStatus, minorStatus=minorStatus, source="StagerSystem")
            # As there might not be a value (see stripValueIfOK), only call
            # returnValueOrRaise if the return value is not OK
            if not retVal["OK"]:  # pylint: disable=unsubscriptable-object
                returnValueOrRaise(retVal)
            return None if i == 0 else f"Found job in Staging after {i} seconds"
