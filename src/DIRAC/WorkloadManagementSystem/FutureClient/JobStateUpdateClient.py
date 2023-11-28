from DIRAC.Core.Security.DiracX import DiracXClient
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue


class JobStateUpdateClient:
    def sendHeartBeat(self, jobID: str | int, dynamicData: dict, staticData: dict):
        raise NotImplementedError("TODO")

    def setJobApplicationStatus(self, jobID: str | int, appStatus: str, source: str = "Unknown"):
        raise NotImplementedError("TODO")

    def setJobAttribute(self, jobID: str | int, attribute: str, value: str):
        with DiracXClient() as api:
            api.jobs.set_single_job_properties(jobID, "need to [patch the client to have a nice summer body ?")
        raise NotImplementedError("TODO")

    def setJobFlag(self, jobID: str | int, flag: str):
        raise NotImplementedError("TODO")

    def setJobParameter(self, jobID: str | int, name: str, value: str):
        raise NotImplementedError("TODO")

    def setJobParameters(self, jobID: str | int, parameters: list):
        raise NotImplementedError("TODO")

    def setJobSite(self, jobID: str | int, site: str):
        raise NotImplementedError("TODO")

    def setJobStatus(
        self,
        jobID: str | int,
        status: str = "",
        minorStatus: str = "",
        source: str = "Unknown",
        datetime=None,
        force=False,
    ):
        raise NotImplementedError("TODO")

    def setJobStatusBulk(self, jobID: str | int, statusDict: dict, force=False):
        raise NotImplementedError("TODO")

    def setJobsParameter(self, jobsParameterDict: dict):
        raise NotImplementedError("TODO")

    def unsetJobFlag(self, jobID: str | int, flag: str):
        raise NotImplementedError("TODO")

    def updateJobFromStager(self, jobID: str | int, status: str):
        raise NotImplementedError("TODO")
