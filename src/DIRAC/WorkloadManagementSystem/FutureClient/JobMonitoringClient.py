from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue

from DIRAC.Core.Security.DiracX import DiracXClient


class JobMonitoringClient:
    def fetch(self, parameters, jobIDs):
        with DiracXClient() as api:
            jobs = api.jobs.search(
                parameters=["JobID"] + parameters,
                search=[{"parameter": "JobID", "operator": "in", "values": jobIDs}],
            )
            return {j["JobID"]: {param: j[param] for param in parameters} for j in jobs}

    @convertToReturnValue
    def getJobsMinorStatus(self, jobIDs):
        return self.fetch(["MinorStatus"], jobIDs)

    @convertToReturnValue
    def getJobsStates(self, jobIDs):
        return self.fetch(["Status", "MinorStatus", "ApplicationStatus"], jobIDs)

    @convertToReturnValue
    def getJobsSites(self, jobIDs):
        return self.fetch(["Site"], jobIDs)
