# pylint: disable=import-error
from diracx.client import Dirac
from diracx.client.models import JobSearchParams

from diracx.cli.utils import get_auth_headers
from diracx.core.preferences import DiracxPreferences

from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue


class JobMonitoringClient:
    def __init__(self, *args, **kwargs):
        self.endpoint = DiracxPreferences().url

    def fetch(self, parameters, jobIDs):
        with Dirac(endpoint=self.endpoint) as api:
            jobs = api.jobs.search(
                parameters=["JobID"] + parameters,
                search=[{"parameter": "JobID", "operator": "in", "values": jobIDs}],
                headers=get_auth_headers(),
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
