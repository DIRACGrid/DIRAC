""" Class that contains client access to the JobStateUpdate handler. """

from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.WorkloadManagementSystem.FutureClient.JobStateUpdateClient import (
    JobStateUpdateClient as futureJobStateUpdateClient,
)


@createClient("WorkloadManagement/JobStateUpdate")
class JobStateUpdateClient(Client):
    """JobStateUpdateClient sets url for the JobStateUpdateHandler."""

    diracxClient = futureJobStateUpdateClient

    def __init__(self, url=None, **kwargs):
        """
        Sets URL for JobStateUpdate handler

        :param self: self reference
        :param url: url of the JobStateUpdateHandler
        :param kwargs: forwarded to the Base Client class
        """

        super().__init__(**kwargs)

        if not url:
            self.serverURL = "WorkloadManagement/JobStateUpdate"

        else:
            self.serverURL = url

    def setJobStatus(self, jobID, status="", minorStatus="", source="Unknown", datetime_=None, force=False, **kwargs):
        return self._getRPC(**kwargs).setJobStatus(jobID, status, minorStatus, source, datetime_, force)
