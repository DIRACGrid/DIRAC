""" Class that contains client access to the JobManager handler. """

from DIRAC.Core.Base.Client import Client, createClient


@createClient("WorkloadManagement/JobManager")
class JobManagerClient(Client):
    """JobManagerClient sets url for the JobManagerHandler."""

    def __init__(self, url=None, **kwargs):
        """
        Sets URL for JobManager handler

        :param self: self reference
        :param url: url of the JobManagerHandler
        :param kwargs: forwarded to the Base Client class
        """

        super().__init__(**kwargs)

        if not url:
            self.serverURL = "WorkloadManagement/JobManager"

        else:
            self.serverURL = url
