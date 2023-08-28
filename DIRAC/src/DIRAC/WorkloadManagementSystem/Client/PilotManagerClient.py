""" Module that contains client access to the Pilots handler.
"""

from DIRAC.Core.Base.Client import Client, createClient


@createClient("WorkloadManagement/PilotManager")
class PilotManagerClient(Client):
    """PilotManagerClient sets url for the PilotManagerHandler."""

    def __init__(self, url=None, **kwargs):
        """
        Sets URL for PilotManager handler

        :param self: self reference
        :param url: url of the PilotManagerHandler
        :param kwargs: forwarded to the Base Client class
        """

        super().__init__(**kwargs)

        if not url:
            self.serverURL = "WorkloadManagement/PilotManager"

        else:
            self.serverURL = url
