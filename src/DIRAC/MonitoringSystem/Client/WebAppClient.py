""" Module that contains client access to the WebApp handler.
"""

from DIRAC.Core.Base.Client import Client, createClient


@createClient("Monitoring/WebApp")
class WebAppClient(Client):
    """WebAppClient sets url for the WebAppHandler."""

    def __init__(self, url=None, **kwargs):
        """
        Sets URL for WebApp handler

        :param self: self reference
        :param url: url of the WebAppHandler
        :param kwargs: forwarded to the Base Client class
        """

        super().__init__(**kwargs)

        if not url:
            self.serverURL = "Monitoring/WebApp"

        else:
            self.serverURL = url
