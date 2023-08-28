""" Class that contains client access to the Publisher handler. """

from DIRAC.Core.Base.Client import Client


class PublisherClient(Client):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.setServer("ResourceStatus/Publisher")
