""" Class that contains client access to the Publisher handler. """

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client


class PublisherClient(Client):
    def __init__(self, **kwargs):

        super(PublisherClient, self).__init__(**kwargs)
        self.setServer("ResourceStatus/Publisher")
