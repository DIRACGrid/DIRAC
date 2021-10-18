""" The TokenManagerClient is a class representing the client of the DIRAC TokenManager service.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client, createClient


@createClient("Framework/TokenManager")
class TokenManagerClient(Client):
    """Client exposing the TokenManager Service."""

    def __init__(self, **kwargs):
        super(TokenManagerClient, self).__init__(**kwargs)
        self.setServer("Framework/TokenManager")
