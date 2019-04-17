""" DIRAC OAuth Client class encapsulates the methods exposed
    by the OAuth service.
"""

__RCSID__ = "$Id$"

from DIRAC import gLogger, S_ERROR
from DIRAC.Core.Base.Client import Client, createClient


@createClient('Framework/OAuth')
class OAuthClient(Client):

  def __init__(self, **kwargs):
    """ OAuth Client constructor
    """
    super(OAuthClient, self).__init__(**kwargs)

    self.log = gLogger.getSubLogger('OAuthClient')
    self.setServer('Framework/OAuth')
  