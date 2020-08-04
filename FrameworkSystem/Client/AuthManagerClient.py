""" DIRAC AuthManager Client class encapsulates the methods exposed
    by the AuthManager service.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.Core.Utilities import DIRACSingleton
from DIRAC.FrameworkSystem.Client.AuthManagerData import gAuthManagerData

__RCSID__ = "$Id$"


@createClient('Framework/AuthManager')
@six.add_metaclass(DIRACSingleton.DIRACSingleton)
class AuthManagerClient(Client):
  """ Authentication manager
  """

  def __init__(self, **kwargs):
    """ Constructor
    """
    super(AuthManagerClient, self).__init__(**kwargs)
    self.setServer('Framework/AuthManager')

  def parseAuthResponse(self, response, state):
    """ Fill session by user profile, tokens, comment, OIDC authorize status, etc.
        Prepare dict with user parameters, if DN is absent there try to get it.
        Create new or modify existend DIRAC user and store the session

        :param dict response: authorization response
        :param basestring state: session number

        :return: S_OK(dict)/S_ERROR()
    """
    result = self._getRPC().parseAuthResponse(response, state)
    if result['OK'] and result['Value']['Status'] in ['authed', 'redirect']:
      gAuthManagerData.updateProfiles(result['Value']['upProfile'])
      gAuthManagerData.updateSessions(result['Value']['upSession'])

    return result


gSessionManager = AuthManagerClient()
