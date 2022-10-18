"""
IdProvider using oidc-agent set up in the running environment
"""

import subprocess

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Resources.IdProvider.IdProvider import IdProvider
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import OAuth2Token

class OidcAgentIdProvider(IdProvider):

  def __init__( self, **kwargs ):
    """Initialization"""
    super().__init__( **kwargs )

  def refreshToken(self, refreshToken=None, **kwargs):

    userName = kwargs.get("userName")
    group = kwargs.get("group")
    scope = kwargs.get("scope")
    audience = kwargs.get("audience")
    timeLeft = kwargs.get("requiredTimeLeft") or 3500

    cmd = f"oidc-token -t {timeLeft} {userName}"
    result = shellCall(30, cmd)
    if not result['OK']:
      return S_ERROR("Failed call to oidc-agent")
    if result['Value'][0] != 0:
      return S_ERROR(result["Value"][2])
    return S_OK(OAuth2Token(result["Value"][1]))
