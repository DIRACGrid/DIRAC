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
    self.issuer = self.parameters["issuer"]
    self.scope = []

  def getToken(self, **kwargs):

    userName = kwargs.get("userName")
    userGroup = kwargs.get("group")
    scope = self.scope.extend(kwargs.get("scope", []))
    audience = kwargs.get("audience")
    timeLeft = kwargs.get("requiredTimeLeft") or 3500

    scopeStr = " ".join([f"-s {scopeItem}" for scopeItem in scope])

    cmd = f"oidc-token -t {timeLeft} {scopeStr} {userName}"
    result = shellCall(30, cmd)
    if not result['OK']:
      return S_ERROR("Failed call to oidc-agent")
    if result['Value'][0] != 0:
      return S_ERROR(result["Value"][2])
    return S_OK(OAuth2Token(result["Value"][1]))
