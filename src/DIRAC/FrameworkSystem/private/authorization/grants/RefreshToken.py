from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# from authlib.oauth2.rfc6749.util import scope_to_list
from authlib.oauth2.rfc6749.grants import RefreshTokenGrant as _RefreshTokenGrant

# from DIRAC.ConfigurationSystem.Client.Helpers import Registry
# from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
# from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import BearerTokenValidator


class RefreshTokenGrant(_RefreshTokenGrant):
  # def __init__(self, *args, **kwargs):
  #   super(RefreshTokenGrant, self).__init__(*args, **kwargs)
  #   self.validator = BearerTokenValidator()

  def authenticate_refresh_token(self, refresh_token):
    """ Get credential for token

        :param str refresh_token: refresh token

        :return: object
    """
    # Check auth session
    result = self.server.db.getTokenByRefreshToken(refresh_token)
    if not result['OK']:
      raise OAuth2Error('Cannot get token', result['Message'])
    return result['Value']
    # if not session:
    #   return None

    # # Check token
    # token = self.validator(refresh_token, self.request.scope, self.request, 'OR')

    # # # To special flow to change group
    # # if self.request.scope and 'changeGroup' in self.request.scope:
    # #   scopes = scope_to_list(self.request.scope)
    # #   reqGroups = [s.split(':')[1] for s in scopes if s.startswith('g:')]
    # #   if len(reqGroups) != 1 or not reqGroups[0]:
    # #     return None
    # #   group = reqGroups[0]
    # #   result = Registry.getUsernameForID(token['sub'])
    # #   if not result['OK']:
    # #     return None
    # #   result = gProxyManager.getGroupsStatusByUsername(result['Value'], group)
    # #   if not result['OK']:
    # #     return None
    # #   if result['Value'][group]['Status'] not in ['ready', 'unknown']:
    # #     return None
    # return self.validator(refresh_token, self.request.scope, self.request, 'OR')

  def authenticate_user(self, credential):
    """ Authorize user

        :param object credential: credential

        :return: str
    """
    return credential.sub

  def revoke_old_credential(self, credential):
    """ Remove old credential

        :param object credential: credential
    """
    self.server.db.removeToken(credential['refresh_token'])
