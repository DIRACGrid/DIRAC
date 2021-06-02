from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from authlib.oauth2.base import OAuth2Error
from authlib.oauth2.rfc6749.grants import RefreshTokenGrant as _RefreshTokenGrant


class RefreshTokenGrant(_RefreshTokenGrant):
  """ See :class:`authlib.oauth2.rfc6749.grants.RefreshTokenGrant` """

  DEFAULT_EXPIRES_AT = 12 * 3600

  def authenticate_refresh_token(self, refresh_token):
    """ Get credential for token

        :param str refresh_token: refresh token

        :return: dict or None
    """
    result = self.server.db.decryptRefreshToken({'refresh_token': refresh_token})
    if not result['OK']:
      raise OAuth2Error(result['Message'])
    return result['Value']

  def _validate_token_scope(self, token):
    """ Skip scope validadtion """
    pass

  def authenticate_user(self, credential):
    """ Authorize user """
    return True

  def issue_token(self, user, credential):
    """ Refresh tokens

        :param user: unuse
        :param dict credential: token credential

        :return: dict
    """
    result = self.server.idps.getIdProvider(credential['provider'])
    if result['OK']:
      result = result['Value'].refreshToken(credential['refresh_token'])
      if result['OK']:
        result = self.server.db.encryptRefreshToken(result['Value'], dict(provider=credential['provider'],
                                                                          client_id=credential['client_id'],
                                                                          expires_at=self.DEFAULT_EXPIRES_AT))
    if not result['OK']:
      raise OAuth2Error(result['Message'])
    return result['Value']

  def revoke_old_credential(self, credential):
    """ Remove old credential """
    pass
