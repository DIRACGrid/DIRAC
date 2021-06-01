from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from authlib.oauth2.base import OAuth2Error
from authlib.oauth2.rfc7009 import RevocationEndpoint as _RevocationEndpoint


class RevocationEndpoint(_RevocationEndpoint):
  """ See :class:`authlib.oauth2.rfc7009.RevocationEndpoint` """

  def query_token(self, token, token_type_hint, client):
    """ Query requested token from database.

        :param str token: token
        :param str token_type_hint: token type
        :param client: client

        :return: str
    """
    if token_type_hint == 'refresh_token':
      result = self.server.db.decryptRefreshToken({'refresh_token': token})
      if not result['OK']:
        raise OAuth2Error(result['Message'])
      return result['Value']
    return token

  def revoke_token(self, token):
    """ Mark the give token as revoked.

        :param dict token: token dict
    """
    if isinstance(token, dict):
      result = self.server.idps.getIdProvider(token['provider'])
    else:
      result = self.server.idps.getIdProviderForToken(token)
    if result['OK']:
      result = result['Value'].revokeToken(token)
    if not result['OK']:
      raise OAuth2Error(result['Message'])
