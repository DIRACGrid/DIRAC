from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from time import time

from authlib.oauth2.rfc6749.util import scope_to_list
from authlib.oauth2.rfc6749.wrappers import OAuth2Token as _OAuth2Token
from authlib.integrations.sqla_oauth2 import OAuth2TokenMixin


class OAuth2Token(_OAuth2Token, OAuth2TokenMixin):
  """ Implementation a Token object """

  def __init__(self, params=None, **kwargs):
    kwargs.update(params or {})
    kwargs['revoked'] = False if kwargs.get('revoked', 'False') == 'False' else True
    self.sub = kwargs.get('sub')
    self.issuer = kwargs.get('iss')
    self.client_id = kwargs.get('client_id', kwargs.get('aud'))
    self.token_type = kwargs.get('token_type')
    self.access_token = kwargs.get('access_token')
    self.refresh_token = kwargs.get('refresh_token')
    self.scope = kwargs.get('scope')
    self.revoked = kwargs.get('revoked')
    self.issued_at = int(kwargs.get('issued_at', kwargs.get('iat', time())))
    self.expires_in = int(kwargs.get('expires_in', 0))
    self.expires_at = int(kwargs.get('expires_at', kwargs.get('exp', 0)))
    if not self.issued_at:
      raise Exception('Missing "iat" in token.')
    if not self.expires_at:
      if not self.expires_in:
        raise Exception('Cannot calculate token "expires_at".')
      self.expires_at = self.issued_at + self.expires_in
    if not self.expires_in:
      self.expires_in = self.expires_at - self.issued_at
    kwargs.update({'client_id': self.client_id,
                   'token_type': self.token_type,
                   'access_token': self.access_token,
                   'refresh_token': self.refresh_token,
                   'scope': self.scope,
                   'revoked': self.revoked,
                   'issued_at': self.issued_at,
                   'expires_in': self.expires_in,
                   'expires_at': self.expires_at})
    super(OAuth2Token, self).__init__(kwargs)

  @property
  def scopes(self):
    """ Get tokens scopes

        :return: list
    """
    return scope_to_list(self.scope) or []

  @property
  def groups(self):
    """ Get tokens groups

        :return: list
    """
    return [s.split(':')[1] for s in self.scopes if s.startswith('g:')]
