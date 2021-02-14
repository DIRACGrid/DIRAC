from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from time import time
import functools
from contextlib import contextmanager

from authlib.jose import jwt
from authlib.oauth2 import OAuth2Error, ResourceProtector as _ResourceProtector
from authlib.oauth2.rfc6749 import MissingAuthorizationError, HttpRequest
from authlib.oauth2.rfc6750 import BearerTokenValidator as _BearerTokenValidator
from authlib.oauth2.rfc6749.wrappers import OAuth2Token as _OAuth2Token
from authlib.integrations.sqla_oauth2 import OAuth2TokenMixin
from authlib.oauth2.rfc6749.util import scope_to_list, list_to_scope

class OAuth2Token(_OAuth2Token, OAuth2TokenMixin):
  def __init__(self, params=None, **kwargs):
    kwargs.update(params or {})
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
    return scope_to_list(self.scope) or []
  
  @property
  def groups(self):
    return [s.split(':')[1] for s in self.scopes if s.startswith('g:')]


class ResourceProtector(_ResourceProtector):
  """ A protecting method for resource servers. """
  def __init__(self):
    self.validator = BearerTokenValidator()
    self._token_validators = {self.validator.TOKEN_TYPE: self.validator}

  def acquire_token(self, request, scope=None, operator='AND'):
    """ A method to acquire current valid token with the given scope.

        :param request: Tornado HTTP request instance
        :param scope: string or list of scope values
        :param operator: value of "AND" or "OR"

        :return: token object
    """
    req = HttpRequest(request.method, request.uri, request.body, request.headers)
    return self.validate_request(scope, req, operator if callable(operator) else operator.upper())


class BearerTokenValidator(_BearerTokenValidator):
  """ Token validator """
  def authenticate_token(self, token):
    """ A method to query token from database with the given token string.

        :param str token: A string to represent the access_token.
        
        :return: token
    """
    # Read public key of DIRAC auth service
    with open('/opt/dirac/etc/grid-security/jwtRS256.key.pub', 'rb') as f:
      key = f.read()

    # Get claims and verify signature
    claims = jwt.decode(token, key)
    
    # Verify token
    claims.validate()
    
    return OAuth2Token(claims, access_token=token)

  def request_invalid(self, request):
    """ Request validation

        :param object request: request

        :return: bool
    """
    return False

  def token_revoked(self, token):
    """ If token can be revoked

        :param object token: token

        :return: bool
    """
    return token.revoked
