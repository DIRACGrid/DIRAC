from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from time import time
from authlib.oauth2 import OAuth2Error
from authlib.oauth2.rfc6749.grants import AuthorizationEndpointMixin
from authlib.oauth2.rfc6749.errors import InvalidClientError, UnauthorizedClientError
from authlib.oauth2.rfc8628 import (
    DeviceAuthorizationEndpoint as _DeviceAuthorizationEndpoint,
    DeviceCodeGrant as _DeviceCodeGrant,
    DeviceCredentialDict
)

from DIRAC import gLogger

log = gLogger.getSubLogger(__name__)


class DeviceAuthorizationEndpoint(_DeviceAuthorizationEndpoint):
  def create_endpoint_response(self, req):
    c, data, h = super(DeviceAuthorizationEndpoint, self).create_endpoint_response(req)
    req.query += '&response_type=device&state=%s' % data['device_code']
    self.server.updateSession(data['device_code'], request=req)  # , group=req.args.get('group'))
    return c, data, h

  def get_verification_uri(self):
    # TODO: Fix hardcore url
    return 'https://marosvn32.in2p3.fr/DIRAC/auth/device'

  def save_device_credential(self, client_id, scope, data):
    data['verification_uri_complete'] = '%s/%s' % (data['verification_uri'], data['user_code'])
    self.server.addSession(data['device_code'], client_id=client_id, scope=scope, **data)


class DeviceCodeGrant(_DeviceCodeGrant, AuthorizationEndpointMixin):
  RESPONSE_TYPES = {'device'}

  def validate_authorization_request(self):
    client_id = self.request.client_id
    log.debug('Validate authorization request of %r', client_id)
    if client_id is None:
      raise InvalidClientError(state=self.request.state)
    client = self.server.query_client(client_id)
    if not client:
      raise InvalidClientError(state=self.request.state)
    response_type = self.request.response_type
    if not client.check_response_type(response_type):
      raise UnauthorizedClientError('The client is not authorized to use '
                                    '"response_type={}"'.format(response_type))
    self.request.client = client
    self.validate_requested_scope()

    # Check user_code, when user go to authorization endpoint
    userCode = self.request.args.get('user_code')
    if not userCode:
      raise OAuth2Error('user_code is absent.')
    session, _ = self.server.getSessionByOption('user_code', userCode)
    from pprint import pprint
    pprint(self.server.getSessions())
    if not session:
      raise OAuth2Error('Session with %s user code is expired.' % userCode)
    self.execute_hook('after_validate_authorization_request')
    return None

  def create_authorization_response(self, redirect_uri, grant_user):
    return 200, 'Authorization complite.', set()

  def query_device_credential(self, device_code):
    _, data = self.server.getSessionByOption('device_code', device_code)
    if not data:
      return None
    data['expires_at'] = data['expires_in'] + int(time())
    data['interval'] = 5
    # TODO: Fix hardcore url
    data['verification_uri'] = 'https://marosvn32.in2p3.fr/DIRAC/auth/device'
    return DeviceCredentialDict(data)

  def query_user_grant(self, user_code):
    _, data = self.server.getSessionByOption('user_code', user_code)
    return (data['userID'], True) if data.get('username') else None

  def should_slow_down(self, credential, now):
    return False
