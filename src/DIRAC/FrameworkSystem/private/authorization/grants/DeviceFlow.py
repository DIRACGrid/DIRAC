from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import time
import requests
from authlib.oauth2 import OAuth2Error
from authlib.oauth2.rfc6749.grants import AuthorizationEndpointMixin
from authlib.oauth2.rfc6749.errors import InvalidClientError, UnauthorizedClientError
from authlib.oauth2.rfc8628 import (
    DeviceAuthorizationEndpoint as _DeviceAuthorizationEndpoint,
    DeviceCodeGrant as _DeviceCodeGrant,
    DeviceCredentialDict
)

from DIRAC import gLogger, S_OK, S_ERROR

log = gLogger.getSubLogger(__name__)


def submitUserAuthorizationFlow(client=None, idP=None, group=None):
  """ Submit authorization flow
  """
  # TODO: Fix hardcore url
  issuer = 'https://marosvn32.in2p3.fr/DIRAC/auth'

  try:
    # TODO: ask public client in REST API of the configuration
    if not client:
      # Prepare client
      r = requests.get('%s/clientsinfo' % issuer, verify=False)
      r.raise_for_status()
      client = r.json().get("CLI")

    url = '%s/device?client_id=%s' % (issuer, client['client_id'])
    if group:
      url += '&scope=g:%s' % group
    if idP:
      url += '&provider=%s' % idP

    r = requests.post(url, verify=False)
    r.raise_for_status()
    authFlowData = r.json()

    # Check if all main keys are present here
    for k in ['user_code', 'device_code', 'verification_uri']:
      if not authFlowData.get(k):
        return S_ERROR('Mandatory %s key is absent in authentication response.' % k)

    authFlowData['client_id'] = client['client_id']
    return S_OK(authFlowData)
  except requests.exceptions.Timeout:
    return S_ERROR('Authentication server is not answer, timeout.')
  except requests.exceptions.RequestException as ex:
    return S_ERROR(r.content or repr(ex))
  except Exception as ex:
    return S_ERROR('Cannot read authentication response: %s' % repr(ex))


def waitFinalStatusOfUserAuthorizationFlow(authFlowData, timeout=300):
  """ Submit waiting loop process, that will monitor current authorization session status

      :param dict authFlowData: authentication flow parameters
      :param int timeout: max time of waiting

      :return: S_OK(dict)/S_ERROR() - dictionary contain access/refresh token and some metadata
  """
  __start = time.time()

  # TODO: Fix hardcore url
  issuer = 'https://marosvn32.in2p3.fr/DIRAC/auth'

  url = '%s/token?client_id=%s' % (issuer, authFlowData['client_id'])
  url += '&grant_type=urn:ietf:params:oauth:grant-type:device_code&device_code=%s' % authFlowData['device_code']
  while True:
    time.sleep(authFlowData.get('interval', 5))
    if time.time() - __start > timeout:
      return S_ERROR('Time out.')
    r = requests.post(url, verify=False)
    token = r.json()
    if not token:
      return S_ERROR('Resived token is empty!')
    if 'error' not in token:
      os.environ['DIRAC_TOKEN'] = r.text
      return S_OK(token)
    if token['error'] != 'authorization_pending':
      return S_ERROR(token['error'] + ' : ' + token.get('description', ''))


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
    log.debug('Validate authorization request of', client_id)
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
    data['expires_at'] = data['expires_in'] + int(time.time())
    data['interval'] = 5
    # TODO: Fix hardcore url
    data['verification_uri'] = 'https://marosvn32.in2p3.fr/DIRAC/auth/device'
    return DeviceCredentialDict(data)

  def query_user_grant(self, user_code):
    _, data = self.server.getSessionByOption('user_code', user_code)
    return (data['userID'], True) if data.get('username') else None

  def should_slow_down(self, credential, now):
    return False
