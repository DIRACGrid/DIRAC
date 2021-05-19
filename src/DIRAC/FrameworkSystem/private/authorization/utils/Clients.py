from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import json
import time

from authlib.integrations.sqla_oauth2 import OAuth2ClientMixin
from authlib.oauth2.rfc6749.util import scope_to_list, list_to_scope

from DIRAC import gLogger

__RCSID__ = "$Id$"

DEFAULT_SCOPE = 'proxy g: lifetime:'

DEFAULT_CLIENTS = {
    'DIRACCLI': dict(
        ProviderType='DIRAC',
        client_id='DIRAC_CLI',
        response_types=['device'],
        grant_types=['urn:ietf:params:oauth:grant-type:device_code']
    ),
    'WebAppDIRAC': dict(
        ProviderType='DIRAC',
        token_endpoint_auth_method='client_secret_basic',
        response_types=['code'],
        grant_types=['authorization_code', 'refresh_token']
    )
}


class Client(OAuth2ClientMixin):
  def __init__(self, params):

    super(Client, self).__init__()
    client_metadata = params.get('client_metadata', params)
    client_metadata['scope'] = ' '.join([client_metadata.get('scope', ''), DEFAULT_SCOPE])
    if params.get('redirect_uri') and not client_metadata.get('redirect_uris'):
      client_metadata['redirect_uris'] = [params['redirect_uri']]
    self.client_id = params['client_id']
    self.client_secret = params.get('client_secret', '')
    self.client_id_issued_at = params.get('client_id_issued_at', int(time.time()))
    self.client_secret_expires_at = params.get('client_secret_expires_at', 0)
    if isinstance(client_metadata, dict):
      self._client_metadata = json.dumps(client_metadata)
    else:
      self._client_metadata = client_metadata

  def get_allowed_scope(self, scope):
    if not isinstance(scope, six.string_types):
      scope = list_to_scope(scope)
    allowed = scope_to_list(super(Client, self).get_allowed_scope(scope))
    for s in scope_to_list(scope):
      for def_scope in scope_to_list(DEFAULT_SCOPE):
        if s.startswith(def_scope) and s not in allowed:
          allowed.append(s)
    gLogger.debug('Try to allow "%s" scope:' % scope, allowed)
    return list_to_scope(list(set(allowed)))
