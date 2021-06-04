from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import json
import time
import pprint

from authlib.oauth2.rfc6749.util import scope_to_list, list_to_scope
from authlib.integrations.sqla_oauth2 import OAuth2ClientMixin
from DIRAC.Resources.IdProvider.Utilities import getProvidersForInstance, getProviderInfo

from DIRAC import gLogger

__RCSID__ = "$Id$"

DEFAULT_SCOPE = 'proxy g: lifetime:'

DEFAULT_CLIENTS = {
    'DIRACCLI': dict(
        verify=False,
        client_id='DIRAC_CLI',
        client_secret='secret',
        response_types=['device'],
        grant_types=['urn:ietf:params:oauth:grant-type:device_code', 'refresh_token'],
        ProviderType='DIRACCLI'
    ),
    'DIRACWeb': dict(
        response_types=['code'],
        grant_types=['authorization_code', 'refresh_token'],
        ProviderType='DIRACWeb'
    )
}


def getDIACClientByID(clientID):
  """ Search authorization client

      :param str clientID: client ID

      :return: object or None
  """
  gLogger.debug('Try to query %s client' % clientID)
  if clientID == DEFAULT_CLIENTS['DIRACCLI']['client_id']:
    gLogger.debug('Found client:\n', pprint.pformat(DEFAULT_CLIENTS['DIRACCLI']))
    return Client(DEFAULT_CLIENTS['DIRACCLI'])

  result = getProvidersForInstance('Id')
  if not result['OK']:
    gLogger.error(result['Message'])
    return None

  for client in result['Value']:
    result = getProviderInfo(client)
    if not result['OK']:
      gLogger.debug(result['Message'])
      continue
    data = DEFAULT_CLIENTS.get(result['Value']['ProviderType'], {})
    data.update(result['Value'])
    if data.get('client_id') and data['client_id'] == clientID:
      gLogger.debug('Found client:\n', pprint.pformat(data))
      return Client(data)

  return None


class Client(OAuth2ClientMixin):
  def __init__(self, params):
    super(Client, self).__init__()
    client_metadata = params.get('client_metadata', params)
    if client_metadata.get('scope') and DEFAULT_SCOPE not in client_metadata['scope']:
      client_metadata['scope'] += ' %s' % DEFAULT_SCOPE
    else:
      client_metadata['scope'] = DEFAULT_SCOPE
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
