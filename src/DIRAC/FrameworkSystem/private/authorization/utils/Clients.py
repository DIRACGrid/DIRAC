from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
import time
import pprint

from DIRAC import gConfig, gLogger
from authlib.oauth2.rfc6749.util import scope_to_list, list_to_scope
from authlib.integrations.sqla_oauth2 import OAuth2ClientMixin

__RCSID__ = "$Id$"

DEFAULT_CLIENTS = {
    'DIRACCLI': dict(client_id='DIRAC_CLI', scope='proxy g: lifetime:', response_types=['device'],
                     grant_types=['urn:ietf:params:oauth:grant-type:device_code', 'refresh_token'],
                     token_endpoint_auth_method='none', verify=False,
                     ProviderType='OAuth2'),
    'DIRACWeb': dict(client_id='DIRAC_Web', scope='g:', response_types=['code'],
                     grant_types=['authorization_code', 'refresh_token'],
                     ProviderType='OAuth2')
}


def getDIRACClients():
  """ Get DIRAC authorization clients

      :return: S_OK(dict)/S_ERROR()
  """
  clients = DEFAULT_CLIENTS.copy()
  result = gConfig.getOptionsDictRecursively('/DIRAC/Security/Authorization/Client')
  if not result['OK']:
    gLogger.error(result['Message'])
  confClients = result.get('Value', {})
  for cli in confClients:
    if cli not in clients:
      clients[cli] = confClients[cli]
    else:
      clients[cli].update(confClients[cli])
  return clients


class Client(OAuth2ClientMixin):

  def __init__(self, params):
    if params.get('redirect_uri') and not params.get('redirect_uris'):
      params['redirect_uris'] = [params['redirect_uri']]
    self.set_client_metadata(params)
    self.client_id = params['client_id']
    self.client_secret = params.get('client_secret', '')
    self.client_id_issued_at = params.get('client_id_issued_at', int(time.time()))
    self.client_secret_expires_at = params.get('client_secret_expires_at', 0)

  def get_allowed_scope(self, scope):
    if not isinstance(scope, six.string_types):
      scope = list_to_scope(scope)
    allowed = scope_to_list(super(Client, self).get_allowed_scope(scope))
    for s in scope_to_list(scope):
      for def_scope in scope_to_list(self.scope):
        if s.startswith(def_scope) and s not in allowed:
          allowed.append(s)
    gLogger.debug('Try to allow "%s" scope:' % scope, allowed)
    return list_to_scope(list(set(allowed)))
