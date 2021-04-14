from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import time

from authlib.integrations.sqla_oauth2 import OAuth2ClientMixin
from authlib.oauth2.rfc7591 import ClientRegistrationEndpoint as _ClientRegistrationEndpoint
from authlib.oauth2.rfc6749.util import scope_to_list, list_to_scope
from authlib.common.security import generate_token

from DIRAC.Core.Utilities import ThreadSafe
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.ConfigurationSystem.Client.Utilities import getAuthClients

__RCSID__ = "$Id$"

gCacheClient = ThreadSafe.Synchronizer()


class Client(OAuth2ClientMixin):
  def __init__(self, params):
    super(Client, self).__init__()
    self.client_id = params['client_id']
    self.client_secret = params.get('client_secret', '')
    self.client_id_issued_at = params['client_id_issued_at']
    self.client_secret_expires_at = params['client_secret_expires_at']
    if isinstance(params['client_metadata'], dict):
      self._client_metadata = json.dumps(params['client_metadata'])
    else:
      self._client_metadata = params['client_metadata']

  def get_allowed_scope(self, scope):
    if not scope:
      return ''
    allowed = set(self.scope.split())
    scopes = scope_to_list(scope)
    return list_to_scope([s for s in scopes if s in allowed or s.startswith('g:')])


class ClientManager(object):
  def __init__(self, database):
    self.__db = database
    self.__clients = DictCache()

  @gCacheClient
  def addClient(self, data):
    result = self.__db.addClient(data)
    if result['OK']:
      data = result['Value']
      self.__clients.add(data['client_id'], 24 * 3600, Client(data))
    return result

  @gCacheClient
  def getClient(self, clientID):
    print('getClient: %s ' % clientID)
    client = self.__clients.get(clientID)
    print(client)
    if not client:
      result = getAuthClients(clientID)
      if not result['OK'] or not result['Value']:
        result = self.__db.getClient(clientID)
      print('getClient result: %s' % result)
      if result['OK']:
        cliDict = result['Value']
        cliDict['client_id_issued_at'] = cliDict.get('client_id_issued_at', int(time.time()))
        cliDict['client_secret_expires_at'] = cliDict.get('client_secret_expires_at', 0)
        client = Client(cliDict)
        print('getClient client: %s' % str(client))
        self.__clients.add(clientID, 24 * 3600, client)
        print('getClient client added')
    print('finish: client: %s' % client)
    return client


class ClientRegistrationEndpoint(_ClientRegistrationEndpoint):
  """ The client registration endpoint is an OAuth 2.0 endpoint designed to
      allow a client to be registered with the authorization server. See authlib
      :mod:`ClientRegistrationEndpoint <authlib.oauth2.rfc7591.ClientRegistrationEndpoint>` class.
  """
  # TODO: align with last version authlib

  def authenticate_user(self, request):
    return True

  def authenticate_token(self, request):
    # TODO: Provider token verification to allow regster clients only for reg users
    return False

  def save_client(self, client_info, client_metadata, request):
    print("Save client:")
    print(client_info)
    print(client_metadata)
    for k, v in [('grant_types',
                  ['authorization_code', 'urn:ietf:params:oauth:grant-type:device_code']),
                 ('response_types', ['code', 'device']),
                 ('token_endpoint_auth_method', 'none')]:
      if k not in client_metadata:
        client_metadata[k] = v

    if client_metadata['token_endpoint_auth_method'] == 'none':
      client_info['client_secret'] = ''
    # else:
    #   client_info['client_secret'] = generate_token(48)

    client_info['client_metadata'] = client_metadata

    print(client_info)
    result = self.server.addClient(client_info)
    return Client(result['Value']) if result['OK'] else None
