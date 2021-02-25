""" This class describe Authorization Code grant type
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from time import time
from pprint import pprint
from authlib.jose import JsonWebSignature
from authlib.oidc.core import UserInfo
from authlib.oidc.core.grants import OpenIDCode as _OpenIDCode
from authlib.oauth2.rfc6749.grants import AuthorizationCodeGrant as _AuthorizationCodeGrant
from authlib.oauth2.rfc7636 import CodeChallenge
from authlib.common.encoding import to_unicode, json_dumps, json_b64encode, urlsafe_b64decode, json_loads

from DIRAC import gLogger, S_OK, S_ERROR

log = gLogger.getSubLogger(__name__)


class OAuth2Code(dict):
  def __init__(self, params):
    params['auth_time'] = params.get('auth_time', int(time()))
    super(OAuth2Code, self).__init__(params)

  @property
  def user(self):
    return self.get('user_id') #(self.get('user_id'), self.get('group'))
  
  @property
  def code_challenge(self):
    return self.get('code_challenge')

  @property
  def code_challenge_method(self):
    return self.get('code_challenge_method', 'pain')

  def is_expired(self):
    return self.get('auth_time') + 300 < time()

  def get_redirect_uri(self):
    return self.get('redirect_uri')

  def get_scope(self):
    return self.get('scope', '')

  def get_auth_time(self):
    return self.get('auth_time')
  
  def get_nonce(self):
    return self.get('nonce')


class OpenIDCode(_OpenIDCode):
  def exists_nonce(self, nonce, request):
    return False
    # try:
    #   AuthorizationCode.objects.get(client_id=request.client_id, nonce=nonce)
    #   return True
    # except AuthorizationCode.DoesNotExist:
    #   return False

  def get_jwt_config(self, grant):
    with open('/opt/dirac/etc/grid-security/jwtRS256.key', 'rb') as f:
      key = f.read()
    issuer = grant.server.metadata['issuer']
    return {'key': key, 'alg': 'RS512', 'iss': issuer, 'exp': 3600}

  def generate_user_info(self, user, scope):
    print('== generate_user_info ==')
    # pprint(self.__dict__)
    print(user)
    print(scope)
    # data = self.server.getSession(self.request.state)
    # return UserInfo(sub=user[0], profile=data['profile'], grp=user[1])
    return UserInfo(sub=user[0], grp=user[1])


class AuthorizationCodeGrant(_AuthorizationCodeGrant):
  TOKEN_ENDPOINT_AUTH_METHODS = ['client_secret_basic', 'client_secret_post', 'none']

  def save_authorization_code(self, code, request):
    pass
  
  def delete_authorization_code(self, authorization_code):
    pass

  def query_authorization_code(self, code, client):
    """ Parse authorization code

        :param code: authorization code as JWS
        :param client: client

        :return: OAuth2Code or None
    """
    print('== query_authorization_code ==')
    pprint(code)
    jws = JsonWebSignature(algorithms=['RS256'])
    with open('/opt/dirac/etc/grid-security/jwtRS256.key.pub', 'rb') as f:
      key = f.read()
    data = jws.deserialize_compact(code, key)
    try:
      item = OAuth2Code(json_loads(urlsafe_b64decode(data['payload'])))
      pprint(dict(item))
      print('get_scope: %s' % item.get_scope())
    except Exception as e:
      return None
    if not item.is_expired():
      return item

  def authenticate_user(self, authorization_code):
    return authorization_code.user
  
  def generate_authorization_code(self):
    """ return code """
    print('========= generate_authorization_code =========')
    print('DICT:')
    pprint(self.__dict__)
    print('Reuest:')
    pprint(self.request.data)
    print('Session:')
    pprint(self.server.getSession(self.request.state))
    print('-----------------------------------------------')
    sessionDict = self.server.getSession(self.request.state)
    jws = JsonWebSignature(algorithms=['RS256'])
    protected = {'alg': 'RS256'}
    code = OAuth2Code({'user_id': sessionDict['userID'],
                      #  'group': sessionDict['group'],
                       'scope': self.request.data['scope'],
                       'redirect_uri': self.request.args['redirect_uri'],
                       'client_id': self.request.args['client_id'],
                       'code_challenge': self.request.args.get('code_challenge'),
                       'code_challenge_method': self.request.args.get('code_challenge_method')})
    print('--= Payload =--')
    pprint(dict(code))
    # payload = json_dumps(dict(code)) #
    payload = json_b64encode(dict(code))
    pprint(payload)
    print('--=         =--')
    with open('/opt/dirac/etc/grid-security/jwtRS256.key', 'rb') as f:
      key = f.read()
    return jws.serialize_compact(protected, payload, key)
