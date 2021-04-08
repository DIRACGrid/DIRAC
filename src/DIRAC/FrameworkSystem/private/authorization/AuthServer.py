""" This class provides authorization server activity. """
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import io
import json
from time import time
import pprint
from tornado.httpclient import HTTPResponse
from tornado.httputil import HTTPHeaders

from authlib.deprecate import deprecate
from authlib.jose import jwt
from authlib.oauth2 import (
    OAuth2Error,
    # OAuth2Request,
    HttpRequest,
    AuthorizationServer as _AuthorizationServer,
)
from authlib.oauth2.rfc6749.grants import ImplicitGrant
from DIRAC.FrameworkSystem.private.authorization.grants.DeviceFlow import (
    DeviceAuthorizationEndpoint,
    DeviceCodeGrant
)
from DIRAC.FrameworkSystem.private.authorization.grants.AuthorizationCode import (
    OpenIDCode,
    AuthorizationCodeGrant
)
from DIRAC.FrameworkSystem.private.authorization.grants.RefreshToken import RefreshTokenGrant
from DIRAC.FrameworkSystem.private.authorization.grants.TokenExchange import TokenExchangeGrant
from DIRAC.FrameworkSystem.private.authorization.grants.ImplicitFlow import (
    OpenIDImplicitGrant,
    NotebookImplicitGrant
)
from DIRAC.FrameworkSystem.private.authorization.utils.Clients import (
    Client,
    ClientRegistrationEndpoint,
    ClientManager
)
from DIRAC.FrameworkSystem.private.authorization.utils.Sessions import SessionManager
from DIRAC.FrameworkSystem.private.authorization.utils.Requests import (
    OAuth2Request,
    createOAuth2Request
)
from authlib.oidc.core import UserInfo

from authlib.oauth2.rfc6750 import BearerToken
from authlib.oauth2.rfc7636 import CodeChallenge
from authlib.oauth2.rfc8414 import AuthorizationServerMetadata
from authlib.common.security import generate_token
from authlib.common.encoding import to_unicode, json_dumps, json_b64encode, urlsafe_b64decode, json_loads

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.FrameworkSystem.DB.AuthDB import AuthDB
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getSetup
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
from DIRAC.FrameworkSystem.Client.AuthManagerClient import gSessionManager
# from DIRAC.Core.Web.SessionData import SessionStorage

import logging
import sys
log = logging.getLogger('authlib')
log.addHandler(logging.StreamHandler(sys.stdout))
log.setLevel(logging.DEBUG)
log = gLogger.getSubLogger(__name__)


class AuthServer(_AuthorizationServer, SessionManager, ClientManager):
  """ Implementation of :class:`authlib.oauth2.rfc6749.AuthorizationServer`.

      Initialize it ::

        server = AuthServer()
  """
  metadata_class = AuthorizationServerMetadata

  def __init__(self):
    self.__db = AuthDB()
    self.idps = IdProviderFactory()
    ClientManager.__init__(self, self.__db)
    SessionManager.__init__(self)
    # Privide two authlib methods query_client and save_token
    _AuthorizationServer.__init__(self, query_client=self.getClient, save_token=self.saveToken)
    self.generate_token = BearerToken(self.access_token_generator, self.refresh_token_generator)
    self.config = {}
    self.metadata = {}
    self.pubClients = {}
    # TODO: move to conf utility
    result = gConfig.getOptionsDictRecursively('/Systems/Framework/Production/Services/AuthManager/AuthorizationServer')
    if result['OK']:
      data = {}
      # Search values with type list
      for key, v in result['Value'].items():
        data[key] = [e for e in v.replace(', ', ',').split(',') if e] if ',' in v else v
      # Verify metadata
      metadata = self.metadata_class(data)
      metadata.validate()
      self.metadata = metadata

    clientsData = '/Systems/Framework/Production/Services/AuthManager/AuthorizationServer/Clients'
    result = gConfig.getOptionsDictRecursively(clientsData)
    if result['OK']:
      self.pubClients = result['Value']

    self.config.setdefault('error_uris', self.metadata.get('OAUTH2_ERROR_URIS'))
    if self.metadata.get('OAUTH2_JWT_ENABLED'):
      deprecate('Define "get_jwt_config" in OpenID Connect grants', '1.0')
      self.init_jwt_config(self.metadata)

    self.register_grant(NotebookImplicitGrant)  # OpenIDImplicitGrant)
    self.register_grant(TokenExchangeGrant)
    self.register_grant(DeviceCodeGrant)
    self.register_grant(AuthorizationCodeGrant, [CodeChallenge(required=True), OpenIDCode(require_nonce=False)])
    self.register_endpoint(ClientRegistrationEndpoint)
    self.register_endpoint(DeviceAuthorizationEndpoint)

  def saveToken(self, token, request):
    """ Store tokens

        :param dict token: tokens
        :param object request: http Request object, implemented for compatibility with authlib library (unuse)
    """
    if 'refresh_token' in token:
      gLogger.debug('Save long-token:\n', pprint.pformat(dict(token)))
      # Cache it for one month
      self.addSession(token['refresh_token'], exp=int(time()) + (30 * 24 * 3600), token=dict(token))
    return None

  def getIdPAuthorization(self, providerName, mainSession):
    """ Submit subsession and return dict with authorization url and session number

        :param str providerName: provider name
        :param str mainSession: main session identificator

        :return: S_OK(dict)/S_ERROR() -- dictionary contain next keys:
                 Status -- session status
                 UserName -- user name, returned if status is 'ready'
                 Session -- session id, returned if status is 'needToAuth'
    """
    # Start subsession
    session = generate_token(10)
    self.addSession(session, mainSession=mainSession, Provider=providerName)

    result = gSessionManager.getIdPAuthorization(providerName, session)
    if result['OK']:
      authURL, sessionParams = result['Value']
      self.updateSession(session, **sessionParams)
    return S_OK(authURL) if result['OK'] else result

  def parseIdPAuthorizationResponse(self, response, session):
    """ Fill session by user profile, tokens, comment, OIDC authorize status, etc.
        Prepare dict with user parameters, if DN is absent there try to get it.
        Create new or modify existing DIRAC user and store the session

        :param dict response: authorization response
        :param str session: session

        :return: S_OK(dict)/S_ERROR()
    """
    # print('All sessions')
    # pprint(self.getSessions())
    # Get IdP authorization flows session
    session = self.getSession(session)
    if not session:
      return S_ERROR("Session expired.")
    # And remove it, the credentionals will be stored to the database.
    self.removeSession(session)

    result = gSessionManager.parseAuthResponse(session['Provider'], createOAuth2Request(response).toDict(),
                                               session)
    if not result['OK']:
      self.updateSession(session['mainSession'], Status='failed', Comment=result['Message'])
      return result

    username, profile, _ = result['Value']

    if username and profile:
      self.updateSession(session['mainSession'], username=username, profile=profile, userID=profile['ID'])
    return S_OK(session['mainSession'])

  def access_token_generator(self, client, grant_type, user, scope):
    """ A function to generate ``access_token``

        :param object client: Client object
        :param str grant_type: grant type
        :param str user: user unique id
        :param str scope: scope

        :return: jwt object
    """
    gLogger.debug('GENERATE DIRAC ACCESS TOKEN for "%s" with "%s" scopes.' % (user, scope))
    header = {'alg': 'RS256'}
    payload = {'sub': user,
               'iss': self.metadata['issuer'],
               'iat': int(time()),
               'exp': int(time()) + (12 * 3600),
               'scope': scope,
               'setup': getSetup()}
    # Read private key of DIRAC auth service
    with open('/opt/dirac/etc/grid-security/jwtRS256.key', 'r') as f:
      key = f.read()
    # Need to use enum==0.3.1 for python 2.7
    return jwt.encode(header, payload, key)

  def refresh_token_generator(self, client, grant_type, user, scope):
    """ A function to generate ``refresh_token``

        :param object client: Client object
        :param str grant_type: grant type
        :param str user: user unique id
        :param str scope: scope

        :return: jwt object
    """
    gLogger.debug('GENERATE DIRAC REFRESH TOKEN for "%s" with "%s" scopes.' % (user, scope))
    header = {'alg': 'RS256'}
    payload = {'sub': user,
               'iss': self.metadata['issuer'],
               'iat': int(time()),
               'exp': int(time()) + (30 * 24 * 3600),
               'scope': scope,
               'setup': getSetup(),
               'client_id': client.client_id}
    # Read private key of DIRAC auth service
    with open('/opt/dirac/etc/grid-security/jwtRS256.key', 'r') as f:
      key = f.read()
    # Need to use enum==0.3.1 for python 2.7
    return jwt.encode(header, payload, key)

  def init_jwt_config(self, config):
    """ Initialize JWT related configuration. """
    jwt_iss = config.get('OAUTH2_JWT_ISS')
    if not jwt_iss:
      raise RuntimeError('Missing "OAUTH2_JWT_ISS" configuration.')

    jwt_key_path = config.get('OAUTH2_JWT_KEY_PATH')
    if jwt_key_path:
      with open(jwt_key_path, 'r') as f:
        if jwt_key_path.endswith('.json'):
          jwt_key = json.load(f)
        else:
          jwt_key = to_unicode(f.read())
    else:
      jwt_key = config.get('OAUTH2_JWT_KEY')

    if not jwt_key:
      raise RuntimeError('Missing "OAUTH2_JWT_KEY" configuration.')

    jwt_alg = config.get('OAUTH2_JWT_ALG')
    if not jwt_alg:
      raise RuntimeError('Missing "OAUTH2_JWT_ALG" configuration.')

    jwt_exp = config.get('OAUTH2_JWT_EXP', 3600)
    self.config.setdefault('jwt_iss', jwt_iss)
    self.config.setdefault('jwt_key', jwt_key)
    self.config.setdefault('jwt_alg', jwt_alg)
    self.config.setdefault('jwt_exp', jwt_exp)

  def get_error_uris(self, request):
    error_uris = self.config.get('error_uris')
    if error_uris:
      return dict(error_uris)

  def create_oauth2_request(self, request, method_cls=OAuth2Request, use_json=False):
    gLogger.debug('Create OAuth2 request', 'with json' if use_json else '')
    return createOAuth2Request(request, method_cls, use_json)

  def create_json_request(self, request):
    return self.create_oauth2_request(request, HttpRequest, True)

  def handle_response(self, status_code, payload, headers):
    gLogger.debug('Handle authorization response with %s status code:' % status_code, payload)
    gLogger.debug(headers)
    if isinstance(payload, dict):
      # `OAuth2Request` is not JSON serializable
      payload.pop('request', None)
      payload = json_dumps(payload)
    # return (payload, status_code, headers)

    header = HTTPHeaders()
    for h in headers:
      header.add(*h)
    # Expected that 'data' is unicode string, for Python 2 => unicode(str, "utf-8")
    return dict(code=status_code, headers=header, buffer=io.StringIO(to_unicode(payload)))
    # return HTTPResponse(self.request, status_code, headers=header, buffer=io.StringIO(payload))

  def validate_consent_request(self, request, end_user=None):
    """ Validate current HTTP request for authorization page. This page
        is designed for resource owner to grant or deny the authorization::

        :param object request: tornado request
        :param end_user: end user

        :return: grant instance
    """
    print('==== validate_consent_request ===')
    req = self.create_oauth2_request(request)
    req.user = end_user
    grant = self.get_authorization_grant(req)
    print('==== GRANT: %s ===' % grant)
    grant.validate_consent_request()
    session = req.state or generate_token(10)
    # self.server.updateSession(session, request=req, group=req.args.get('group'))
    if not hasattr(grant, 'prompt'):
      grant.prompt = None
    print('==== Session: %s' % session)
    print('==== Request:')
    pprint.pprint(req.data)
    print('============')
    self.updateSession(session, request=req, createIfNotExist=True)
    return grant, session
