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
    ClientRegistrationEndpoint,
    ClientManager
)
from DIRAC.FrameworkSystem.private.authorization.utils.Sessions import SessionManager
from DIRAC.FrameworkSystem.private.authorization.utils.Requests import (
    OAuth2Request,
    createOAuth2Request
)
# from authlib.oidc.core import UserInfo

from authlib.oauth2.rfc6750 import BearerToken
from authlib.oauth2.rfc7636 import CodeChallenge
from authlib.oauth2.rfc8414 import AuthorizationServerMetadata
from authlib.common.security import generate_token
from authlib.common.encoding import to_unicode, json_dumps

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.FrameworkSystem.DB.AuthDB import AuthDB
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getSetup
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
from DIRAC.FrameworkSystem.Client.AuthManagerClient import gSessionManager
from DIRAC.ConfigurationSystem.Client.Utilities import getAuthorisationServerMetadata
# from DIRAC.Core.Web.SessionData import SessionStorage

import logging
import sys
log = logging.getLogger('authlib')
log.addHandler(logging.StreamHandler(sys.stdout))
log.setLevel(logging.DEBUG)
log = gLogger.getSubLogger(__name__)


class AuthServer(_AuthorizationServer, SessionManager, ClientManager):
  """ Implementation of :class:`authlib.oauth2.rfc6749.AuthorizationServer`.

      Initialize::

        server = AuthServer()
  """
  metadata_class = AuthorizationServerMetadata

  def __init__(self):
    self.db = AuthDB()
    self.idps = IdProviderFactory()
    ClientManager.__init__(self, self.db)
    SessionManager.__init__(self)
    # Privide two authlib methods query_client and save_token
    _AuthorizationServer.__init__(self, query_client=self.getClient, save_token=self.saveToken)
    self.generate_token = BearerToken(self.access_token_generator, self.refresh_token_generator)
    self.config = {}
    self.collectMetadata()

    self.register_grant(NotebookImplicitGrant)  # OpenIDImplicitGrant)
    self.register_grant(TokenExchangeGrant)
    self.register_grant(DeviceCodeGrant)
    self.register_grant(AuthorizationCodeGrant, [CodeChallenge(required=True), OpenIDCode(require_nonce=False)])
    self.register_endpoint(ClientRegistrationEndpoint)
    self.register_endpoint(DeviceAuthorizationEndpoint)

  def collectMetadata(self):
    """ Collect metadata """
    self.metadata = {}
    result = getAuthorisationServerMetadata()
    if not result['OK']:
      raise Exception('Cannot prepare authorization server metadata. %s' % result['Message'])
    # Verify metadata
    metadata = self.metadata_class(result['Value'])
    metadata.validate()
    self.metadata = metadata

  def addSession(self, session):
    self.db.addSession(session)
  
  def getSession(self, session):
    self.db.getSession(session)

  def saveToken(self, token, request):
    """ Store tokens

        :param dict token: tokens
        :param object request: http Request object, implemented for compatibility with authlib library (unuse)
    """
    if 'refresh_token' in token:
      self.db.storeToken(token)
    return None

  def getIdPAuthorization(self, providerName, mainSession=None, oldState=''):
    """ Submit subsession and return dict with authorization url and session number

        :param str providerName: provider name
        :param str mainSession: main session identificator

        :return: S_OK(dict)/S_ERROR() -- dictionary contain next keys:
                 Status -- session status
                 UserName -- user name, returned if status is 'ready'
                 Session -- session id, returned if status is 'needToAuth'
    """
    result = self.idps.getIdProvider(providerName)
    if not result['OK']:
      return result
    idpObj = result['Value']
    result = idpObj.submitNewSession()
    if not result['OK']:
      return result
    authURL, state, session = result['Value']
    session['Provider'] = providerName
    session['mainSessionState'] = mainSession.pop('state')

    gLogger.verbose('Redirect to', authURL)
    return self.handle_response(302, {}, [("Location", authURL)],
                                saveSessions={state: json.dumps(session),
                                              session['mainSessionState']: json.dumps(mainSession)},
                                removeSessions=[oldState])

  def parseIdPAuthorizationResponse(self, response, session):
    """ Fill session by user profile, tokens, comment, OIDC authorize status, etc.
        Prepare dict with user parameters, if DN is absent there try to get it.
        Create new or modify existing DIRAC user and store the session

        :param dict response: authorization response
        :param str session: session

        :return: S_OK(dict)/S_ERROR()
    """
    return gSessionManager.parseAuthResponse(session.pop('Provider'), createOAuth2Request(response).toDict(),
                                             session)

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
    # #
    # Return proxy with token in one response?
    # #

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

  def get_error_uris(self, request):
    error_uris = self.config.get('error_uris')
    if error_uris:
      return dict(error_uris)

  def create_oauth2_request(self, request, method_cls=OAuth2Request, use_json=False):
    gLogger.debug('Create OAuth2 request', 'with json' if use_json else '')
    return createOAuth2Request(request, method_cls, use_json)

  def create_json_request(self, request):
    return self.create_oauth2_request(request, HttpRequest, True)

  def handle_response(self, status_code, payload, headers, saveSessions={}, removeSessions=[], **actions):
    gLogger.debug('Handle authorization response with %s status code:' % status_code, payload)
    gLogger.debug(headers)
    # if isinstance(payload, dict):
    #   # `OAuth2Request` is not JSON serializable
    #   payload.pop('request', None)
    #   payload = json_dumps(payload)
    # # return (payload, status_code, headers)

    # header = HTTPHeaders()
    # for h in headers:
    #   header.add(*h)
    # # Expected that 'data' is unicode string, for Python 2 => unicode(str, "utf-8")

    # # # if state:
    # # #   actions['clear_cookie'] = ([state], {})
    # # # gLogger.debug(actions)
    return S_OK(((status_code, headers, payload, saveSessions, removeSessions), actions))
    
    # return dict(code=status_code, headers=header, buffer=io.StringIO(to_unicode(payload)))
    # return HTTPResponse(self.request, status_code, headers=header, buffer=io.StringIO(payload))

  def create_authorization_response(self, response, username):
    result = super(AuthServer, self).create_authorization_response(response, username)
    if result['OK']:
      # Remove auth session
      result['Value'][0][4].append(response.data['state'])
      # result['Value'][1].update(dict(clear_cookie=([response.data['state']], {})))
    return result

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
    # self.updateSession(session, request=req, createIfNotExist=True)
    return grant, req
