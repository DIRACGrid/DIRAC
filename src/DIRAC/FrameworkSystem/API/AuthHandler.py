""" This handler basically provides a REST interface to interact with the OAuth 2 authentication server
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import pprint
import requests
from io import open

from dominate import document, tags as dom
from tornado.template import Template
from tornado.httputil import HTTPHeaders
from tornado.httpclient import HTTPResponse, HTTPRequest

from authlib.jose import jwk, jwt
# from authlib.jose import JsonWebKey
from authlib.oauth2.base import OAuth2Error

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Tornado.Server.TornadoREST import TornadoREST
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.FrameworkSystem.private.authorization.AuthServer import AuthServer
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import ResourceProtector
from DIRAC.FrameworkSystem.private.authorization.utils.Clients import ClientRegistrationEndpoint
from DIRAC.FrameworkSystem.private.authorization.grants.DeviceFlow import DeviceAuthorizationEndpoint
from DIRAC.FrameworkSystem.private.authorization.utils.Requests import createOAuth2Request
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory

__RCSID__ = "$Id$"


class AuthHandler(TornadoREST):
  # TODO: docs
  # Authorization access to all methods handled by AuthServer instance
  USE_AUTHZ_GRANTS = ['VISITOR']
  SYSTEM = 'Framework'
  AUTH_PROPS = 'all'
  LOCATION = "/DIRAC/auth"
  CSS = """
.button {
  border-radius: 4px;
  background-color: #ffffff00;
  border: none;
  color: black;
  text-align: center;
  font-size: 14px;
  padding: 12px;
  width: 100%;
  transition: all 0.5s;
  cursor: pointer;
  margin: 5px;
  display: block; /* Make the links appear below each other */
}
.button a {
  color: black;
  cursor: pointer;
  display: inline-block;
  position: relative;
  transition: 0.5s;
  text-decoration: none; /* Remove underline from links */
}
.button a:after {
  content: '\\00bb';
  position: absolute;
  opacity: 0;
  top: 0;
  right: -20px;
  transition: 0.5s;
}
.button:hover a {
  padding-right: 25px;
}
.button:hover a:after {
  opacity: 1;
  right: 0;
}"""

  @classmethod
  def initializeHandler(cls, serviceInfo):
    """ This method is called only one time, at the first request

        :param dict ServiceInfoDict: infos about services
    """
    cls.server = AuthServer()
    cls.idps = IdProviderFactory()
    cls.css = {}
    cls.css_align_center = 'display:block;justify-content:center;align-items:center;'
    cls.css_center_div = 'height:700px;width:100%;position:absolute;top:50%;left:0;margin-top:-350px;'
    cls.css_big_text = 'font-size:28px;'
    cls.css_main = ' '.join([cls.css_align_center, cls.css_center_div, cls.css_big_text])

  def initializeRequest(self):
    """ Called at every request """
    self.currentPath = self.request.protocol + "://" + self.request.host + self.request.path
    # Template for a html UI
    self.doc = document('DIRAC authentication')
    with self.doc.head:
      dom.link(rel='stylesheet',
               href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css")
      dom.style(self.CSS)

  def _parseDIRACResult(self, result):
    """ Here the result which returns handle_response is processed
    """
    if not result['OK']:
      # If response error is DIRAC server error, not OAuth2 flow error
      self.removeSession()
      self.set_status = 400
      self.write({'error': 'server_error',
                  'description': '%s:\n%s' % (result['Message'], '\n'.join(result['CallStack']))})
    else:
      # Successful responses and OAuth2 errors are processed here
      status_code, headers, payload, new_session, error = result['Value'][0]
      if status_code:
        self.set_status(status_code)
      if headers:
        for key, value in headers:
          self.set_header(key, value)
      if payload:
        self.write(payload)
      if new_session:
        self.saveSession(new_session)
      if error:
        self.removeSession()
      for method, args_kwargs in result['Value'][1].items():
        eval('self.%s' % method)(*args_kwargs[0], **args_kwargs[1])
  
  def saveSession(self, session):
    """ Save session to cookie

        :param dict session: session
    """
    self.set_secure_cookie('auth_session', json.dumps(session), secure=True, httponly=True)

  def removeSession(self):
    """ Remove session from cookie """
    self.clear_cookie('auth_session')
  
  def getSession(self, state=None, **kw):
    """ Get session from cookie

        :param str state: state

        :return: dict
    """
    try:
      session = json.loads(self.get_secure_cookie('auth_session'))
      checkState = (session['state'] == state) if state else None
      checkOption = (session[kw.items()[0][0]] == kw.items()[0][0]) if kw else None
    except Exception as e:
      return None
    return session if (checkState or checkOption) else None

  path_index = ['.well-known/(oauth-authorization-server|openid-configuration)']

  def web_index(self, instance):
    """ Well known endpoint, specified by
        `RFC8414 <https://tools.ietf.org/html/rfc8414#section-3>`_

        Request examples::

          GET: LOCATION/.well-known/openid-configuration
          GET: LOCATION/.well-known/oauth-authorization-server

        Responce::

          HTTP/1.1 200 OK
          Content-Type: application/json

          {
            "registration_endpoint": "https://domain.com/DIRAC/auth/register",
            "userinfo_endpoint": "https://domain.com/DIRAC/auth/userinfo",
            "jwks_uri": "https://domain.com/DIRAC/auth/jwk",
            "code_challenge_methods_supported": [
              "S256"
            ],
            "grant_types_supported": [
              "authorization_code",
              "code",
              "urn:ietf:params:oauth:grant-type:device_code",
              "implicit",
              "refresh_token"
            ],
            "token_endpoint": "https://domain.com/DIRAC/auth/token",
            "response_types_supported": [
              "code",
              "device",
              "id_token token",
              "id_token",
              "token"
            ],
            "authorization_endpoint": "https://domain.com/DIRAC/auth/authorization",
            "issuer": "https://domain.com/DIRAC/auth"
          }
    """
    if self.request.method == "GET":
      return dict(self.server.metadata)

  def web_jwk(self):
    """ JWKs endpoint

        Request example::

          GET LOCATION/jwk

        Response::

          HTTP/1.1 200 OK
          Content-Type: application/json

          {
            "keys": [
              {
                "e": "AQAB",
                "kty": "RSA",
                "n": "3Vv5h5...X3Y7k"
              }
            ]
          }
    """
    if self.request.method == "GET":
      with open('/opt/dirac/etc/grid-security/jwtRS256.key.pub', 'rb') as f:
        key = f.read()
      # # # For newer version
      # # # key = JsonWebKey.import_key(key, {'kty': 'RSA'})
      # # # self.finish(key.as_dict())
      return {'keys': [jwk.dumps(key, kty='RSA', alg='RS256')]}

  # auth_userinfo = ["authenticated"]
  def web_userinfo(self):
    """ The UserInfo endpoint can be used to retrieve identity information about a user,
        see `spec <https://openid.net/specs/openid-connect-core-1_0.html#UserInfo>`_

        GET LOCATION/userinfo

        Parameters:
        +---------------+--------+---------------------------------+--------------------------------------------------+
        | **name**      | **in** | **description**                 | **example**                                      |
        +---------------+--------+---------------------------------+--------------------------------------------------+
        | Authorization | header | Provide access token            | Bearer jkagfbfd3r4ubf887gqduyqwogasd87           |
        +---------------+--------+---------------------------------+--------------------------------------------------+

        Request example::

          GET LOCATION/userinfo
          Authorization: Bearer <access_token>

        Response::

          HTTP/1.1 200 OK
          Content-Type: application/json

          {
            "sub": "248289761001",
            "name": "Bob Smith",
            "given_name": "Bob",
            "family_name": "Smith",
            "group": [
              "dirac_user",
              "dirac_admin"
            ]
          }
    """
    # Token verification
    token = ResourceProtector().acquire_token(self.request, '')
    return {'sub': token.sub, 'issuer': token.issuer, 'group': token.groups[0]}
    # return {'username': credDict['username'],
    #         'group': credDict['group']}
    # return self.__validateToken()

  def web_register(self):
    """ The Client Registration Endpoint, specified by
        `RFC7591 <https://tools.ietf.org/html/rfc7591#section-3.1>`_

        POST LOCATION/register?data..

        Parameters:
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | **name**       | **in** | **description**                           | **example**                           |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | Authorization  | header | Provide access token                      | Bearer jkagfbfd3r4ubf887gqduyqwogasd8 |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | grant_types    | data   | list of grant types, more supported       | ["authorization_code","refresh_token"]|
        |                |        | more supported grant types in *grants     |                                       |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | scope          | data   | list of scoupes separated by a space      | something                             |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | response_types | data   | list of returned responses                | ["token","id_token token","code"]     |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | redirect_uris  | data   | Redirection URI to which the response will| ['https://dirac.egi.eu/redirect']     |
        |                |        | be sent.                                  |                                       |
        +----------------+--------+-------------------------------------------+---------------------------------------+

        *:mod:`grants <DIRAC.FrameworkSystem.private.authorization.grants>`

        https://wlcg.cloud.cnaf.infn.it/register

        requests.post('https://marosvn32.in2p3.fr/DIRAC/auth/register',
                      json={'grant_types': ['implicit'],
                            'response_types': ['token'],
                            'redirect_uris': ['https://dirac.egi.eu'],
                            'token_endpoint_auth_method': 'none'}, verify=False).text
        requests.post('https://marosvn32.in2p3.fr/DIRAC/auth/register',
                      json={"scope":"changeGroup",
                            "token_endpoint_auth_method":"client_secret_basic",
                            "grant_types":["authorization_code","refresh_token"],
                            "redirect_uris":["https://marosvn32.in2p3.fr/DIRAC","https://marosvn32.in2p3.fr/DIRAC/loginComplete"],
                            "response_types":["token","id_token token","code"]}, verify=False).text
    """
    return self.server.create_endpoint_response(ClientRegistrationEndpoint.ENDPOINT_NAME, self.request)

  path_device = ['([A-z0-9-_]*)']

  def web_device(self, provider=None):
    """ The device authorization endpoint can be used to request device and user codes.
        This endpoint is used to start the device flow authorization process and user code verification.

        POST LOCATION/device/<provider>?<query>

        Parameters:
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | **name**       | **in** | **description**                           | **example**                           |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | user code      | query  | in the last step to confirm recived user  | WE8R-WEN9                             |
        |                |        | code put it as query parameter (optional) |                                       |
        |                |        | It's possible to add it interactively.    |                                       |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | client_id      | query  | The public client ID                      | 3f6eNw0E6JGq1VuzRkpWUL9XTxhL86efZw    |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | scope          | query  | list of scoupes separated by a space, to  | g:dirac_user                          |
        |                |        | add a group you must add "g:" before the  |                                       |
        |                |        | group name                                |                                       |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | provider       | path   | identity provider to autorize (optional)  | CheckIn                               |
        |                |        | It's possible to add it interactively.    |                                       |
        +----------------+--------+-------------------------------------------+---------------------------------------+


        User code confirmation::

          GET LOCATION/device/<provider>?user_code=<user code>

        Request example, to initialize a Device authentication flow::

          POST LOCATION/device/CheckIn_dev?client_id=3f1DAj8z6eNw0E6JGq1Vu6efZwyV&scope=g:dirac_admin

        Response::

          HTTP/1.1 200 OK
          Content-Type: application/json

          {
            "device_code": "TglwLiow0HUwowjB9aHH5HqH3bZKP9d420LkNhCEuR",
            "verification_uri": "https://marosvn32.in2p3.fr/DIRAC/auth/device",
            "interval": 5,
            "expires_in": 1800,
            "verification_uri_complete": "https://marosvn32.in2p3.fr/DIRAC/auth/device/WSRL-HJMR",
            "user_code": "WSRL-HJMR"
          }

        Request example, to confirm the user code::

          POST LOCATION/device/CheckIn_dev/WSRL-HJMR

        Response::

          HTTP/1.1 200 OK
    """
    if self.request.method == 'POST':
      group = self.get_argument('group', None)
      if group:
        provider = Registry.getIdPForGroup(group)
        if not provider:
          return S_ERROR('No provider found for %s' % group)
        result = self.idps.getIdProvider(provider + '_public')
        if result['OK']:
          idPObj = result['Value']
          result = idPObj.submitDeviceCodeAuthorizationFlow(group)
        if not result['OK']:
          return result
        return result['Value']

      self.log.verbose('Initialize a Device authentication flow.')
      return self.server.create_endpoint_response(DeviceAuthorizationEndpoint.ENDPOINT_NAME, self.request)

    elif self.request.method == 'GET':
      userCode = self.get_argument('user_code', None)
      if userCode:
        # If received a request with a user code, then prepare a request to authorization endpoint
        self.log.verbose('User code verification.')
        session, data = self.server.db.getSessionByOption('user_code', userCode)
        if not session:
          return 'Device flow authorization session %s expired.' % session
        # Get original request from session
        req = createOAuth2Request(dict(method='GET', uri=data['uri']))
        authURL = '/authorization/%s?%s&user_code=%s' % (provider, req.query, userCode)
        # Save session to cookie
        return self.server.handle_response(302, {}, [("Location", authURL)], data)

      # If received a request without a user code, then send a form to enter the user code
      with self.doc:
        dom.div(dom.form(dom._input(type="text", name="user_code", style=self.css_big_text),
                         dom.button('Submit', type="submit", style=self.css_big_text),
                         action=self.currentPath, method="GET"), style=self.css_main)
      return Template(self.doc.render()).generate()

  path_authorization = ['([A-z0-9]*)']

  def web_authorization(self, provider=None):
    """ Authorization endpoint

        GET: LOCATION/authorization/<provider>

        Parameters:
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | **name**       | **in** | **description**                           | **example**                           |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | response_type  | query  | informs of the desired grant type         | code                                  |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | client_id      | query  | The client ID                             | 3f6eNw0E6JGq1VuzRkpWUL9XTxhL86efZw    |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | scope          | query  | list of scoupes separated by a space, to  | g:dirac_user                          |
        |                |        | add a group you must add "g:" before the  |                                       |
        |                |        | group name                                |                                       |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | provider       | path   | identity provider to autorize (optional)  | CheckIn                               |
        |                |        | It's possible to add it interactively.    |                                       |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        General options:
          provider -- identity provider to autorize

        Device flow:
          &user_code=..                         (required)

        Authentication code flow:
          &scope=..                             (optional)
          &redirect_uri=..                      (optional)
          &state=..                             (main session id, optional)
          &code_challenge=..                    (PKCE, optional)
          &code_challenge_method=(pain|S256)    ('pain' by default, optional)
    """
    return self.server.validate_consent_request(self.request, provider)

  def web_redirect(self):
    """ Redirect endpoint.
        After a user successfully authorizes an application, the authorization server will redirect
        the user back to the application with either an authorization code or access token in the URL.
        The full URL of this endpoint must be registered in the identity provider.

        Read more in `oauth.com <https://www.oauth.com/oauth2-servers/redirect-uris/>`_.
        Specified by `RFC6749 <https://tools.ietf.org/html/rfc6749#section-3.1.2>`_.

        GET LOCATION/redirect

        Parameters::

          &chooseScope=..  to specify new scope(group in our case) (optional)
    """
    # Current IdP session state
    state = self.get_argument('state')

    # Try to catch errors
    if self.get_argument('error', None):
      error = OAuth2Error(error=self.get_argument('error'), description=self.get_argument('error_description', ''))
      return self.server.handle_error_response(state, error)

    # Check current auth session that was initiated for the selected external identity provider
    sessionWithExtIdP = self.getSession(state)
    if not sessionWithExtIdP:
      return S_ERROR("%s session is expired." % state)

    if not sessionWithExtIdP.get('authed'):
      # Parse result of the second authentication flow
      self.log.info('%s session, parsing authorization response:\n' % state,
                    '\n'.join([self.request.uri, self.request.query, self.request.body, str(self.request.headers)]))
      
      result = self.server.parseIdPAuthorizationResponse(self.request, sessionWithExtIdP)
      if not result['OK']:
        return result
      # Return main session flow
      sessionWithExtIdP['authed'] = result['Value']

    # Research group
    grant_user, response = self.__researchDIRACGroup(sessionWithExtIdP)
    if not grant_user:
      return response

    # RESPONSE to basic DIRAC client request
    return self.server.create_authorization_response(response, grant_user)

  def web_token(self):
    """ The token endpoint, the description of the parameters will differ depending on the selected grant_type

        POST LOCATION/token

        Parameters:
        +----------------+--------+-------------------------------------+---------------------------------------------+
        | **name**       | **in** | **description**                     | **example**                                 |
        +----------------+--------+-------------------------------------+---------------------------------------------+
        | grant_type     | query  | what grant type to use, more        | urn:ietf:params:oauth:grant-type:device_code|
        |                |        | supported grant types in *grants    |                                             |
        +----------------+--------+-------------------------------------+---------------------------------------------+
        | client_id      | query  | The public client ID                | 3f1DAj8z6eNw0E6JGq1VuzRkpWUL9XTxhL86efZw    |
        +----------------+--------+-------------------------------------+---------------------------------------------+
        | device_code    | query  | device code                         | uW5xL4hr2tqwBPKL5d0JO9Fcc67gLqhJsNqYTSp     |
        +----------------+--------+-------------------------------------+---------------------------------------------+

        *:mod:`grants <DIRAC.FrameworkSystem.private.authorization.grants>`

        Request example::

          POST LOCATION/token?client_id=L86..yV&grant_type=urn:ietf:params:oauth:grant-type:device_code&device_code=uW5

        Response::

          HTTP/1.1 400 OK
          Content-Type: application/json

          {
            "error": "authorization_pending"
          }
    """
    return self.server.create_token_response(self.request)

  def __researchDIRACGroup(self, extSession):
    """ Research DIRAC groups for authorized user

        :param dict extSession: ended authorized external IdP session

        :return: response
    """
    # Base DIRAC client auth session
    firstRequest = createOAuth2Request(extSession['mainSession'])
    # Read requested groups by DIRAC client or user
    firstRequest.addScopes(self.get_arguments('chooseScope', []))
    # Read already authed user
    username, userID = extSession['authed']
    self.log.debug('Next groups has been found for %s:' % username, ', '.join(firstRequest.groups))

    # Researche Group
    result = gProxyManager.getGroupsStatusByUsername(username, firstRequest.groups)
    if not result['OK']:
      return None, result
    groupStatuses = result['Value']
    if not groupStatuses:
      return None, S_ERROR('No groups found.')
    self.log.debug('The state of %s user groups has been checked:' % username, pprint.pformat(groupStatuses))

    if not firstRequest.groups:
      if len(groupStatuses) == 1:
        firstRequest.addScopes(['g:%s' % groupStatuses[0]])
      else:
        # Choose group interface
        with self.doc:
          with dom.div(style=self.css_main):
            with dom.div('Choose group', style=self.css_align_center):
              for group, data in groupStatuses.items():
                # data: Status, Comment, Action
                dom.button(dom.a(group, href='%s?state=%s&chooseScope=g:%s' % (self.currentPath,
                                                                               self.get_argument('state'), group)),
                           cls='button')
        return None, self.server.handle_response(payload=Template(self.doc.render()).generate(), newSession=extSession)

    for group in firstRequest.groups:
      status = groupStatuses[group]['Status']
      action = groupStatuses[group].get('Action')
      comment = groupStatuses[group].get('Comment')

      if status == 'needToAuth':
        # Submit second auth flow through IdP
        idP = action[1][0]
        return None, self.server.getIdPAuthorization(idP, firstRequest)

      if status not in ['ready', 'unknown']:
        self.log.verbose('%s group has bad status: %s; %s' % (group, status, comment))

    # Return grant user
    return {'username': username, 'user_id': userID}, firstRequest
