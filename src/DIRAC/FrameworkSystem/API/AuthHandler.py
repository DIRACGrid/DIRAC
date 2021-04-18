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
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getProvidersForInstance
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
      raise Exception('%s:\n%s' % (result['Message'], '\n'.join(result['CallStack'])))
    status_code, headers, payload, saves, removes = result['Value'][0]
    if status_code:
      self.set_status(status_code)
    if headers:
      for key, value in headers:
        self.set_header(key, value)
    if payload:
      self.write(payload)
    if saves:
      for state, session in saves.items():
        self.saveSession(state, session)
    if removes:
      for state in removes:
        self.removeSession(state)
    print('>>>> PARSE RESULT:')
    pprint.pprint(result['Value'])
    for method, args_kwargs in result['Value'][1].items():
      eval('self.%s' % method)(*args_kwargs[0], **args_kwargs[1])
  
  def saveSession(self, state, payload):
    self.set_secure_cookie(state, payload, secure=True, httponly=True)

  def removeSession(self, state):
    self.clear_cookie(state)
  
  def getSession(self, state):
    return self.get_secure_cookie(state)

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

  def web_device(self, userCode=None):
    """ The device authorization endpoint can be used to request device and user codes.
        This endpoint is used to start the device flow authorization process and user code verification.

        POST LOCATION/device/<user code>?<query>

        Parameters:
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | **name**       | **in** | **description**                           | **example**                           |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | user code      | path   | in the last step to confirm recived user  | WE8R-WEN9                             |
        |                |        | code put it as path parameter (optional)  |                                       |
        |                |        | It's possible to add it interactively.    |                                       |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | client_id      | query  | The public client ID                      | 3f6eNw0E6JGq1VuzRkpWUL9XTxhL86efZw    |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | scope          | query  | list of scoupes separated by a space, to  | g:dirac_user                          |
        |                |        | add a group you must add "g:" before the  |                                       |
        |                |        | group name                                |                                       |
        +----------------+--------+-------------------------------------------+---------------------------------------+
        | provider       | query  | identity provider to autorize (optional)  | CheckIn                               |
        |                |        | It's possible to add it interactively.    |                                       |
        +----------------+--------+-------------------------------------------+---------------------------------------+


        User code confirmation::

          GET LOCATION/device/<UserCode>

          Parameters:
            UserCode - recived user code (optional, it's possible to add it interactively)

        Request example, to initialize a Device authentication flow::

          POST LOCATION/device?client_id=3f1DAj8z6eNw0E6JGq1Vu6efZwyV&scope=g:dirac_admin&provider=CheckIn_dev

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

          POST LOCATION/device/WSRL-HJMR

        Response::

          HTTP/1.1 200 OK
    """
    if self.request.method == 'POST':
      self.log.verbose('Initialize a Device authentication flow.')
      return self.server.create_endpoint_response(DeviceAuthorizationEndpoint.ENDPOINT_NAME, self.request)

    elif self.request.method == 'GET':
      userCode = self.get_argument('user_code', userCode)
      if userCode:
        self.log.verbose('User code verification.')
        session, data = self.server.getSessionByOption('user_code', userCode)
        if not session:
          return 'Device flow authorization session %s expired.' % session
        authURL = self.server.metadata['authorization_endpoint']
        authURL += '?%s&client_id=%s&user_code=%s' % (data['request'].query,
                                                      data['client_id'], userCode)
        return self.server.handle_response(302, {}, [("Location", authURL)])

      # Device code entry interface
      action = "function action() { document.getElementById('form').action="
      action += "'%s/' + document.getElementById('code').value + '?%s' }" % (self.currentPath, self.request.query)
      with self.doc:
        dom.div(dom.form(dom._input(type="text", id="code", name="user_code", style=self.css_big_text),
                         dom.button('Submit', type="submit", style=self.css_big_text), id="form", onsubmit="action()"),
                style=self.css_main)
        dom.script(action)
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
        | provider       | query  | identity provider to autorize (optional)  | CheckIn                               |
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
    grant = None
    if self.request.method == 'GET':
      try:
        grant, request = self.server.validate_consent_request(self.request, None)
      except OAuth2Error as error:
        return self.server.handle_error_response(None, error)

    # Research supported IdPs
    result = getProvidersForInstance('Id')
    if not result['OK']:
      return result
    idPs = result['Value']
    if not idPs:
      return S_ERROR('No identity providers found.')

    idP = self.get_argument('provider', provider)
    if not idP:
      if len(idPs) == 1:
        idP = idPs[0]
      else:
        # Choose IdP interface
        return self.__chooseIdP(idPs)

    self.log.debug('Start authorization with', idP)

    # Check IdP
    if idP not in idPs:
      return '%s is not registered in DIRAC.' % idP

    # # TODO: integrate it with AuthServer
    # # IMPLICIT test for joopiter
    # if grant.GRANT_TYPE == 'implicit' and self.get_argument('access_token', None):
    #   result = self.__implicitFlow()
    #   if not result['OK']:
    #     return result
    #   return self.server.create_authorization_response(self.request, result['Value'])

    request = createOAuth2Request(self.request).toDict()
    request.pop('headers')
    request.get('body')
    # Submit second auth flow through IdP
    mainSession = {'scope': self.get_argument('scope', None), 'state': self.get_argument('state'),
                   'request': request}
    return self.server.getIdPAuthorization(idP, mainSession)

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

    # Check current auth session
    if not state or not self.getSession(state):
      return S_ERROR("%s session is expired." % state)
    # Current auth session
    currentAuthSession = json.loads(self.getSession(state))
    # Base DIRAC client auth session
    mainAuthSession = json.loads(self.getSession(currentAuthSession['mainSessionState']))

    # User info
    username, userID, profile = (None, None, None)
    # Added group
    choosedScopeList = self.get_arguments('chooseScope', None)

    # Read requested groups by DIRAC client or user
    requestedScopesList = mainAuthSession.get('scope', '').split()
    if choosedScopeList:
      userID = self.get_argument('userID', None)
      username = self.get_argument('username', None)
      requestedScopesList = list(set(requestedScopesList + choosedScopeList))
    requestedGroups = [s.split(':')[1] for s in requestedScopesList if s.startswith('g:')]
    self.log.debug('Next groups has been requeted:', ', '.join(requestedGroups))

    if not choosedScopeList:
      # Parse result of the second authentication flow
      self.log.info('%s session, parsing authorization response:\n' % state, '\n'.join([self.request.uri,
                                                                                        self.request.query,
                                                                                        self.request.body,
                                                                                        str(self.request.headers)]))
      
      result = self.server.parseIdPAuthorizationResponse(self.request, currentAuthSession)
      if not result['OK']:
        return result
      # Return main session flow
      username, userID, profile = result['Value']

    self.log.debug('Next groups has been found for %s:' % username, ', '.join(requestedGroups))

    # Researche Group
    result = gProxyManager.getGroupsStatusByUsername(username, requestedGroups)
    if not result['OK']:
      return result
    groupStatuses = result['Value']
    if not groupStatuses:
      return S_ERROR('No groups found.')
    self.log.debug('The state of %s user groups has been checked:' % username, pprint.pformat(groupStatuses))

    if not requestedGroups:
      if len(groupStatuses) == 1:
        requestedGroups = [groupStatuses[0]]
      else:
        # Choose group interface
        return self.__chooseGroup(groupStatuses, username, userID)

    for group in requestedGroups:
      status = groupStatuses[group]['Status']
      action = groupStatuses[group].get('Action')
      comment = groupStatuses[group].get('Comment')

      if status == 'needToAuth':
        # Submit second auth flow through IdP
        idP = action[1][0]
        return self.server.getIdPAuthorization(idP, mainAuthSession)

      if status not in ['ready', 'unknown']:
        self.log.verbose('%s group has bad status: %s; %s' % (group, status, comment))

    # RESPONSE to basic DIRAC client request
    request = createOAuth2Request(mainAuthSession['request'])
    request.data['scope'] = ' '.join(requestedScopesList)
    # Save session to DB
    mainAuthSession.update(dict(id=currentAuthSession['mainSessionState'], user_id=userID))
    self.server.addSession(mainAuthSession)
    return self.server.create_authorization_response(request, {'username': username, 'user_id': userID})

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

  # def __implicitFlow(self):
  #   """ For implicit flow
  #   """
  #   accessToken = self.get_argument('access_token')
  #   providerName = self.get_argument('provider')
  #   result = self.server.idps.getIdProvider(providerName)
  #   if not result['OK']:
  #     return result
  #   provObj = result['Value']

  #   # get keys
  #   try:
  #     r = requests.get(provObj.metadata['jwks_uri'], verify=False)
  #     r.raise_for_status()
  #     jwks = r.json()
  #   except requests.exceptions.Timeout:
  #     return S_ERROR('Authentication server is not answer.')
  #   except requests.exceptions.RequestException as ex:
  #     return S_ERROR(r.content or ex)
  #   except Exception as ex:
  #     return S_ERROR('Cannot read response: %s' % ex)

  #   # Get claims and verify signature
  #   claims = jwt.decode(accessToken, jwks)
  #   # Verify token
  #   claims.validate()

  #   result = Registry.getUsernameForID(claims.sub)
  #   if not result['OK']:
  #     return S_ERROR("User is not valid.")
  #   username = result['Value']

  #   # Check group
  #   group = [s.split(':')[1] for s in self.get_arguments('scope') if s.startswith('g:')][0]

  #   # Researche Group
  #   result = gProxyManager.getGroupsStatusByUsername(username, [group])
  #   if not result['OK']:
  #     return result
  #   groupStatuses = result['Value']

  #   status = groupStatuses[group]['Status']
  #   if status not in ['ready', 'unknown']:
  #     return S_ERROR('%s - bad group status' % status)
  #   return S_OK(claims.sub)

  # def __validateToken(self):
  #   """ Load client certchain in DIRAC and extract informations.

  #       The dictionary returned is designed to work with the AuthManager,
  #       already written for DISET and re-used for HTTPS.

  #       :returns: a dict containing the return of :py:meth:`DIRAC.Core.Security.X509Chain.X509Chain.getCredentials`
  #                 (not a DIRAC structure !)
  #   """
  #   auth = self.request.headers.get("Authorization")
  #   credDict = {}
  #   if not auth:
  #     raise Exception('401 Unauthorize')
  #   # If present "Authorization" header it means that need to use another then certificate authZ
  #   authParts = auth.split()
  #   authType = authParts[0]
  #   if len(authParts) != 2 or authType.lower() != "bearer":
  #     raise Exception("Invalid header authorization")
  #   token = authParts[1]
  #   # Read public key of DIRAC auth service
  #   with open('/opt/dirac/etc/grid-security/jwtRS256.key.pub', 'rb') as f:
  #     key = f.read()
  #   # Get claims and verify signature
  #   claims = jwt.decode(token, key)
  #   # Verify token
  #   claims.validate()
  #   result = Registry.getUsernameForID(claims.sub)
  #   if not result['OK']:
  #     raise Exception("User is not valid.")
  #   claims['username'] = result['Value']
  #   return claims

  def __chooseIdP(self, idPs):
    with self.doc:
      with dom.div(style=self.css_main):
        with dom.div('Choose identity provider', style=self.css_align_center):
          for idP in idPs:
            # data: Status, Comment, Action
            dom.button(dom.a(idP, href='%s/%s?%s' % (self.currentPath, idP, self.request.query)), cls='button')
    return Template(self.doc.render()).generate()

  def __chooseGroup(self, groupStatuses, username, userID):
    if not groupStatuses:
      return S_ERROR('No groups found.')
    elif len(groupStatuses) == 1:
      groups = [groupStatuses[0]]
    else:
      # Choose group interface
      with self.doc:
        with dom.div(style=self.css_main):
          with dom.div('Choose group', style=self.css_align_center):
            for group, data in groupStatuses.items():
              # data: Status, Comment, Action
              dom.button(dom.a(group, href='%s?state=%s&chooseScope=g:%s&username=%s&userID=%s' % (self.currentPath,
                                                                                         self.get_argument('state'),
                                                                                         group, username, userID)),
                         cls='button')
      return Template(self.doc.render()).generate()