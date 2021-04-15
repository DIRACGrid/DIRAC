""" This handler basically provides a REST interface to interact with the OAuth 2 authentication server
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pprint
import requests
from io import open

import dominate
from dominate.tags import link, style, head, div, a, button, form, _input, script
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

__RCSID__ = "$Id$"


class AuthHandler(TornadoREST):
  # TODO: docs
  # Authorization access to all methods handled by AuthServer instance
  USE_AUTHZ_GRANTS = ['VISITOR']
  SYSTEM = 'Framework'
  AUTH_PROPS = 'all'
  LOCATION = "/DIRAC/auth"

  button_style = """
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

  def initializeRequest(self):
    """ Called at every request """
    self.currentPath = self.request.protocol + "://" + self.request.host + self.request.path

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
    name = ClientRegistrationEndpoint.ENDPOINT_NAME
    return self.__response(**self.server.create_endpoint_response(name, self.request))

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
      name = DeviceAuthorizationEndpoint.ENDPOINT_NAME
      return self.__response(**self.server.create_endpoint_response(name, self.request))

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
        return self.__response(code=302, headers=HTTPHeaders({"Location": authURL}))

      # Device code entry interface
      doc = dominate.document(title='Authentication')
      with doc.head:
        link(rel='stylesheet', href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css")
        style(self.button_style)
      _div = div(style='display:flex;justify-content:center;align-items:center;padding:28px;font-size:28px;')
      _div.add('Please, enter user code:')
      _f = form(id="user_code_form", onsubmit="verification_uri_complete()")
      _f.add(_input(type="text", id="user_code", name="user_code"))
      _f.add(button('Submit', type="submit", id="submit"))
      _div.add(_f)
      doc.add(_div)
      doc.add(script("""function verification_uri_complete(){
          var form = document.getElementById('user_code_form');
          form.action = "{{url}}/" + document.getElementById('user_code').value + "?{{query}}";
          }""".format(url=self.currentPath, query=self.request.query)))

      return Template(doc.render()).generate()

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
        grant, _ = self.server.validate_consent_request(self.request, None)
      except OAuth2Error as e:
        session = self.get_argument('state')
        if session:
          self.server.updateSession(session, Status='failed', Comment=': '.join([e.error, e.description]))
        return "%s</br>%s" % (e.error, e.description)

    # Research supported IdPs
    result = getProvidersForInstance('Id')
    if not result['OK']:
      return result
    idPs = result['Value']

    idP = self.get_argument('provider', provider)
    if not idP:
      # Choose IdP interface
      doc = dominate.document(title='Authentication')
      with doc.head:
        link(rel='stylesheet', href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css")
        style(self.button_style)
      with doc:
        div(style='display:flex;justify-content:center;align-items:center;padding:28px;font-size:28px;').add('Please, choose a identity provider:')
        with div(style='display:flex;justify-content:center;align-items:center;').add(div()):
          for idP in idPs:
            # data: Status, Comment, Action
            href = '{url}/{idP}?{query}'.format(url=self.currentPath, idP=idP, query=self.request.query)
            button(cls='button').add(a(idP, href=href))
      return Template(doc.render()).generate()

    self.log.debug('Start authorization with', idP)

    # Check IdP
    if idP not in idPs:
      return '%s is not registered in DIRAC.' % idP

    # TODO: integrate it with AuthServer
    # IMPLICIT test for joopiter
    if grant.GRANT_TYPE == 'implicit' and self.get_argument('access_token', None):
      result = self.__implicitFlow()
      if not result['OK']:
        return result
      return self.__response(**self.server.create_authorization_response(self.request, result['Value']))

    # Submit second auth flow through IdP
    result = self.server.getIdPAuthorization(idP, self.get_argument('state'))
    if not result['OK']:
      return result
    self.log.verbose('Redirect to', result['Value'])
    return self.__response(code=302, headers=HTTPHeaders({"Location": result['Value']}))

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
    # Redirect endpoint for response
    self.log.debug('REDIRECT RESPONSE:\n', '\n'.join([self.request.uri,
                                                      self.request.query,
                                                      self.request.body,
                                                      str(self.request.headers)]))

    # Try to parse IdP session id
    session = self.get_argument('state')

    # Try to catch errors
    error = self.get_argument('error', None)
    if error:
      description = self.get_argument('error_description', '')
      self.server.updateSession(session, Status='failed', Comment=': '.join([error, description]))
      return '%s session crashed with error:\n%s\n%s' % (session, error, description)

    # Added group
    choosedScope = self.get_arguments('chooseScope', None)

    if not choosedScope:
      # Parse result of the second authentication flow
      self.log.info(session, 'session, parsing authorization response %s' % self.get_arguments)
      result = self.server.parseIdPAuthorizationResponse(self.request, session)
      if not result['OK']:
        self.server.updateSession(session, Status='failed', Comment=result['Message'])
        return result
      # Return main session flow
      session = result['Value']

    # Main session metadata
    sessionDict = self.server.getSession(session)
    if not sessionDict:
      return "%s session is expired." % session
    username = sessionDict['username']
    request = sessionDict['request']
    userID = sessionDict['userID']

    scopes = request.data['scope'].split()
    if choosedScope:
      # Modify scope in main session
      scopes.extend(choosedScope)
      request.data['scope'] = ' '.join(list(set(scopes)))
      self.server.updateSession(session, request=request)

    groups = [s.split(':')[1] for s in scopes if s.startswith('g:')]
    self.log.debug('Next groups has been found for %s:' % username, ', '.join(groups))

    # Researche Group
    result = gProxyManager.getGroupsStatusByUsername(username, groups)
    if not result['OK']:
      self.server.updateSession(session, Status='failed', Comment=result['Message'])
      return result
    groupStatuses = result['Value']
    self.log.debug('The state of %s user groups has been checked:' % username, pprint.pformat(groupStatuses))

    if not groups:
      # Choose group interface
      doc = dominate.document(title='Authentication')
      with doc.head:
        link(rel='stylesheet', href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css")
        style(self.button_style)
      with doc:
        div(style='display:flex;justify-content:center;align-items:center;padding:28px;font-size:28px;').add('Please, choose a group:')
        with div(style='display:flex;justify-content:center;align-items:center;').add(div()):
          for group, data in groupStatuses.items():
            # data: Status, Comment, Action
            href = '{url}?state={state}&chooseScope=g:{g}'.format(url=self.currentPath, state=session, g=group)
            button(cls='button').add(a(group, href=href))
      return Template(doc.render()).generate()

    for group in groups:
      status = groupStatuses[group]['Status']
      action = groupStatuses[group].get('Action')
      comment = groupStatuses[group].get('Comment')

      if status == 'needToAuth':
        # Submit second auth flow through IdP
        idP = action[1][0]
        result = self.server.getIdPAuthorization(idP, session)
        if not result['OK']:
          self.server.updateSession(session, Status='failed', Comment=result['Message'])
          return result['Message']
        self.log.verbose('Redirect to', result['Value'])
        return self.__response(code=302, headers=HTTPHeaders({"Location": result['Value']}))

      if status not in ['ready', 'unknown']:
        self.log.verbose('%s group has bad status: %s; %s' % (group, status, comment))

    # self.server.updateSession(session, Status='authed')

    # RESPONSE
    return self.__response(**self.server.create_authorization_response(request, username))

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
    return self.__response(**self.server.create_token_response(self.request))

  def __implicitFlow(self):
    """ For implicit flow
    """
    accessToken = self.get_argument('access_token')
    providerName = self.get_argument('provider')
    result = self.server.idps.getIdProvider(providerName)
    if not result['OK']:
      return result
    provObj = result['Value']

    # get keys
    try:
      r = requests.get(provObj.metadata['jwks_uri'], verify=False)
      r.raise_for_status()
      jwks = r.json()
    except requests.exceptions.Timeout:
      return S_ERROR('Authentication server is not answer.')
    except requests.exceptions.RequestException as ex:
      return S_ERROR(r.content or ex)
    except Exception as ex:
      return S_ERROR('Cannot read response: %s' % ex)

    # Get claims and verify signature
    claims = jwt.decode(accessToken, jwks)
    # Verify token
    claims.validate()

    result = Registry.getUsernameForID(claims.sub)
    if not result['OK']:
      return S_ERROR("User is not valid.")
    username = result['Value']

    # Check group
    group = [s.split(':')[1] for s in self.get_arguments('scope') if s.startswith('g:')][0]

    # Researche Group
    result = gProxyManager.getGroupsStatusByUsername(username, [group])
    if not result['OK']:
      return result
    groupStatuses = result['Value']

    status = groupStatuses[group]['Status']
    if status not in ['ready', 'unknown']:
      return S_ERROR('%s - bad group status' % status)
    return S_OK(claims.sub)

  def __response(self, *args, **kwargs):
    """ Return response as HTTPResponse object """
    return HTTPResponse(HTTPRequest(self.request.full_url(), self.request.method), *args, **kwargs)

  def __validateToken(self):
    """ Load client certchain in DIRAC and extract informations.

        The dictionary returned is designed to work with the AuthManager,
        already written for DISET and re-used for HTTPS.

        :returns: a dict containing the return of :py:meth:`DIRAC.Core.Security.X509Chain.X509Chain.getCredentials`
                  (not a DIRAC structure !)
    """
    auth = self.request.headers.get("Authorization")
    credDict = {}
    if not auth:
      raise Exception('401 Unauthorize')
    # If present "Authorization" header it means that need to use another then certificate authZ
    authParts = auth.split()
    authType = authParts[0]
    if len(authParts) != 2 or authType.lower() != "bearer":
      raise Exception("Invalid header authorization")
    token = authParts[1]
    # Read public key of DIRAC auth service
    with open('/opt/dirac/etc/grid-security/jwtRS256.key.pub', 'rb') as f:
      key = f.read()
    # Get claims and verify signature
    claims = jwt.decode(token, key)
    # Verify token
    claims.validate()
    result = Registry.getUsernameForID(claims.sub)
    if not result['OK']:
      raise Exception("User is not valid.")
    claims['username'] = result['Value']
    return claims
