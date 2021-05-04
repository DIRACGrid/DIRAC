""" Handler to provide REST APIs to manage user authentication.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from pprint import pprint
import requests

from tornado.template import Template
from tornado.httputil import HTTPHeaders
from tornado.httpclient import HTTPResponse, HTTPRequest

from authlib.jose import jwk, jwt
# from authlib.jose import JsonWebKey
from authlib.oauth2.base import OAuth2Error

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Tornado.Server.TornadoREST import TornadoREST
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.FrameworkSystem.private.authorization.AuthServer import AuthServer
from DIRAC.FrameworkSystem.private.authorization.utils.Clients import ClientRegistrationEndpoint
from DIRAC.FrameworkSystem.private.authorization.grants.DeviceFlow import DeviceAuthorizationEndpoint
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getProvidersForInstance
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import ResourceProtector

__RCSID__ = "$Id$"


class AuthHandler(TornadoREST):
  AUTH_PROPS = 'all'
  LOCATION = "/DIRAC/auth"

  @classmethod
  def initializeHandler(cls, serviceInfo):
    """
      This may be overwritten when you write a DIRAC service handler
      And it must be a class method. This method is called only one time,
      at the first request

      :param dict ServiceInfoDict: infos about services, it contains
                                    'serviceName', 'serviceSectionPath',
                                    'csPaths' and 'URL'
    """
    cls.server = AuthServer()
  
  def initializeRequest(self):
    self.currentPath = self.request.protocol + "://" + self.request.host + self.request.path

  path_index = ['.well-known/(oauth-authorization-server|openid-configuration)']
  def web_index(self, instance):
    """ Well known endpoint

        GET: /.well-known/openid-configuration
        GET: /.well-known/oauth-authorization-server
    """
    print('------ web_.well-known --------')
    if self.request.method == "GET":
      return dict(self.server.metadata)
    print('-----> web_.well-known <-------')

  def web_jwk(self):
    """ JWKs
    """
    print('------ web_jwk --------')
    if self.request.method == "GET":
      with open('/opt/dirac/etc/grid-security/jwtRS256.key.pub', 'rb') as f:
        key = f.read()
      # # # For newer version
      # # # key = JsonWebKey.import_key(key, {'kty': 'RSA'})
      # # # self.finish(key.as_dict())
      return {'keys': [jwk.dumps(key, kty='RSA', alg='RS256')]}
    print('-----> web_jwk <-------')
  
  #auth_userinfo = ["authenticated"]
  def web_userinfo(self):
    print('------ web_userinfo --------')
    token = ResourceProtector().acquire_token(self.request, '')
    return {'sub': token.sub, 'issuer': token.issuer, 'group': token.groups[0]}
    # return {'username': credDict['username'],
    #         'group': credDict['group']}
    # return self.__validateToken()
    print('-----> web_userinfo <-------')

  def web_register(self):
    """ Client registry

        POST: /register?client_id=.. &scope=.. &redirect_uri=..
        
        requests.post('https://marosvn32.in2p3.fr/DIRAC/auth/register', json={'grant_types': ['implicit'], 'response_types': ['token'], 'redirect_uris': ['https://dirac.egi.eu'], 'token_endpoint_auth_method': 'none'}, verify=False).text
        requests.post('https://marosvn32.in2p3.fr/DIRAC/auth/register', json={"scope":"changeGroup","token_endpoint_auth_method":"client_secret_basic","grant_types":["authorization_code","refresh_token"],"redirect_uris":["https://marosvn32.in2p3.fr/DIRAC","https://marosvn32.in2p3.fr/DIRAC/loginComplete"],"response_types":["token","id_token token","code"]}, verify=False).text
    """
    print('------ web_register --------')
    name = ClientRegistrationEndpoint.ENDPOINT_NAME
    return self.__response(**self.server.create_endpoint_response(name, self.request))
    print('-----> web_register <-------')

  path_device = ['([A-z0-9-_]*)']
  def web_device(self, userCode=None):
    """ Device authorization flow

        POST: /device?client_id=.. &scope=..
          # group - optional
          provider - optional
        
        GET: /device/<user code>
    """
    print('------ web_device --------')
    if self.request.method == 'POST':
      name = DeviceAuthorizationEndpoint.ENDPOINT_NAME
      return self.__response(**self.server.create_endpoint_response(name, self.request))

    elif self.request.method == 'GET':
      userCode = self.get_argument('user_code', userCode)
      if userCode:
        session, data = self.server.getSessionByOption('user_code', userCode)
        if not session:
          return '%s authorization session expired.' % session
        authURL = self.server.metadata['authorization_endpoint']
        authURL += '?%s&client_id=%s&user_code=%s' % (data['request'].query,
                                                      data['client_id'], userCode)
        return self.__response(code=302, headers=HTTPHeaders({"Location": authURL}))
      
      t = Template('''<!DOCTYPE html>
      <html>
        <head>
          <title>Authentication</title>
          <meta charset="utf-8" />
        </head>
        <body>
          <form id="user_code_form" onsubmit="verification_uri_complete()">
            <input type="text" id="user_code" name="user_code">
            <button type="submit" id="submit">Submit</button>
          </form>
          <script>
            function verification_uri_complete(){
              var form = document.getElementById('user_code_form');
              form.action = "{{url}}/" + document.getElementById('user_code').value + "{{query}}";
            }
          </script>
        </body>
      </html>''')
      return t.generate(url=self.currentPath, query='?' + self.request.query)
    print('-----> web_device <-------')

  path_authorization = ['([A-z0-9]*)']
  def web_authorization(self, provider=None):
    """ Authorization endpoint

        GET: /authorization/< DIRACs IdP >?client_id=.. &response_type=(code|device)&scope=..      #group=..

        Device flow:
          &user_code=..                         (required)

        Authentication code flow:
          &scope=..                             (optional)
          &redirect_uri=..                      (optional)
          &state=..                             (main session id, optional)
          &code_challenge=..                    (PKCE, optional)
          &code_challenge_method=(pain|S256)    ('pain' by default, optional)
    """
    print('------ web_authorization --------')
    grant = None
    if self.request.method == 'GET':
      try:
        grant, _ =self.server.validate_consent_request(self.request, None)
      except OAuth2Error as error:
        return "%s</br>%s" % (error.error, error.description)

    # Research supported IdPs
    result = getProvidersForInstance('Id')
    if not result['OK']:
      return result
    idPs = result['Value']

    idP = self.get_argument('provider', provider)
    if not idP:
      # Choose IdP
      t = Template('''<!DOCTYPE html>
      <html>
        <head>
          <title>Authentication</title>
          <meta charset="utf-8" />
        </head>
        <body>
          <ul>
            {% for idP in idPs %}
              <li> <a href="{{url}}/{{idP}}{{query}}">{{idP}}</a> </li>
            {% end %}
          <ul>
        </body>
      </html>''')
      return t.generate(url=self.currentPath, query='?' + self.request.query, idPs=idPs)

    # Check IdP
    if idP not in idPs:
      return '%s is not registered in DIRAC.' % idP

    # IMPLICIT test
    if grant.GRANT_TYPE == 'implicit' and self.get_argument('access_token', None):
      result = self.__implicitFlow()
      if not result['OK']:
        return result
      return self.__response(**self.server.create_authorization_response(self.request, result['Value']))

    # Submit second auth flow through IdP
    result = self.server.getIdPAuthorization(idP, self.get_argument('state'))
    if not result['OK']:
      return result
    self.log.notice('Redirect to', result['Value'])
    return self.__response(code=302, headers=HTTPHeaders({"Location": result['Value']}))
    print('-----> web_authorization <-------')

  def web_redirect(self):
    print('------ web_redirect --------')
    # Redirect endpoint for response
    self.log.info('REDIRECT RESPONSE:\n', self.request)
    self.log.info(self.request.uri)
    self.log.info(self.request.query)
    self.log.info(self.request.body)
    self.log.info(self.request.headers)
    # Try to catch errors
    error = self.get_argument('error', None)
    if error:
      description = self.get_argument('error_description', '')
      self.server.updateSession(session, Status='failed', Comment=': '.join([error, description]))
      return '%s session crashed with error:\n%s\n%s' % (session, error, description)

    # Try to parse IdP session id
    session = self.get_argument('state')

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
    print('GROUPS: %s' % groups)

    # Researche Group
    result = gProxyManager.getGroupsStatusByUsername(username, groups)
    if not result['OK']:
      self.server.updateSession(session, Status='failed', Comment=result['Message'])
      return result
    groupStatuses = result['Value']
    print('======= Group STATUSES:')
    pprint(groupStatuses)

    if not groups:
      t = Template('''<!DOCTYPE html>
      <html>
        <head>
          <title>Authentication</title>
          <meta charset="utf-8" />
        </head>
        <body>
          Please choose group:
          <ul>
            {% for group, data in groups.items() %}
              <li> <a href="{{url}}?state={{session}}&chooseScope=g:{{group}}">{{group}}</a>
                : {{data['Status']}} </br>
                {{data['Comment']}} </br>
                {% if data.get('Action', '') %}
                  {{data['Action'][0]}} : {{data['Action'][1][0]}}
                {% end %}
              </li>
            {% end %}
          <ul>
        </body>
      </html>''')
      return t.generate(url=self.currentPath, session=session, groups=groupStatuses)

    for group in groups:
      status = groupStatuses[group]['Status']
      action = groupStatuses[group].get('Action')
    
      if status == 'needToAuth':
        # Submit second auth flow through IdP
        idP = action[1][0]
        result = self.server.getIdPAuthorization(idP, session)
        if not result['OK']:
          self.server.updateSession(session, Status='failed', Comment=result['Message'])
          return result['Message']
        self.log.notice('Redirect to', result['Value'])
        return self.__response(code=302, headers=HTTPHeaders({"Location": result['Value']}))

      if status not in ['ready', 'unknown']:
        return '%s - bad group status' % status

    # self.server.updateSession(session, Status='authed')

    ###### RESPONSE
    return self.__response(**self.server.create_authorization_response(request, username))
    print('-----> web_redirect <-------')

  def web_token(self):
    print('------ web_token --------')
    return self.__response(**self.server.create_token_response(self.request))
    print('-----> web_token <-------')

  def __implicitFlow(self):
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
  
  def _gatherPeerCredentials(self):
    return {}