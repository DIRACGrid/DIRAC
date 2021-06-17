""" This class provides authorization server activity. """
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import sys
import time
import pprint
import logging
from dominate import document, tags as dom
from tornado.template import Template

from authlib.jose import jwt
from authlib.oauth2 import HttpRequest, AuthorizationServer as _AuthorizationServer
from authlib.oauth2.base import OAuth2Error
from authlib.common.security import generate_token
from authlib.oauth2.rfc7636 import CodeChallenge
from authlib.oauth2.rfc8414 import AuthorizationServerMetadata
from authlib.oauth2.rfc6749.util import scope_to_list, list_to_scope

from DIRAC.FrameworkSystem.private.authorization.grants.RevokeToken import RevocationEndpoint
from DIRAC.FrameworkSystem.private.authorization.grants.RefreshToken import RefreshTokenGrant
from DIRAC.FrameworkSystem.private.authorization.grants.DeviceFlow import (DeviceAuthorizationEndpoint,
                                                                           DeviceCodeGrant)
from DIRAC.FrameworkSystem.private.authorization.grants.AuthorizationCode import AuthorizationCodeGrant
from DIRAC.FrameworkSystem.private.authorization.utils.Clients import getDIACClientByID
from DIRAC.FrameworkSystem.private.authorization.utils.Requests import OAuth2Request, createOAuth2Request

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.FrameworkSystem.DB.AuthDB import AuthDB
from DIRAC.Resources.IdProvider.Utilities import getProvidersForInstance
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
from DIRAC.ConfigurationSystem.Client.Utilities import getAuthorizationServerMetadata, isDownloadablePersonalProxy
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import (getUsernameForDN, getEmailsForGroup, wrapIDAsDN,
                                                               getDNForUsername, getIdPForGroup)
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import ProxyManagerClient
from DIRAC.FrameworkSystem.Client.TokenManagerClient import TokenManagerClient

log = logging.getLogger('authlib')
log.addHandler(logging.StreamHandler(sys.stdout))
log.setLevel(logging.DEBUG)
log = gLogger.getSubLogger(__name__)


def collectMetadata(issuer=None):
  """ Collect metadata for DIRAC Authorization Server(DAS), a metadata format defines by IETF specification:
      https://datatracker.ietf.org/doc/html/rfc8414#section-2

      :param str issuer: issuer to set

      :return: dict -- dictionary is the AuthorizationServerMetadata object in the same time
  """
  result = getAuthorizationServerMetadata(issuer)
  if not result['OK']:
    raise Exception('Cannot prepare authorization server metadata. %s' % result['Message'])
  metadata = result['Value']
  metadata['jwks_uri'] = metadata['issuer'] + '/jwk'
  metadata['token_endpoint'] = metadata['issuer'] + '/token'
  metadata['userinfo_endpoint'] = metadata['issuer'] + '/userinfo'
  metadata['revocation_endpoint'] = metadata['issuer'] + '/revoke'
  metadata['authorization_endpoint'] = metadata['issuer'] + '/authorization'
  metadata['device_authorization_endpoint'] = metadata['issuer'] + '/device'
  metadata['grant_types_supported'] = ['code', 'authorization_code', 'refresh_token',
                                       'urn:ietf:params:oauth:grant-type:device_code']
  metadata['response_types_supported'] = ['code', 'device', 'token']
  metadata['code_challenge_methods_supported'] = ['S256']
  metadata['scopes_supported'] = ['g:', 'proxy', 'lifetime:']
  return AuthorizationServerMetadata(metadata)


class AuthServer(_AuthorizationServer):
  """ Implementation of the :class:`authlib.oauth2.rfc6749.AuthorizationServer`.

      Initialize::

        server = AuthServer()
  """
  css = {}
  LOCATION = None
  REFRESH_TOKEN_EXPIRES_IN = 24 * 3600

  def __init__(self):
    self.db = AuthDB()
    self.log = log
    self.proxyCli = ProxyManagerClient()
    self.tokenCli = TokenManagerClient()
    self.idps = IdProviderFactory()
    # Privide two authlib methods query_client and save_token
    _AuthorizationServer.__init__(self, query_client=getDIACClientByID, save_token=lambda x, y: None)
    self.generate_token = self.generateProxyOrToken
    self.config = {}
    self.metadata = collectMetadata()
    self.metadata.validate()
    # Register configured grants
    self.register_grant(RefreshTokenGrant)
    self.register_grant(DeviceCodeGrant)
    self.register_endpoint(DeviceAuthorizationEndpoint)
    self.register_endpoint(RevocationEndpoint)
    self.register_grant(AuthorizationCodeGrant, [CodeChallenge(required=True)])

  def addSession(self, session):
    self.db.addSession(session)

  def getSession(self, session):
    self.db.getSession(session)

  def _getScope(self, scope, param):
    """ Get parameter scope

        :param str scope: scope
        :param str param: parameter scope

        :return: str or None
    """
    try:
      return [s.split(':')[1] for s in scope_to_list(scope) if s.startswith('%s:' % param)][0]
    except Exception:
      return None

  def generateProxyOrToken(self, client, grant_type, user=None, scope=None,
                           expires_in=None, include_refresh_token=True):
    """ Generate proxy or tokens after authorization
    """
    group = self._getScope(scope, 'g')
    lifetime = self._getScope(scope, 'lifetime')
    provider = getIdPForGroup(group)

    # Search DIRAC username
    result = getUsernameForDN(wrapIDAsDN(user))
    if not result['OK']:
      raise Exception(result['Message'])
    userName = result['Value']

    if 'proxy' in scope_to_list(scope):
      # Try to return user proxy if proxy scope present in the authorization request
      if not isDownloadablePersonalProxy():
        raise Exception("You can't get proxy, configuration settings(downloadablePersonalProxy) not allow to do that.")
      self.log.debug('Try to query %s@%s proxy%s' % (user, group, ('with lifetime:%s' % lifetime) if lifetime else ''))
      result = getDNForUsername(userName)
      if not result['OK']:
        raise Exception(result['Message'])
      userDNs = result['Value']
      err = []
      for dn in userDNs:
        self.log.debug('Try to get proxy for %s' % dn)
        if lifetime:
          result = self.proxyCli.downloadProxy(dn, group, requiredTimeLeft=int(lifetime))
        else:
          result = self.proxyCli.downloadProxy(dn, group)
        if not result['OK']:
          err.append(result['Message'])
        else:
          self.log.info('Proxy was created.')
          result = result['Value'].dumpAllToString()
          if not result['OK']:
            raise Exception(result['Message'])
          return {'proxy': result['Value']}
      raise Exception('; '.join(err))

    else:
      # Ask TokenManager to generate new tokens for user
      result = self.tokenCli.getToken(userName, group)
      if not result['OK']:
        raise OAuth2Error(result['Message'])
      token = result['Value']

      # Wrap the refresh token and register it to protect against reuse
      result = self.registerRefreshToken(dict(sub=user, scope=scope, provider=provider,
                                              azp=client.get_client_id()), token)
      if not result['OK']:
        raise OAuth2Error(result['Message'])
      return result['Value']

  def __signToken(self, payload):
    """ Sign token

        :param dict payload: payload

        :return: S_OK(str)/S_ERROR()
    """
    result = self.db.getPrivateKey()
    if not result['OK']:
      return result
    key = result['Value']['rsakey']
    kid = result['Value']['kid']
    try:
      return S_OK(jwt.encode(dict(alg='RS256', kid=kid), payload, key))
    except Exception as e:
      self.log.exception(e)
      return S_ERROR(repr(e))

  def registerRefreshToken(self, payload, token):
    """ Register refresh token to protect it from reuse

        :param dict payload: payload
        :param dict token: token as a dictionary

        :return: S_OK(dict)S_ERROR()
    """
    result = self.db.storeRefreshToken(token, payload.get('jti'))
    if result['OK']:
      payload.update(result['Value'])
      result = self.__signToken(payload)
    if not result['OK']:
      if token.get('refresh_token'):
        prov = self.idps.getIdProvider(payload['provider'])
        if prov['OK']:
          prov['Value'].revokeToken(token['refresh_token'])
          prov['Value'].revokeToken(token['access_token'], 'access_token')
      return result
    token['refresh_token'] = result['Value']
    return S_OK(token)

  def getIdPAuthorization(self, providerName, request):
    """ Submit subsession and return dict with authorization url and session number

        :param str providerName: provider name
        :param object request: main session request

        :return: S_OK(response)/S_ERROR() -- dictionary contain response generated by `handle_response`
    """
    result = self.idps.getIdProvider(providerName)
    if not result['OK']:
      return result
    idpObj = result['Value']
    authURL, state, session = idpObj.submitNewSession()
    session['state'] = state
    session['Provider'] = providerName
    session['mainSession'] = request if isinstance(request, dict) else request.toDict()

    self.log.verbose('Redirect to', authURL)
    return self.handle_response(302, {}, [("Location", authURL)], session)

  def parseIdPAuthorizationResponse(self, response, session):
    """ Fill session by user profile, tokens, comment, OIDC authorize status, etc.
        Prepare dict with user parameters, if DN is absent there try to get it.
        Create new or modify existing DIRAC user and store the session

        :param dict response: authorization response
        :param str session: session

        :return: S_OK(dict)/S_ERROR()
    """
    providerName = session.pop('Provider')
    self.log.debug('Try to parse authentification response from %s:\n' % providerName, pprint.pformat(response))
    # Parse response
    result = self.idps.getIdProvider(providerName)
    if not result['OK']:
      return result
    idpObj = result['Value']
    result = idpObj.parseAuthResponse(response, session)
    if not result['OK']:
      return result

    # FINISHING with IdP
    # As a result of authentication we will receive user credential dictionary
    credDict = result['Value']

    self.log.debug("Read profile:", pprint.pformat(credDict))
    # Is ID registred?
    result = getUsernameForDN(credDict['DN'])
    if not result['OK']:
      comment = '%s ID is not registred in the DIRAC.' % credDict['ID']
      result = self.__registerNewUser(providerName, credDict)
      if result['OK']:
        comment += ' Administrators have been notified about you.'
      else:
        comment += ' Please, contact the DIRAC administrators.'
      return S_ERROR(comment)
    credDict['username'] = result['Value']

    # Update token for user. This token will be stored separately in the database and
    # updated from time to time. This token will never be transmitted,
    # it will be used to make exchange token requests.
    result = self.tokenCli.updateToken(idpObj.token, credDict['ID'], idpObj.name)
    return S_OK(credDict) if result['OK'] else result

  def get_error_uris(self, request):
    error_uris = self.config.get('error_uris')
    if error_uris:
      return dict(error_uris)

  def create_oauth2_request(self, request, method_cls=OAuth2Request, use_json=False):
    self.log.debug('Create OAuth2 request', 'with json' if use_json else '')
    return createOAuth2Request(request, method_cls, use_json)

  def create_json_request(self, request):
    return self.create_oauth2_request(request, HttpRequest, True)

  def validate_requested_scope(self, scope, state=None):
    """ See :func:`authlib.oauth2.rfc6749.authorization_server.validate_requested_scope` """
    # We also consider parametric scope containing ":" charter
    extended_scope = list_to_scope([re.sub(r':.*$', ':', s) for s in scope_to_list(scope or '')])
    super(AuthServer, self).validate_requested_scope(extended_scope, state)

  def handle_error_response(self, request, error):
    return self.handle_response(*error(translations=self.get_translations(request),
                                       error_uris=self.get_error_uris(request)), error=True)

  def handle_response(self, status_code=None, payload=None, headers=None, newSession=None, error=None, **actions):
    self.log.debug('Handle authorization response with %s status code:' % status_code, payload)
    self.log.debug('Headers:', headers)
    if newSession:
      self.log.debug('newSession:', newSession)
    return S_OK([[status_code, headers, payload, newSession, error], actions])

  def create_authorization_response(self, response, username):
    result = super(AuthServer, self).create_authorization_response(response, username)
    if result['OK']:
      # Remove auth session
      result['Value'][0][4] = True
    return result

  def validate_consent_request(self, request, provider=None):
    """ Validate current HTTP request for authorization page. This page
        is designed for resource owner to grant or deny the authorization::

        :param object request: tornado request
        :param provider: provider

        :return: response generated by `handle_response` or S_ERROR or html
    """
    if request.method != 'GET':
      return 'Use GET method to access this endpoint.'
    try:
      req = self.create_oauth2_request(request)
      self.log.info('Validate consent request for', req.state)
      grant = self.get_authorization_grant(req)
      self.log.debug('Use grant:', grant)
      grant.validate_consent_request()
      if not hasattr(grant, 'prompt'):
        grant.prompt = None

      # Check Identity Provider
      provider, providerChooser = self.validateIdentityProvider(req, provider)
      if not provider:
        return providerChooser

      # Submit second auth flow through IdP
      return self.getIdPAuthorization(provider, req)
    except OAuth2Error as error:
      return self.handle_error_response(None, error)

  def validateIdentityProvider(self, request, provider):
    """ Check if identity provider registred in DIRAC

        :param object request: request
        :param str provider: provider name

        :return: str, S_OK()/S_ERROR() -- provider name and html page to choose it
    """
    # Research supported IdPs
    result = getProvidersForInstance('Id')
    if not result['OK']:
      return None, result
    idPs = result['Value']

    # Remove settings of the DIRAC AS
    result = getProvidersForInstance('Id', 'DIRAC')
    if not result['OK']:
      return None, result
    for dCli in result['Value']:
      if dCli in idPs:
        idPs.remove(dCli)

    if not idPs:
      return None, S_ERROR('No identity providers found.')

    if not provider:
      if len(idPs) == 1:
        return idPs[0], None
      # Choose IdP interface
      doc = document('DIRAC authentication')
      with doc.head:
        dom.link(rel='stylesheet',
                 href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css")
        dom.style(self.css['CSS'])
      with doc:
        with dom.div(style=self.css['css_main']):
          with dom.div('Choose identity provider', style=self.css['css_align_center']):
            for idP in idPs:
              # data: Status, Comment, Action
              dom.button(dom.a(idP, href='%s/authorization/%s?%s' % (self.LOCATION, idP, request.query)),
                         cls='button')
      return None, self.handle_response(payload=Template(doc.render()).generate())

    # Check IdP
    if provider not in idPs:
      return None, S_ERROR('%s is not registered in DIRAC.' % provider)

    return provider, None

  def __registerNewUser(self, provider, userProfile):
    """ Register new user

        :param str provider: provider
        :param dict userProfile: user information dictionary

        :return: S_OK()/S_ERROR()
    """
    from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

    username = userProfile['ID']

    mail = {}
    mail['subject'] = "[SessionManager] User %s to be added." % username
    mail['body'] = 'User %s was authenticated by ' % username
    mail['body'] += provider
    mail['body'] += "\n\nAuto updating of the user database is not allowed."
    mail['body'] += " New user %s to be added," % username
    mail['body'] += "with the following information:\n"
    mail['body'] += "\nUser ID: %s\n" % username
    mail['body'] += "\nUser profile:\n%s" % pprint.pformat(userProfile)
    mail['body'] += "\n\n------"
    mail['body'] += "\n This is a notification from the DIRAC AuthManager service, please do not reply.\n"
    result = S_OK()
    for addresses in getEmailsForGroup('dirac_admin'):
      result = NotificationClient().sendMail(addresses, mail['subject'], mail['body'], localAttempt=False)
      if not result['OK']:
        self.log.error(result['Message'])
    if result['OK']:
      self.log.info(result['Value'], "administrators have been notified about a new user.")
    return result
