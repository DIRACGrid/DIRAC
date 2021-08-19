""" This class provides authorization server activity. """
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import sys
import time
import json
import pprint
import logging
from dominate import document, tags as dom
from tornado.template import Template

import authlib
from authlib.jose import JsonWebKey, jwt
from authlib.oauth2 import HttpRequest, AuthorizationServer as _AuthorizationServer
from authlib.oauth2.base import OAuth2Error
from authlib.oauth2.rfc7636 import CodeChallenge
from authlib.oauth2.rfc6749.util import scope_to_list, list_to_scope

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.FrameworkSystem.DB.AuthDB import AuthDB
from DIRAC.Resources.IdProvider.Utilities import getProvidersForInstance, getProviderInfo
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
from DIRAC.ConfigurationSystem.Client.Utilities import isDownloadablePersonalProxy
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import (getUsernameForDN, getEmailsForGroup, wrapIDAsDN,
                                                               getDNForUsername, getIdPForGroup)
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import ProxyManagerClient
from DIRAC.FrameworkSystem.Client.TokenManagerClient import TokenManagerClient
from DIRAC.Core.Tornado.Server.private.BaseRequestHandler import TornadoResponse
from DIRAC.FrameworkSystem.private.authorization.utils.Clients import getDIRACClients, Client
from DIRAC.FrameworkSystem.private.authorization.utils.Requests import OAuth2Request, createOAuth2Request
from DIRAC.FrameworkSystem.private.authorization.utils.Utilities import collectMetadata, getHTML
from DIRAC.FrameworkSystem.private.authorization.grants.RevokeToken import RevocationEndpoint
from DIRAC.FrameworkSystem.private.authorization.grants.RefreshToken import RefreshTokenGrant
from DIRAC.FrameworkSystem.private.authorization.grants.DeviceFlow import (DeviceAuthorizationEndpoint,
                                                                           DeviceCodeGrant)
from DIRAC.FrameworkSystem.private.authorization.grants.AuthorizationCode import AuthorizationCodeGrant

log = logging.getLogger('authlib')
log.addHandler(logging.StreamHandler(sys.stdout))
log.setLevel(logging.DEBUG)
log = gLogger.getSubLogger(__name__)


class AuthServer(_AuthorizationServer):
  """ Implementation of the :class:`authlib.oauth2.rfc6749.AuthorizationServer`.

      Initialize::

        server = AuthServer()
  """
  LOCATION = None
  REFRESH_TOKEN_EXPIRES_IN = 24 * 3600

  def __init__(self):
    self.db = AuthDB()
    self.log = log
    self.idps = IdProviderFactory()
    self.proxyCli = ProxyManagerClient()
    self.tokenCli = TokenManagerClient()
    self.metadata = collectMetadata()
    self.metadata.validate()
    # args for authlib < 1.0.0: (query_client=self.query_client, save_token=None, metadata=self.metadata)
    # for authlib >= 1.0.0:
    _AuthorizationServer.__init__(self, scopes_supported=self.metadata['scopes_supported'])
    # Skip authlib method save_token and send_signal
    self.save_token = lambda x, y: None
    self.send_signal = lambda *x, **y: None
    self.generate_token = self.generateProxyOrToken
    # Register configured grants
    self.register_grant(RefreshTokenGrant)
    self.register_grant(DeviceCodeGrant)
    self.register_endpoint(DeviceAuthorizationEndpoint)
    self.register_endpoint(RevocationEndpoint)
    self.register_grant(AuthorizationCodeGrant, [CodeChallenge(required=True)])

  # pylint: disable=method-hidden
  def query_client(self, client_id):
    """ Search authorization client.

        :param str clientID: client ID

        :return: client as object or None
    """
    gLogger.debug('Try to query %s client' % client_id)
    clients = getDIRACClients()
    for cli in clients:
      if client_id == clients[cli]['client_id']:
        gLogger.debug('Found %s client:\n' % cli, pprint.pformat(clients[cli]))
        return Client(clients[cli])
    return None

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
      return [s.split(':')[1] for s in scope_to_list(scope) if s.startswith('%s:' % param) and s.split(':')[1]][0]
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
      raise OAuth2Error(result['Message'])
    userName = result['Value']

    if 'proxy' in scope_to_list(scope):
      # Try to return user proxy if proxy scope present in the authorization request
      if not isDownloadablePersonalProxy():
        raise OAuth2Error("You can't get proxy, configuration(downloadablePersonalProxy) not allow to do that.")
      self.log.debug('Try to query %s@%s proxy%s' % (user, group, ('with lifetime:%s' % lifetime) if lifetime else ''))
      result = getDNForUsername(userName)
      if not result['OK']:
        raise OAuth2Error(result['Message'])
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
            raise OAuth2Error(result['Message'])
          return {'proxy': result['Value'].decode() if isinstance(result['Value'], bytes) else result['Value']}
      raise OAuth2Error('; '.join(err))

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
    key = result['Value']
    try:
      return S_OK(jwt.encode(dict(alg='RS256', kid=key.thumbprint()), payload, key).decode('utf-8'))
    except Exception as e:
      self.log.exception(e)
      return S_ERROR(repr(e))

  def readToken(self, token):
    """ Decode self token

        :param str token: token to decode

        :return: S_OK(dict)/S_ERROR()
    """
    result = self.db.getKeySet()
    if not result['OK']:
      return result
    try:
      return S_OK(jwt.decode(token, JsonWebKey.import_key_set(result['Value'].as_dict())))
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

  def getIdPAuthorization(self, provider, request):
    """ Submit subsession and return dict with authorization url and session number

        :param str provider: provider name
        :param object request: main session request

        :return: S_OK(response)/S_ERROR() -- dictionary contain response generated by `handle_response`
    """
    result = self.idps.getIdProvider(provider)
    if not result['OK']:
      return result
    idpObj = result['Value']
    authURL, state, session = idpObj.submitNewSession()
    session['state'] = state
    session['Provider'] = provider
    session['firstRequest'] = request if isinstance(request, dict) else request.toDict()

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
    credDict, payload = result['Value']

    self.log.debug("Read profile:", pprint.pformat(credDict))
    # Is ID registred?
    result = getUsernameForDN(credDict['DN'])
    if not result['OK']:
      comment = 'Your ID is not registred in the DIRAC: %s.' % credDict['ID']
      payload.update(idpObj.getUserProfile().get('Value', {}))
      result = self.__registerNewUser(providerName, payload)
    
      if result['OK']:
        comment += ' Administrators have been notified about you.'
      else:
        comment += ' Please, contact the DIRAC administrators.'
      
      # Notify user about problem
      html = getHTML("unregister")
      # Create HTML page
      with html:
        with dom.div(cls="container"):
          with dom.div(cls="row m-5 justify-content-md-center align-items-center"):
            dom.div(dom.img(src=self.metadata.get('logoURL', ''), cls="card-img p-5"), cls="col-md-4")
            with dom.div(cls="col-md-4"):
              dom.small(dom.i(cls="fa fa-ticket-alt", style="color:green;"))
              dom.small('user code verified.', cls="p-3 h6")
              dom.br()
              dom.small(dom.i(cls="fa fa-user-check", style="color:green;"))
              dom.small('Identity Provider selected.', cls="p-3 h6")
              dom.br()
              dom.small(dom.i(cls="fa fa-exclamation-circle", style="color:red;"))
              dom.small('authorization failed.', cls="p-3 h6")
              dom.br()
              dom.br()
              dom.small(dom.i(cls="fa fa-info"))
              dom.small(comment, cls="p-2")

      return S_ERROR(html.render())
    credDict['username'] = result['Value']

    # Update token for user. This token will be stored separately in the database and
    # updated from time to time. This token will never be transmitted,
    # it will be used to make exchange token requests.
    result = self.tokenCli.updateToken(idpObj.token, credDict['ID'], idpObj.name)
    return S_OK(credDict) if result['OK'] else result

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

  def handle_response(self, status_code=None, payload=None, headers=None, newSession=None):
    """ Handle response

        :param int status_code: http status code
        :param payload: response payload
        :param list headers: headers
        :param dict newSession: session data to store

        :return: TornadoResponse()
    """
    self.log.debug('Handle authorization response with %s status code:' % status_code, payload)
    resp = TornadoResponse(payload)
    if status_code:
      resp.set_status(status_code)  # pylint: disable=no-member
    if headers:
      self.log.debug('Headers:', headers)
      for key, value in headers:
        resp.set_header(key, value)  # pylint: disable=no-member
    if newSession:
      self.log.debug('newSession:', newSession)
      # pylint: disable=no-member
      resp.set_secure_cookie('auth_session', json.dumps(newSession), secure=True, httponly=True)
    if isinstance(payload, dict) and 'error' in payload:
      resp.clear_cookie('auth_session')  # pylint: disable=no-member
    return resp

  def create_authorization_response(self, response, username):
    response = super(AuthServer, self).create_authorization_response(response, username)
    response.clear_cookie('auth_session')
    return response

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
      # Check Identity Provider
      req, result = self.validateIdentityProvider(self.create_oauth2_request(request), provider)
      if not req:
        return result

      self.log.info('Validate consent request for', req.state)
      grant = self.get_authorization_grant(req)
      self.log.debug('Use grant:', grant)
      grant.validate_consent_request()
      if not hasattr(grant, 'prompt'):
        grant.prompt = None

      # Submit second auth flow through IdP
      return self.getIdPAuthorization(req.provider, req)
    except OAuth2Error as error:
      return self.handle_error_response(None, error)

  def validateIdentityProvider(self, request, provider):
    """ Check if identity provider registred in DIRAC

        :param object request: request
        :param str provider: provider name

        :return: str, S_OK()/S_ERROR() -- provider name and html page to choose it
    """
    if provider:
      request.provider = provider

    # Find identity provider for group
    groupProvider = getIdPForGroup(request.group) if request.groups else None

    # If requested access token for group that is not registred in any identity provider
    # or the requested provider does not match the group return error
    if request.group and not groupProvider and 'proxy' not in request.scope:
      self.db.removeSession(request.sessionID)
      return None, S_ERROR('The %s group belongs to the VO that is not tied to any Identity Provider.' % request.group)
    # if provider and provider != groupProvider:
      self.db.removeSession(request.sessionID)
      return None, S_ERROR('The %s group Identity Provider is "%s" and not "%s".' % (request.group, groupProvider,
                                                                                     request.provider))
    # provider = groupProvider

    self.log.debug("Check if %s identity provider registred in DIRAC.." % request.provider)
    # Research supported IdPs
    result = getProvidersForInstance('Id')
    if not result['OK']:
      self.db.removeSession(request.sessionID)
      return None, result

    idPs = result['Value']
    if not idPs:
      self.db.removeSession(request.sessionID)
      return None, S_ERROR('No identity providers found.')

    if request.provider:
      if request.provider not in idPs:
        self.db.removeSession(request.sessionID)
        return None, S_ERROR('%s identity provider is not registered.' % request.provider)
      elif groupProvider and request.provider != groupProvider:
        self.db.removeSession(request.sessionID)
        return None, S_ERROR('The %s group Identity Provider is "%s" and not "%s".' % (request.group, groupProvider,
                                                                                       request.provider))
      return request, None

    # If no identity provider is specified, it must be assigned
    if groupProvider:
      request.provider = groupProvider
      return request, None

    # If only one identity provider is registered, then choose it
    if len(idPs) == 1:
      request.provider = idPs[0]
      return request, None

    # Choose IdP HTML interface
    html = getHTML("IdP selector", style=".card{transition:.3s;}.card:hover{transform:scale(1.05);}")
    # Create HTML page
    with html:
      with dom.div(cls="container"):
        with dom.div(cls="row m-5 justify-content-md-center align-items-center"):
          dom.div(dom.img(src=self.metadata.get('logoURL', ''), cls="card-img p-5"), cls="col-md-4")
          with dom.div(cls="col-md-4"):
            dom.small(dom.i(cls="fa fa-ticket-alt", style="color:green;"))
            dom.small('user code verified.', cls="p-3 h6")
            dom.br()
            dom.small(dom.i(cls="fa fa-user-check"))
            dom.small('Identity Provider selection..', cls="p-3 h6")
            dom.br()
            dom.br()
            dom.small(dom.i(cls="fa fa-info"))
            dom.small('Dirac itself is not an Identity Provider. You will need to select one to continue.', cls="p-2")
        with dom.div(cls="row m-5 justify-content-md-center"):
          for idP in idPs:
            result = getProviderInfo(idP)
            if result['OK']:
              logo = result['Value'].get('logoURL')
              with dom.div(cls="col-lg-4").add(dom.div(cls="card shadow-lg h-100 border-0")):
                with dom.div(cls="row m-2 align-items-center h-100"):
                  with dom.div(cls="col-lg-8"):
                    dom.h2(idP)
                    dom.a(href='%s/authorization/%s?%s' % (self.LOCATION, idP, request.query), cls="stretched-link")
                  if logo:
                    dom.div(dom.img(src=logo, cls="card-img"), cls="col-lg-4")
    # Render into HTML
    return None, html.render()

  def __registerNewUser(self, provider, payload):
    """ Register new user

        :param str provider: provider
        :param dict payload: user information dictionary

        :return: S_OK()/S_ERROR()
    """
    from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

    username = payload['sub']

    mail = {}
    mail['subject'] = "[DIRAC AS] User %s to be added." % username
    mail['body'] = 'User %s was authenticated by %s.' % (username, provider)
    mail['body'] += "\n\nNew user to be added with the following information:\n%s" % pprint.pformat(payload)
    mail['body'] += "\n\nPlease, add '%s' to /Register/Users/<username>/DN option.\n" % wrapIDAsDN(username)
    mail['body'] += "\n\n------"
    mail['body'] += "\n This is a notification from the DIRAC authorization service, please do not reply.\n"
    result = S_OK()
    for addresses in getEmailsForGroup('dirac_admin'):
      result = NotificationClient().sendMail(addresses, mail['subject'], mail['body'], localAttempt=False)
      if not result['OK']:
        self.log.error(result['Message'])
    if result['OK']:
      self.log.info(result['Value'], "administrators have been notified about a new user.")
    return result
