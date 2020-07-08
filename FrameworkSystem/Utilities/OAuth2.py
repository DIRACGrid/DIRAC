""" OAuth2

    OAuth2 included all methods to work with OIDC authentication flow.

    .. _timeouts:
    Most requests to external servers should have a timeout attached,
    in case the server is not responding in a timely manner. By default, requests do not time out unless a timeout value is set explicitly. Without a timeout, your code may hang for minutes or more.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import random
import string
import pprint
# from oauthlib.oauth2 import WebApplicationClient

from requests import Session, exceptions

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Utilities import getAuthAPI
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getProviderInfo

__RCSID__ = "$Id$"


class OAuth2(Session):
  def __init__(self, name=None,
               scope=None, prompt=None,
               issuer=None, jwks_uri=None,
               client_id=None, redirect_uri=None,
               client_secret=None, proxy_endpoint=None,
               token_endpoint=None, providerOfWhat=None,
               scopes_supported=None, userinfo_endpoint=None,
               max_proxylifetime=None, revocation_endpoint=None,
               registration_endpoint=None, grant_types_supported=None,
               authorization_endpoint=None, introspection_endpoint=None,
               response_types_supported=None, **kwargs):
    """ OIDCClient constructor
    """
    super(OAuth2, self).__init__()
    self.exceptions = exceptions
    self.verify = False

    __optns = {}
    self.parameters = {}
    self.parameters['name'] = name or kwargs.get('ProviderName')
    self.log = gLogger.getSubLogger("OAuth2/%s" % self.parameters['name'])

    # Get information from CS
    result = getProviderInfo(self.parameters['name'])
    if not result['OK']:
      raise Exception(result['Message'])
    __csDict = result['Value']

    # Get configuration from providers server
    self.parameters['issuer'] = issuer or kwargs.get('issuer') or __csDict.get('issuer')
    if self.parameters['issuer']:
      result = self.getWellKnownDict()
      if not result['OK']:
        self.log.warn('Cannot get settings remotely:' % result['Message'])
      elif isinstance(result['Value'], dict):
        __optns = result['Value']

    for d in [__csDict, kwargs]:
      for key, value in d.iteritems():
        __optns[key] = value

    # Get redirect URL from CS
    authAPI = getAuthAPI()
    if authAPI:
      redirect_uri = '%s/auth/redirect' % authAPI.strip('/')

    # Check client Id
    self.parameters['client_id'] = client_id or __optns.get('client_id')
    if not self.parameters['client_id']:
      raise Exception('client_id parameter is absent.')

    # Create list of all possible scopes
    self.parameters['scope'] = scope or __optns.get('scope') or []
    if not isinstance(self.parameters['scope'], list):
      self.parameters['scope'] = self.parameters['scope'].split(',')

    # Init main OAuth2 options
    self.parameters['prompt'] = prompt or __optns.get('prompt')
    self.parameters['redirect_uri'] = redirect_uri or __optns.get('redirect_uri')
    self.parameters['client_secret'] = client_secret or __optns.get('client_secret')
    self.parameters['token_endpoint'] = token_endpoint or __optns.get('token_endpoint')
    self.parameters['proxy_endpoint'] = proxy_endpoint or __optns.get('proxy_endpoint')
    self.parameters['scopes_supported'] = scopes_supported or __optns.get('scopes_supported')
    self.parameters['userinfo_endpoint'] = userinfo_endpoint or __optns.get('userinfo_endpoint')
    self.parameters['max_proxylifetime'] = max_proxylifetime or __optns.get('max_proxylifetime') or 86400
    self.parameters['revocation_endpoint'] = revocation_endpoint or __optns.get('revocation_endpoint')
    self.parameters['registration_endpoint'] = registration_endpoint or __optns.get('registration_endpoint')
    self.parameters['authorization_endpoint'] = authorization_endpoint or __optns.get('authorization_endpoint')
    self.parameters['introspection_endpoint'] = introspection_endpoint or __optns.get('introspection_endpoint')

  def get(self, parameter):
    return self.parameters.get(parameter)

  def createAuthRequestURL(self, state, **kwargs):
    """ Create link for authorization and state of authorization session

        :param str state: session number
        :param `**kwargs`: OAuth2 parameters that will be added to request url,
               e.g. authorization_endpoint='http://domain.ua/auth', scope=['openid','profile']

        :return: S_OK(dict)/S_ERROR() -- dict contain URL for authentication and session number
    """
    # Fill arguments for preperation URL
    kwargs['response_type'] = 'code'
    kwargs['state'] = state
    kwargs['client_id'] = self.parameters['client_id']
    kwargs['redirect_uri'] = kwargs.get('redirect_uri') or self.parameters['redirect_uri']
    kwargs['scope'] = kwargs.get('scope') or self.parameters['scope'] or self.parameters['scopes_supported']
    if self.parameters['prompt']:
      kwargs['prompt'] = self.parameters['prompt']

    # Add IdP authorization endpoint
    self.log.info(kwargs['state'], 'session, generate URL for authetication.')
    url = (kwargs.get('authorization_endpoint') or self.parameters['authorization_endpoint']) + '?access_type=offline'
    if not url:
      return S_ERROR('No found authorization endpoint.')

    # Add arguments
    for key, value in kwargs.items():
      url += '&%s=%s' % (key, '+'.join(list(set(v.strip() for v in value))) if isinstance(value, list) else value)
    return S_OK(url)

  def parseAuthResponse(self, code):
    """ Collecting information about user

        :param str code: authorize code that come with response(authorize code flow)

        :result: S_OK(dict)/S_ERROR()
    """
    oaDict = {}

    # Get tokens
    result = self.fetchToken(code)
    if not result['OK']:
      return result
    self.log.debug('Token RESPONSE:\n', pprint.pformat(result['Value']))
    oaDict['Tokens'] = result['Value']

    # Get user profile
    result = self.getUserProfile(oaDict['Tokens']['access_token'])
    if not result['OK']:
      return result
    oaDict['UserProfile'] = result['Value']
    self.log.debug('User profile RESPONSE:\n', pprint.pformat(result['Value']))

    # Get tokens
    result = self.fetchToken(refreshToken=oaDict['Tokens']['refresh_token'])
    if not result['OK']:
      return result
    oaDict['Tokens'] = result['Value']
    self.log.debug('Token RESPONSE:\n', pprint.pformat(result['Value']))

    return S_OK(oaDict)

  def getUserProfile(self, accessToken):
    """ Get user profile

        :param str accessToken: access token

        :return: S_OK(dict)/S_ERROR()
    """
    if not self.parameters['userinfo_endpoint']:
      return S_ERROR('Not found userinfo endpoint.')
    try:
      r = self.request('GET', self.parameters['userinfo_endpoint'],
                       headers={'Authorization': 'Bearer ' + accessToken})
      r.raise_for_status()
      return S_OK(r.json())
    except (self.exceptions.RequestException, ValueError) as e:
      return S_ERROR("%s: %s" % (e.message, r.text))

  def revokeToken(self, accessToken=None, refreshToken=None):
    """ Revoke token

        :param str accessToken: access token
        :param str refreshToken: refresh token

        :return: S_OK()/S_ERROR()
    """
    if not accessToken and not refreshToken:
      return S_ERROR('Not found any token to revocation.')
    if not self.parameters['revocation_endpoint']:
      return S_ERROR('Not found revocation endpoint for %s provider' % self.parameters['name'])
    for key, value in [('access_token', accessToken), ('refresh_token', refreshToken)]:
      if not value:
        continue
      self.params = {'token': key, 'token_type_hint': value}
      try:
        r = self.request('POST', self.parameters['token_endpoint'])
        r.raise_for_status()
      except self.exceptions.RequestException as e:
        return S_ERROR("%s: %s" % (e.message, r.text))
    return S_OK()

  def fetchToken(self, code=None, refreshToken=None):
    """ Update tokens

        :param str code: authorize code that come with response(authorize code flow)
        :param str refreshToken: refresh token

        :return: S_OK(dict)/S_ERROR()
    """
    if not self.parameters['token_endpoint']:
      return S_ERROR('Not found token_endpoint for %s provider' % self.parameters['name'])
    self.params = {'access_type': 'offline'}
    for arg in ['client_id', 'client_secret', 'prompt']:
      self.params[arg] = self.parameters[arg]
    if code:
      if not self.parameters['redirect_uri']:
        return S_ERROR('Not found redirect_uri for %s provider' % self.parameters['name'])
      self.params['code'] = code
      self.params['grant_type'] = 'authorization_code'
      self.params['redirect_uri'] = self.parameters['redirect_uri']
    elif refreshToken:
      self.params['grant_type'] = 'refresh_token'
      self.params['refresh_token'] = refreshToken
    else:
      return S_ERROR('No authorization code or refresh token found.')
    try:
      r = self.request('POST', self.parameters['token_endpoint'],
                       headers={'Content-Type': 'application/x-www-form-urlencoded'})
      r.raise_for_status()
      return S_OK(r.json())
    except (self.exceptions.RequestException, ValueError) as e:
      return S_ERROR("%s: %s" % (e.message, r.text))

  def getWellKnownDict(self, url=None, issuer=None):
    """ Returns OpenID Connect metadata related to the specified authorization server
        of provider, enough one parameter

        :param str wellKnown: complete link to provider oidc configuration
        :param str issuer: base URL of provider

        :return: S_OK(dict)/S_ERROR()
    """
    url = url or self.parameters['issuer'] and '%s/.well-known/openid-configuration' % self.parameters['issuer']
    if not url:
      return S_ERROR('Cannot get %s provider issuer/wellKnow url' % self.parameters['name'])
    try:
      r = self.request('GET', url)
      r.raise_for_status()
      return S_OK(r.json())
    except (self.exceptions.RequestException, ValueError) as e:
      return S_ERROR("%s: %s" % (e.message, r.text))
