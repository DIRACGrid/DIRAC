""" IdProvider based on OAuth2 protocol
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import pprint
from requests import exceptions
from authlib.common.urls import url_decode
from authlib.common.security import generate_token
from authlib.integrations.requests_client import OAuth2Session
from authlib.oidc.discovery.well_known import get_well_known_url
from authlib.oauth2.rfc8414 import AuthorizationServerMetadata
from authlib.oauth2.rfc6749.parameters import prepare_token_request

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Resources.IdProvider.IdProvider import IdProvider
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getProviderByAlias
from DIRAC.FrameworkSystem.private.authorization.utils.Sessions import Session
from DIRAC.FrameworkSystem.private.authorization.utils.Requests import createOAuth2Request
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import OAuth2Token
from DIRAC.ConfigurationSystem.Client.Utilities import getAuthClients
from DIRAC.FrameworkSystem.private.authorization.utils.ProfileParser import *

__RCSID__ = "$Id$"

DEFAULT_HEADERS = {
    'Accept': 'application/json',
    'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
}


def checkResponse(func):
  def function_wrapper(*args, **kwargs):
    try:
      return func(*args, **kwargs)
    except exceptions.Timeout:
      return S_ERROR('Time out')
    except exceptions.RequestException as ex:
      return S_ERROR(str(ex))
  return function_wrapper


class OAuth2IdProvider(IdProvider, OAuth2Session):
  def __init__(self, name=None, token_endpoint_auth_method=None,
               revocation_endpoint_auth_method=None,
               scope=None, token=None, token_placement='header',
               update_token=None, **parameters):
    """ OIDCClient constructor
    """
    # result = getAuthClients()
    # if not result['OK']:
    #   raise Exception('Cannot get clients dict from configuration.')
    # clientsData = result['Value']
    # if 'redirect_uri' not in parameters:
    #   parameters['redirect_uri'] = clientsData.get('redirect_uri')
    if 'ProviderName' not in parameters:
      parameters['ProviderName'] = name
    IdProvider.__init__(self, **parameters)
    OAuth2Session.__init__(self, token_endpoint_auth_method=token_endpoint_auth_method,
                           revocation_endpoint_auth_method=revocation_endpoint_auth_method,
                           scope=scope, token=token, token_placement=token_placement,
                           update_token=update_token, **parameters)
    # Convert scope to list
    self.parser = ProfileParser(**parameters)
    scope = scope or ''
    self.scope = [s.strip() for s in scope.strip().replace('+', ' ').split(',' if ',' in scope else ' ')]
    self.parameters = parameters
    self.exceptions = exceptions
    self.name = parameters['ProviderName']

    # Add hooks to raise HTTP errors
    self.hooks['response'] = lambda r, *args, **kwargs: r.raise_for_status()
    self.update_token = update_token or self._updateToken
    self.store_token = self._storeToken
    self.metadata_class = AuthorizationServerMetadata
    self.server_metadata_url = parameters.get('server_metadata_url', get_well_known_url(self.metadata['issuer'], True))
    try:
      self.metadata_class(self.metadata).validate()
    except ValueError:
      r = self.request('GET', self.server_metadata_url, withhold_token=True)
      r.raise_for_status()
      metadata = self.metadata_class(r.json())
      for k, v in metadata.items():
        if k not in self.metadata:
          self.metadata[k] = v
      self.metadata_class(self.metadata).validate()

    self.log.debug('"%s" OAuth2 IdP initialization done:\
                   \nclient_id: %s\nclient_secret: %s\nmetadata:\n%s' % (self.name,
                                                                         self.client_id,
                                                                         self.client_secret,
                                                                         pprint.pformat(self.metadata)))

  def _storeToken(self, token):
    if self.sessionManager:
      return self.sessionManager.storeToken(dict(self.token))

  def _updateToken(self, token, refresh_token):
    if self.sessionManager:
      # Here "token" is `OAuth2Token` type
      self.sessionManager.updateToken(dict(token), refresh_token)

  def request(self, *args, **kwargs):
    self.token_endpoint_auth_methods_supported = self.metadata.get('token_endpoint_auth_methods_supported')
    if self.token_endpoint_auth_methods_supported:
      if self.token_endpoint_auth_method not in self.token_endpoint_auth_methods_supported:
        self.token_endpoint_auth_method = self.token_endpoint_auth_methods_supported[0]
    return OAuth2Session.request(self, verify=False, *args, **kwargs)

  def getIDsMetadata(self, ids=None):
    """ Metadata for IDs
    """
    metadata = {}
    result = self.isSessionManagerAble()
    if not result['OK']:
      return result
    result = self.sessionManager.getIdPTokens(self.name, ids)
    if not result['OK']:
      return result
    for token in result['Value']:
      if token['user_id'] in metadata:
        continue
      self.token = token
      result = self.__getUserInfo()
      if result['OK']:
        result = self.parser(result['Value'])
        # result = self._parseUserProfile(result['Value'])
        if result['OK']:
          _, _, profile = result['Value']
          metadata[token['user_id']] = profile[self.name][token['user_id']]

    return S_OK(metadata)

  def submitNewSession(self, session=None):
    """ Submit new authorization session

        :param str session: session number

        :return: S_OK(str)/S_ERROR()
    """
    url, state = self.create_authorization_url(self.metadata['authorization_endpoint'], state=self.generateState(session))
    return S_OK((url, state, {}))

  @checkResponse
  def parseAuthResponse(self, response, session=None):
    """ Make user info dict:

        :param dict response: response on request to get user profile
        :param object session: session

        :return: S_OK(dict)/S_ERROR()
    """
    response = createOAuth2Request(response)

    self.log.debug('Try to parse authentication response:', pprint.pformat(response.data))

    if not session:
      session = {}  # Session(response.args['state'])

    self.log.debug('Current session is:\n', pprint.pformat(dict(session)))
    # self.log.debug('Current metadata is:\n', pprint.pformat(self.metadata))

    self.fetch_access_token(authorization_response=response.uri,
                            code_verifier=session.get('code_verifier'))

    # Get user info
    result = self.__getUserInfo()
    if not result['OK']:
      return result
    credDict = parseBasic(result['Value'])
    credDict.update(parseEduperson(result['Value']))
    cerdDict = userDiscover(credDict)
    result = self.parser(result['Value'])
    if not result['OK']:
      return result
    username, userID, userProfile = result['Value']
    userProfile['credDict'] = credDict

    self.log.debug('Got response dictionary:\n', pprint.pformat(userProfile))

    # Store token
    self.token['client_id'] = self.client_id
    self.token['provider'] = self.name
    self.token['user_id'] = userID
    self.log.debug('Store token to the database:\n', pprint.pformat(dict(self.token)))

    result = self.store_token(self.token)
    if not result['OK']:
      return result

    return S_OK((username, userID, userProfile))

  def _fillUserProfile(self, useToken=None):
    result = self.__getUserInfo(useToken)
    return self.parser(result['Value']) if result['OK'] else result
    # return self._parseUserProfile(result['Value']) if result['OK'] else result

  def __getUserInfo(self, useToken=None):
    self.log.debug('Sent request to userinfo endpoint..')
    r = None
    try:
      r = self.request('GET', self.metadata['userinfo_endpoint'],
                       withhold_token=useToken)
      r.raise_for_status()
      return S_OK(r.json())
    except (self.exceptions.RequestException, ValueError) as e:
      return S_ERROR("%s: %s" % (repr(e), r.text if r else ''))

  # def _parseUserProfile(self, userProfile):
  #   """ Parse user profile

  #       :param dict userProfile: user profile in OAuht2 format

  #       :return: S_OK()/S_ERROR()
  #   """
  #   # Generate username
  #   gname = userProfile.get('given_name')
  #   fname = userProfile.get('family_name')
  #   pname = userProfile.get('preferred_username')
  #   name = userProfile.get('name') and userProfile['name'].split(' ')
  #   username = pname or gname and fname and gname[0] + fname
  #   username = username or name and len(name) > 1 and name[0][0] + name[1] or ''
  #   username = re.sub('[^A-Za-z0-9]+', '', username.lower())[:13]
  #   self.log.debug('Parse user name:', username)

  #   profile = {}

  #   # Set provider
  #   profile['Provider'] = self.name

  #   # Collect user info
  #   profile['ID'] = userProfile.get('sub')
  #   if not profile['ID']:
  #     return S_ERROR('No ID of user found.')
  #   profile['Email'] = userProfile.get('email')
  #   profile['FullName'] = gname and fname and ' '.join([gname, fname]) or name and ' '.join(name) or ''
  #   self.log.debug('Parse user profile:\n', profile)

  #   # Default DIRAC groups, configured for IdP
  #   profile['Groups'] = self.parameters.get('DiracGroups')
  #   if profile['Groups'] and not isinstance(profile['Groups'], list):
  #     profile['Groups'] = profile['Groups'].replace(' ', '').split(',')
  #   self.log.debug('Default groups:', ', '.join(profile['Groups'] or []))
  #   self.log.debug('Response Information:', pprint.pformat(userProfile))

  #   self.log.debug('Read regex syntax to get DNs describetion dictionary..')
  #   userDNs = {}
  #   dictItemRegex, listItemRegex = {}, None
  #   try:
  #     dnClaim = self.parameters['Syntax']['DNs']['claim']
  #     for k, v in self.parameters['Syntax']['DNs'].items():
  #       if isinstance(v, dict) and v.get('item'):
  #         dictItemRegex[k] = v['item']
  #       elif k == 'item':
  #         listItemRegex = v
  #   except Exception as e:
  #     if not profile['Groups']:
  #       self.log.warn('No DNs described in Syntax/DNs IdP configuration section were found in the response.',
  #                     "And no DiracGroups were found fo IdP.")
  #     return S_OK((username, profile))

  #   self.log.debug('Dict type items pattern:\n', pprint.pformat(dictItemRegex))
  #   self.log.debug('List type items pattern:\n', pprint.pformat(listItemRegex))

  #   if not userProfile.get(dnClaim) and not profile['Groups']:
  #     self.log.warn('No "DiracGroups", no claim "%s" that describe DNs found.' % dnClaim)
  #   else:
  #     self.log.debug('Found "%s" claim that describe user DNs' % dnClaim)
  #     if not isinstance(userProfile[dnClaim], list):
  #       self.log.debug('Convert "%s" claim to list..' % dnClaim)
  #       userProfile[dnClaim] = userProfile[dnClaim].split(',')

  #     for item in userProfile[dnClaim]:
  #       dnInfo = {}
  #       self.log.debug('Read "%s" item:' % dnClaim, item)
  #       if isinstance(item, dict):
  #         for subClaim, reg in dictItemRegex.items():
  #           result = re.compile(reg).match(item[subClaim])
  #           if result:
  #             for k, v in result.groupdict().items():
  #               dnInfo[k] = v
  #       elif listItemRegex:
  #         result = re.compile(listItemRegex).match(item)
  #         if result:
  #           for k, v in result.groupdict().items():
  #             dnInfo[k] = v

  #       self.log.debug('Read parsed DN information:\n', dnInfo)
  #       if dnInfo.get('DN'):
  #         if not dnInfo['DN'].startswith('/'):
  #           self.log.debug('Convert %s to view with slashes.' % dnInfo['DN'])
  #           items = dnInfo['DN'].split(',')
  #           items.reverse()
  #           dnInfo['DN'] = '/' + '/'.join(items)
  #         if dnInfo.get('ProxyProvider'):
  #           self.log.debug('Found %s provider in item,' % dnInfo['ProxyProvider'])
  #           result = getProviderByAlias(dnInfo['ProxyProvider'], instance='Proxy')
  #           dnInfo['ProxyProvider'] = result['Value'] if result['OK'] else 'Certificate'
  #           self.log.debug('In the DIRAC configuration it corresponds to the ', dnInfo['ProxyProvider'])
  #         userDNs[dnInfo['DN']] = dnInfo
  #     if userDNs:
  #       profile['DNs'] = userDNs

  #   self.log.verbose('We were able to compile the following profile for %s:\n' % username, profile)
  #   return S_OK((username, profile))

  def exchange_token(self, url, subject_token=None, subject_token_type=None, body='',
                     refresh_token=None, access_token=None, auth=None, headers=None, **kwargs):
    """ Fetch a new access token using a refresh token.

        :param url: Refresh Token endpoint, must be HTTPS.
        :param str subject_token: subject_token
        :param str subject_token_type: token type https://tools.ietf.org/html/rfc8693#section-3
        :param body: Optional application/x-www-form-urlencoded body to add the
                      include in the token request. Prefer kwargs over body.
        :param str refresh_token: refresh token
        :param str access_token: access token
        :param auth: An auth tuple or method as accepted by requests.
        :param headers: Dict to default request headers with.
        :return: A :class:`OAuth2Token` object (a dict too).
    """
    session_kwargs = self._extract_session_request_params(kwargs)
    refresh_token = refresh_token or self.token.get('refresh_token')
    access_token = access_token or self.token.get('access_token')
    subject_token = subject_token or refresh_token
    subject_token_type = subject_token_type or 'urn:ietf:params:oauth:token-type:refresh_token'
    if 'scope' not in kwargs and self.scope:
      kwargs['scope'] = self.scope
    body = prepare_token_request('urn:ietf:params:oauth:grant-type:token-exchange', body,
                                 subject_token=subject_token, subject_token_type=subject_token_type, **kwargs)

    if headers is None:
      headers = DEFAULT_HEADERS

    for hook in self.compliance_hook.get('exchange_token_request', []):
      url, headers, body = hook(url, headers, body)

    if auth is None:
      auth = self.client_auth(self.token_endpoint_auth_method)

    return self._exchange_token(url, refresh_token=refresh_token, body=body, headers=headers,
                                auth=auth, **session_kwargs)

  def _exchange_token(self, url, body='', refresh_token=None, headers=None, auth=None, **kwargs):
    resp = self.session.post(url, data=dict(url_decode(body)), headers=headers, auth=auth, **kwargs)

    for hook in self.compliance_hook.get('exchange_token_response', []):
      resp = hook(resp)

    token = self.parse_response_token(resp.json())
    if 'refresh_token' not in token:
      self.token['refresh_token'] = refresh_token

    if callable(self.update_token):
      self.update_token(self.token, refresh_token=refresh_token)

    return self.token

  def generateState(self, session=None):
    return session or generate_token(10)