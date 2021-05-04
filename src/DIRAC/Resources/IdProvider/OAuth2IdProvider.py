""" IdProvider based on OAuth2 protocol
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import pprint
from requests import exceptions
from authlib.integrations.requests_client import OAuth2Session
from authlib.oidc.discovery.well_known import get_well_known_url
from authlib.oauth2.rfc8414 import AuthorizationServerMetadata

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Resources.IdProvider.IdProvider import IdProvider
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getProviderByAlias
from DIRAC.FrameworkSystem.private.authorization.utils.Sessions import Session
from DIRAC.FrameworkSystem.private.authorization.utils.Requests import createOAuth2Request
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import OAuth2Token

__RCSID__ = "$Id$"


class OAuth2IdProvider(IdProvider, OAuth2Session):
  def __init__(self, name=None, token_endpoint_auth_method=None,
               revocation_endpoint_auth_method=None,
               scope=None, token=None, token_placement='header',
               update_token=None, **parameters):
    """ OIDCClient constructor
    """
    IdProvider.__init__(self, **parameters)
    OAuth2Session.__init__(self, token_endpoint_auth_method=token_endpoint_auth_method,
                           revocation_endpoint_auth_method=revocation_endpoint_auth_method,
                           scope=scope, token=token, token_placement=token_placement,
                           update_token=update_token, **parameters)
    # Convert scope to list
    scope = scope or ''
    self.scope = [s.strip() for s in scope.strip().replace('+', ' ').split(',' if ',' in scope else ' ')]
    self.parameters = parameters
    self.exceptions = exceptions
    self.name = name or parameters.get('ProviderName')

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

    print('========= %s ===========' % self.name)
    print(self.client_id)
    print(self.client_secret)
    print(self.token)
    pprint.pprint(self.metadata)

  def _storeToken(self, token, session=None):
    return self.sessionManager.storeToken(dict(self.token))

  def _updateToken(self, token, refresh_token):
    # Here "token" is `OAuth2Token` type
    self.sessionManager.updateToken(dict(token), refresh_token)

  def checkResponse(func):
    def function_wrapper(*args, **kwargs):
      try:
        return func(*args, **kwargs)
      except exceptions.Timeout:
        return S_ERROR('Time out')
      except exceptions.RequestException as ex:
        return S_ERROR(str(ex))
    return function_wrapper

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
        result = self._parseUserProfile(result['Value'])
        if result['OK']:
          _, profile = result['Value']
          metadata[token['user_id']] = profile

    return S_OK(metadata)

  def submitNewSession(self, session):
    """ Submit new authorization session

        :param str session: session number

        :return: S_OK(str)/S_ERROR()
    """
    url, _ = self.create_authorization_url(self.metadata['authorization_endpoint'], state=session)
    return S_OK((url, {}))

  @checkResponse
  def parseAuthResponse(self, response, session=None):
    """ Make user info dict:
          - username(preferred user name)
          - nosupport(VOs in response that not in DIRAC)
          - UsrOptns(User profile that convert to DIRAC user options)
          - Tokens(Contain refreshed tokens, type and etc.)
          - Groups(New DIRAC groups that need created)

        :param dict response: response on request to get user profile
        :param object session: session

        :return: S_OK(dict)/S_ERROR()
    """
    print('====>> IDP parseAuthResponse')
    pprint.pprint(response)
    
    response = createOAuth2Request(response)
    if not session:
      session = Session(response.args['state'])
    print('Session:')
    pprint.pprint(dict(session))
    print('--> METADATA:')
    pprint.pprint(self.metadata)
    self.fetch_access_token(authorization_response=response.uri,
                            code_verifier=session.get('code_verifier'))
    print('---->> IDP __getUserInfo')
    # Get user info
    result = self._fillUserProfile()
    if not result['OK']:
      return result
    username, userProfile = result['Value']
    
    # Store token
    print('---->> IDP Store token')
    pprint.pprint(self.token)
    self.token['client_id'] = self.client_id
    self.token['provider'] = self.name
    self.token['user_id'] = userProfile['ID']
    print(dict(self.token))

    if self.store_token:
      result = self.store_token(self.token, session.id)
      if not result['OK']:
        return result

    self.log.debug('Got response dictionary:\n', pprint.pformat(userProfile))
    return S_OK((username, userProfile, session.update(token=dict(self.token))))
  
  def _fillUserProfile(self, useToken=None):
    result = self.__getUserInfo(useToken)
    return self._parseUserProfile(result['Value']) if result['OK'] else result

  def __getUserInfo(self, useToken=None):
    r = None
    try:
      r = self.request('GET', self.metadata['userinfo_endpoint'],
                       withhold_token=useToken)
      r.raise_for_status()
      return S_OK(r.json())
    except (self.exceptions.RequestException, ValueError) as e:
      return S_ERROR("%s: %s" % (e.message, r.text if r else ''))

  def _parseUserProfile(self, userProfile):
    """ Parse user profile

        :param dict userProfile: user profile in OAuht2 format

        :return: S_OK()/S_ERROR()
    """
    # Generate username
    gname = userProfile.get('given_name')
    fname = userProfile.get('family_name')
    pname = userProfile.get('preferred_username')
    name = userProfile.get('name') and userProfile['name'].split(' ')
    username = pname or gname and fname and gname[0] + fname
    username = username or name and len(name) > 1 and name[0][0] + name[1] or ''
    username = re.sub('[^A-Za-z0-9]+', '', username.lower())[:13]
    self.log.debug('Parse user name:', username)

    profile = {}

    # Set provider
    profile['Provider'] = self.name

    # Collect user info    
    profile['ID'] = userProfile.get('sub')
    if not profile['ID']:
      return S_ERROR('No ID of user found.')
    profile['Email'] = userProfile.get('email')
    profile['FullName'] = gname and fname and ' '.join([gname, fname]) or name and ' '.join(name) or ''
    self.log.debug('Parse user profile:\n', profile)

    # Default DIRAC groups
    profile['Groups'] = self.parameters.get('DiracGroups')
    if profile['Groups'] and not isinstance(profile['Groups'], list):
      profile['Groups'] = profile['Groups'].replace(' ', '').split(',')
    self.log.debug('Default for groups:', ', '.join(profile['Groups'] or []))
    self.log.debug('Response Information:', pprint.pformat(userProfile))

    # Read regex syntax to get DNs describe dictionary
    userDNs = {}
    dictItemRegex, listItemRegex = {}, None
    try:
      dnClaim = self.parameters['Syntax']['DNs']['claim']
      for k, v in self.parameters['Syntax']['DNs'].items():
        if isinstance(v, dict) and v.get('item'):
          dictItemRegex[k] = v['item']
        elif k == 'item':
          listItemRegex = v
    except Exception as e:
      if not profile['Groups']:
        self.log.warn('No "DiracGroups", no claim with DNs describe in Syntax/DNs section found.')
      return S_OK((username, profile))

    if not userProfile.get(dnClaim) and not profile['Groups']:
      self.log.warn('No "DiracGroups", no claim "%s" that describe DNs found.' % dnClaim)
    else:

      if not isinstance(userProfile[dnClaim], list):
        userProfile[dnClaim] = userProfile[dnClaim].split(',')

      for item in userProfile[dnClaim]:
        dnInfo = {}
        if isinstance(item, dict):
          for subClaim, reg in dictItemRegex.items():
            result = re.compile(reg).match(item[subClaim])
            if result:
              for k, v in result.groupdict().items():
                dnInfo[k] = v
        elif listItemRegex:
          result = re.compile(listItemRegex).match(item)
          if result:
            for k, v in result.groupdict().items():
              dnInfo[k] = v

        if dnInfo.get('DN'):
          if not dnInfo['DN'].startswith('/'):
            items = dnInfo['DN'].split(',')
            items.reverse()
            dnInfo['DN'] = '/' + '/'.join(items)
          if dnInfo.get('PROVIDER'):
            result = getProviderByAlias(dnInfo['PROVIDER'], instance='Proxy')
            dnInfo['PROVIDER'] = result['Value'] if result['OK'] else 'Certificate'
          userDNs[dnInfo['DN']] = dnInfo
      if userDNs:
        profile['DNs'] = userDNs

    return S_OK((username, profile))
