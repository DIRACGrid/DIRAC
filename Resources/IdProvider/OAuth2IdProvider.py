""" IdProvider based on OAuth2 protocol
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import urllib
import pprint

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Resources.IdProvider.IdProvider import IdProvider
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getProviderByAlias

from DIRAC.FrameworkSystem.Utilities.OAuth2 import OAuth2

__RCSID__ = "$Id$"


class OAuth2IdProvider(IdProvider):

  def __init__(self, parameters=None, sessionManager=None):
    super(OAuth2IdProvider, self).__init__(parameters, sessionManager)

  def setParameters(self, parameters):
    """ Set identity providers parameters

        :param dict parameters: parameters to set
    """
    self.parameters = parameters
    self.oauth2 = OAuth2(parameters['ProviderName'])

  def submitNewSession(self, session=None):
    """ Submit new authorization session

        :param str session: session number

        :return: S_OK(str)/S_ERROR()
    """
    provider = self.parameters['ProviderName']
    result = self.isSessionManagerAble()
    if not result['OK']:
      return result

    result = self.sessionManager.createNewSession(provider, session=session)
    if not result['OK']:
      return result
    session = result['Value']
    self.log.verbose(session, 'session was created.')

    result = self.oauth2.createAuthRequestURL(session)
    if result['OK']:
      url = result['Value']
      result = self.sessionManager.updateSession(session, {'Provider': provider,
                                                           'Comment': url})
    if not result['OK']:
      kill = self.sessionManager.killSession(session)
      return result if kill['OK'] else kill

    return S_OK(session)

  def checkStatus(self, session):
    """ Read ready to work status of identity provider

        :param str session: if need to check session

        :return: S_OK(dict)/S_ERROR() -- dictionary contain fields:
                 - 'Status' with ready to work status[ready, needToAuth]
                 - 'AccessToken' with list of access token
    """
    result = self.isSessionManagerAble()
    if not result['OK']:
      return result

    result = self.sessionManager.getSessionLifetime(session)
    if not result['OK']:
      return result
    if result['Value'] < 10 * 60:
      self.log.debug('%s session tokens are expired, try to refresh' % session)
      result = self.sessionManager.getSessionTokens(session)
      if not result['OK']:
        return result
      tokens = result['Value']
      result = self.__fetchTokens(tokens)
      if result['OK']:
        tokens = result['Value']
        if not tokens.get('RefreshToken'):
          return S_ERROR('No refresh token found in response.')
        return self.sessionManager.updateSession(session, tokens)
      kill = self.sessionManager.killSession(session)
      return result if kill['OK'] else kill

    return S_OK()

  def parseAuthResponse(self, response, session):
    """ Make user info dict:
          - username(preferd user name)
          - nosupport(VOs in response that not in DIRAC)
          - UsrOptns(User profile that convert to DIRAC user options)
          - Tokens(Contain refreshed tokens, type and etc.)
          - Groups(New DIRAC groups that need created)

        :param dict response: response on request to get user profile
        :param str session: session

        :return: S_OK(dict)/S_ERROR()
    """
    resDict = {}
    result = self.oauth2.fetchToken(response['code'])
    if not result['OK']:
      return result
    tokens = self.__parseTokens(result['Value'])
    result = self.oauth2.getUserProfile(tokens['AccessToken'])
    if not result['OK']:
      return result
    result = self.__parseUserProfile(result['Value'])
    if not result['OK']:
      return result
    userProfile = result['Value']

    for key, value in userProfile.items():
      resDict[key] = value

    upDict = tokens
    upDict['ID'] = resDict['UsrOptns']['ID']
    result = self.sessionManager.updateSession(session, upDict)
    if not result['OK']:
      return result
    self.log.debug('Got response dictionary:\n', pprint.pformat(resDict))
    return S_OK(resDict)

  def fetch(self, session):
    """ Fetch tokens and update session in DB

        :param str,dict session: session number or dictionary with tokens

        :return: S_OK()/S_ERROR()
    """
    tokens = session
    if isinstance(session, str):
      result = self.sessionManager.getSessionTokens(session)
      if not result['OK']:
        return result
      tokens = result['Value']
    result = self.__fetchTokens(tokens)
    if not result['OK']:
      kill = self.sessionManager.killSession(session)
      return result if kill['OK'] else kill
    tokens = result['Value']
    if not tokens.get('RefreshToken'):
      return S_ERROR('No refresh token found in response.')
    return self.sessionManager.updateSession(session, tokens)

  def __fetchTokens(self, tokens):
    """ Fetch tokens

        :param dict tokens: tokens

        :return: S_OK(dict)/S_ERROR() -- dictionary contain tokens
    """
    result = self.oauth2.fetchToken(refreshToken=tokens['RefreshToken'])
    if not result['OK']:
      return result
    return S_OK(self.__parseTokens(result['Value']))

  def __parseTokens(self, tokens):
    """ Parse session tokens

        :param dict tokens: tokens

        :return: dict
    """
    resDict = {}
    resDict['ExpiresIn'] = tokens.get('expires_in') or 0
    resDict['TokenType'] = tokens.get('token_type') or 'bearer'
    resDict['AccessToken'] = tokens.get('access_token')
    resDict['RefreshToken'] = tokens.get('refresh_token')
    return resDict

  def __parseUserProfile(self, userProfile):
    """ Parse user profile

        :param dict userProfile: user profile in OAuht2 format

        :return: S_OK()/S_ERROR()
    """
    resDict = {}
    gname = userProfile.get('given_name')
    fname = userProfile.get('family_name')
    pname = userProfile.get('preferred_username')
    name = userProfile.get('name') and userProfile['name'].split(' ')
    resDict['username'] = pname or gname and fname and gname[0] + fname
    resDict['username'] = resDict['username'] or name and len(name) > 1 and name[0][0] + name[1] or ''
    resDict['username'] = re.sub('[^A-Za-z0-9]+', '', resDict['username'].lower())[:13]
    self.log.debug('Parse user name:', resDict['username'])

    # Collect user info
    resDict['UsrOptns'] = {}
    resDict['UsrOptns']['DNs'] = {}
    resDict['UsrOptns']['ID'] = userProfile.get('sub')
    if not resDict['UsrOptns']['ID']:
      return S_ERROR('No ID of user found.')
    resDict['UsrOptns']['Email'] = userProfile.get('email')
    resDict['UsrOptns']['FullName'] = gname and fname and ' '.join([gname, fname]) or name and ' '.join(name) or ''
    self.log.debug('Parse user profile:\n', resDict['UsrOptns'])

    # Default DIRAC groups
    resDict['UsrOptns']['Groups'] = self.parameters.get('DiracGroups') or []
    if not isinstance(resDict['UsrOptns']['Groups'], list):
      resDict['UsrOptns']['Groups'] = resDict['UsrOptns']['Groups'].replace(' ', '').split(',')
    self.log.debug('Default for groups:', ', '.join(resDict['UsrOptns']['Groups']))
    self.log.debug('Response Information:', pprint.pformat(userProfile))

    # Read regex syntax to get DNs describe dictionary
    dictItemRegex, listItemRegex = {}, None
    try:
      dnClaim = self.parameters['Syntax']['DNs']['claim']
      for k, v in self.parameters['Syntax']['DNs'].items():
        if isinstance(v, dict) and v.get('item'):
          dictItemRegex[k] = v['item']
        elif k == 'item':
          listItemRegex = v
    except Exception as e:
      if not resDict['UsrOptns']['Groups']:
        self.log.warn('No "DiracGroups", no claim with DNs decsribe in Syntax/DNs section found.')
      return S_OK(resDict)

    if not userProfile.get(dnClaim) and not resDict['UsrOptns']['Groups']:
      self.log.warn('No "DiracGroups", no claim "%s" that decsribe DNs found.' % dnClaim)
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
          resDict['UsrOptns']['DNs'][dnInfo['DN']] = dnInfo

    return S_OK(resDict)

  def getUserProfile(self, session):
    """ Get user information from identity provider

        :param str,dict session: session number or dictionary

        :return: S_OK(dict)/S_ERROR() -- dictionary contain user profile information
    """
    tokens = session
    if isinstance(session, str):
      result = self.sessionManager.getSessionTokens(session)
      if not result['OK']:
        return result
      tokens = result['Value']
    result = self.oauth2.getUserProfile(tokens['AccessToken'])
    if not result['OK']:
      result = self.__fetchTokens(tokens)
      if result['OK']:
        tokens = result['Value']
        result = self.oauth2.getUserProfile(result['Value']['AccessToken'])
    if not result['OK']:
      kill = self.sessionManager.killSession(session)
      return result if kill['OK'] else kill
    userProfile = result['Value']
    result = self.sessionManager.updateSession(session, tokens)
    if not result['OK']:
      return result
    return self.__parseUserProfile(userProfile)

  def logOut(self, session):
    """ Revoke tokens

        :param str session: session number

        :return: S_OK()/S_ERROR()
    """
    result = self.sessionManager.getSessionTokens(session)
    if not result['OK']:
      return result
    tokens = result['Value']
    return self.oauth2.revokeToken(tokens['AccessToken'], tokens['RefreshToken'])
