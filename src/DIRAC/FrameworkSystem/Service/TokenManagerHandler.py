""" TokenManager service

    .. literalinclude:: ../ConfigTemplate.cfg
      :start-after: ##BEGIN TokenManager:
      :end-before: ##END
      :dedent: 2
      :caption: TokenManager options
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
import pprint

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Security import Properties
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.FrameworkSystem.DB.TokenDB import TokenDB
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory


class TokenManagerHandler(TornadoService):

  __maxExtraLifeFactor = 1.5
  __tokenDB = None

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    try:
      cls.__tokenDB = TokenDB()
    except Exception as e:
      gLogger.exception(e)
      return S_ERROR('Could not connect to the database %s' % repr(e))

    cls.idps = IdProviderFactory()
    return S_OK()

  def __generateUsersTokensInfo(self, users):
    """ Generate information dict about user tokens

        :return: dict
    """
    tokensInfo = []
    credDict = self.getRemoteCredentials()
    result = Registry.getDNForUsername(credDict['username'])
    if not result['OK']:
      return result
    for dn in result['Value']:
      result = Registry.getIDFromDN(dn)
      if result['OK']:
        result = self.__tokenDB.getTokensByUserID(result['Value'])
        if not result['OK']:
          gLogger.error(result['Message'])
        tokensInfo += result['Value']
    return tokensInfo

  def __generateUserTokensInfo(self):
    """ Generate information dict about user tokens

        :return: dict
    """
    tokensInfo = []
    credDict = self.getRemoteCredentials()
    result = Registry.getDNForUsername(credDict['username'])
    if not result['OK']:
      return result
    for dn in result['Value']:
      result = Registry.getIDFromDN(dn)
      if result['OK']:
        result = self.__tokenDB.getTokensByUserID(result['Value'])
        if not result['OK']:
          gLogger.error(result['Message'])
        tokensInfo += result['Value']
    return tokensInfo

  def __addKnownUserTokensInfo(self, retDict):
    """ Given a S_OK/S_ERR add a tokens entry with info of all the tokens a user has uploaded

        :return: S_OK(dict)/S_ERROR()
    """
    retDict['tokens'] = self.__generateUserTokensInfo()
    return retDict

  auth_getUserTokensInfo = ['authenticated']

  def export_getUserTokensInfo(self):
    """ Get the info about the user tokens in the system

        :return: S_OK(dict)
    """
    return S_OK(self.__generateUserTokensInfo())
  
  auth_getUsersTokensInfo = [Properties.PROXY_MANAGEMENT]

  def export_getUsersTokensInfo(self, users):
    """ Get the info about the user tokens in the system

        :param list users: user names

        :return: S_OK(dict)
    """
    tokensInfo = []
    for user in users:
      result = Registry.getDNForUsername(user)
      if not result['OK']:
        return result
      for dn in result['Value']:
        uid = Registry.getIDFromDN(dn).get('Value')
        if uid:
          result = self.__tokenDB.getTokensByUserID(uid)
          if not result['OK']:
            gLogger.error(result['Message'])
          else:
            for tokenDict in result['Value']:
              if tokenDict not in tokensInfo:
                tokenDict['username'] = user
                tokensInfo.append(tokenDict)
    return S_OK(tokensInfo)

  auth_uploadToken = ['authenticated']

  def export_updateToken(self, token, userID, provider, rt_expired_in=24 * 3600):
    """ Request to delegate tokens to DIRAC

        :param dict token: token
        :param str userID: user ID
        :param str provider: provider name
        :param int rt_expired_in: refresh token expires time

        :return: S_OK(list)/S_ERROR() -- list contain uploaded tokens info as dictionaries
    """
    self.log.verbose('Update %s user token for %s:\n' % (userID, provider), pprint.pformat(token))
    result = self.idps.getIdProvider(provider)
    if not result['OK']:
      return result
    idPObj = result['Value']
    result = self.__tokenDB.updateToken(token, userID, provider, rt_expired_in)
    if not result['OK']:
      return result
    for oldToken in result['Value']:
      if 'refresh_token' in oldToken and oldToken['refresh_token'] != token['refresh_token']:
        self.log.verbose('Revoke old refresh token:\n', pprint.pformat(oldToken))
        idPObj.revokeToken(oldToken['refresh_token'])
    return self.__tokenDB.getTokensByUserID(userID)

  def __checkProperties(self, requestedUserDN, requestedUserGroup):
    """ Check the properties and return if they can only download limited tokens if authorized

        :param str requestedUserDN: user DN
        :param str requestedUserGroup: DIRAC group

        :return: S_OK(bool)/S_ERROR()
    """
    credDict = self.getRemoteCredentials()
    if Properties.FULL_DELEGATION in credDict['properties']:
      return S_OK(False)
    if Properties.LIMITED_DELEGATION in credDict['properties']:
      return S_OK(True)
    if Properties.PRIVATE_LIMITED_DELEGATION in credDict['properties']:
      if credDict['DN'] != requestedUserDN:
        return S_ERROR("You are not allowed to download any token")
      if Properties.PRIVATE_LIMITED_DELEGATION not in Registry.getPropertiesForGroup(requestedUserGroup):
        return S_ERROR("You can't download tokens for that group")
      return S_OK(True)
    # Not authorized!
    return S_ERROR("You can't get tokens!")

  def export_getToken(self, username, userGroup):
    """ Get a access token for a user/group

          * Properties:
              * FullDelegation <- permits full delegation of tokens
              * LimitedDelegation <- permits downloading only limited tokens
              * PrivateLimitedDelegation <- permits downloading only limited tokens for one self
    """
    userID = []
    provider = Registry.getIdPForGroup(userGroup)
    if not provider:
      return S_ERROR('The %s group belongs to the VO that is not tied to any Identity Provider.' % userGroup)

    result = self.idps.getIdProvider(provider)
    if not result['OK']:
      return result
    idpObj = result['Value']

    result = Registry.getDNForUsername(username)
    if not result['OK']:
      return result

    err = []
    for dn in result['Value']:
      result = Registry.getIDFromDN(dn)
      if result['OK']:
        result = self.__tokenDB.getTokenForUserProvider(result['Value'], provider)
        if result['OK'] and result['Value']:
          idpObj.token = result['Value']
          result = self.__checkProperties(dn, userGroup)
          if result['OK']:
            result = idpObj.exchangeGroup(userGroup)
            if result['OK']:
              return result
      err.append(result.get('Message', 'No token found for %s.' % dn))
    return S_ERROR('; '.join(err or ['No user ID found for %s' % username]))

  def export_deleteToken(self, userDN):
    """ Delete a token from the DB

        :param str userDN: user DN

        :return: S_OK()/S_ERROR()
    """
    credDict = self.getRemoteCredentials()
    if Properties.PROXY_MANAGEMENT not in credDict['properties']:
      if userDN != credDict['DN']:
        return S_ERROR("You aren't allowed!")
    result = Registry.getIDFromDN(userDN)
    return self.__tokenDB.removeToken(user_id=result['Value']) if result['OK'] else result
