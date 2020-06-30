""" ProxyProvider implementation for the proxy generation using OIDC flow
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pprint
import datetime

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getProvidersForInstance
from DIRAC.ConfigurationSystem.Client.Utilities import getAuthAPI
from DIRAC.Resources.ProxyProvider.ProxyProvider import ProxyProvider

from DIRAC.FrameworkSystem.Utilities.OAuth2 import OAuth2
from DIRAC.FrameworkSystem.Client.AuthManagerClient import gSessionManager
from DIRAC.FrameworkSystem.Client.AuthManagerData import gAuthManagerData

__RCSID__ = "$Id$"


class OAuth2ProxyProvider(ProxyProvider):

  def __init__(self, parameters=None):
    super(OAuth2ProxyProvider, self).__init__(parameters)

  def setParameters(self, parameters):
    self.parameters = parameters
    self.idProviders = self.parameters['IdProvider'] or []
    if not isinstance(self.parameters['IdProvider'], list):
      self.idProviders = [self.parameters['IdProvider']]
    if not self.idProviders:
      result = getProvidersForInstance('Id', providerType='OAuth2')
      if not result['OK']:
        return result
      self.idProviders = result['Value']
    self.oauth2 = None

  def checkStatus(self, userDN):
    """ Read ready to work status of proxy provider

        :param str userDN: user DN

        :return: S_OK(dict)/S_ERROR() -- dictionary contain fields:
                 - 'Status' with ready to work status[ready, needToAuth]
                 - 'AccessTokens' with list of access token
    """
    result = self.__findReadySessions(userDN)
    if not result['OK']:
      self.log.error(result['Message'])
      return result
    sessions = result['Value']
    if not sessions:
      idP = self.idProviders[0]
      return S_OK({'Status': 'needToAuth', 'Comment': 'Need to auth with %s identity provider' % idP,
                   'Action': ['auth', [idP, 'inThread', '%s/auth/%s' % (getAuthAPI().strip('/'), idP)]]})

    # Proxy uploaded in DB?
    result = self.proxyManager._isProxyExist(userDN, 12 * 3600)
    if not result['OK']:
      self.log.error(result['Message'])
      return result
    if not result['Value']:
      # Proxy not uploaded in DB, lets generate and upload
      result = self.getProxy(userDN, sessions=sessions)
      if not result['OK']:
        self.log.error(result['Message'])
        return result

    return S_OK({'Status': 'ready'})

  def __findReadySessions(self, userDN):
    """ Read ready to work status of proxy provider

        :param str userDN: user DN

        :return: S_OK(dict)/S_ERROR() -- dictionary contain fields:
                 - 'Status' with ready to work status[ready, needToAuth]
                 - 'AccessTokens' with list of access token
    """
    result = Registry.getUsernameForDN(userDN)
    if not result['OK']:
      return result
    userName = result['Value']
    return gSessionManager.getReservedSessions(Registry.getIDsForUsername(userName), self.idProviders, True)

  def getProxy(self, userDN, sessions=None):
    """ Generate user proxy with OIDC flow authentication

        :param str userDN: user DN
        :param list sessions: sessions

        :return: S_OK/S_ERROR, Value is a proxy string
    """
    if not sessions:
      result = self.__findReadySessions(userDN)
      if not result['OK']:
        return result
      sessions = result['Value']
    if not sessions:
      return S_ERROR('No sessions found for proxy request.')

    for session in sessions:
      result = gAuthManagerData.getIdPForSession(session)
      if not result['OK']:
        return result

      self.oauth2 = OAuth2(result['Value'])

      self.log.verbose('For proxy request use session:', session)

      # Get proxy request
      result = self.__getProxyRequest(session)
      if not result['OK']:
        self.log.error(result['Message'])
        result = gSessionManager.refreshSession(session)
        if not result['OK']:
          self.log.error(result['Message'])
          continue

        # Try to get proxy request again
        result = self.__getProxyRequest(session)
        if not result['OK']:
          self.log.error(result['Message'])
          result = gSessionManager.logOutSession(session)
          if not result['OK']:
            self.log.error(result['Message'])
          continue
        if not result['Value']:
          result = S_ERROR('Returned proxy is empty.')
          continue

      self.log.info('Proxy is taken')
      break

    if not result['OK']:
      return result
    proxyStr = result['Value'].encode("utf-8")

    # Get DN
    chain = X509Chain()
    result = chain.loadProxyFromString(proxyStr)
    if not result['OK']:
      return result
    result = chain.getCredentials()
    if not result['OK']:
      return result
    DN = result['Value']['identity']

    # Check
    if DN != userDN:
      return S_ERROR('Received proxy DN "%s" not match with requested DN "%s"' % (DN, userDN))

    # Store proxy in proxy manager
    result = self.proxyManager._storeProxy(DN, chain)
    if not result['OK']:
      return result

    return S_OK(chain)  # {'proxy': proxyStr, 'DN': DN})

  def __getProxyRequest(self, session):
    """ Get user proxy from proxy provider

        :param str session: access token

        :return: S_OK(basestring)/S_ERROR()
    """
    # Get tokens
    result = gSessionManager.getSessionTokens(session)
    if not result['OK']:
      return result
    tokens = result['Value']

    kwargs = {'access_token': tokens['AccessToken']}
    kwargs['access_type'] = 'offline'
    kwargs['proxylifetime'] = self.parameters['MaxProxyLifetime'] or 3600 * 24

    # Get proxy request
    self.log.verbose('Send proxy request to %s' % self.parameters['GetProxyEndpoint'])
    kwargs['client_id'] = self.oauth2.get('client_id')
    kwargs['client_secret'] = self.oauth2.get('client_secret')
    try:
      r = self.oauth2.request('GET', self.parameters['GetProxyEndpoint'], params=kwargs, headers={})
      r.raise_for_status()
      return S_OK(r.text)
    except self.oauth2.exceptions.RequestException as e:
      return S_ERROR("%s: %s" % (e.message, r.text))
