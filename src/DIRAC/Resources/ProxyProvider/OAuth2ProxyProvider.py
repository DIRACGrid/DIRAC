""" ProxyProvider implementation for the proxy generation using OIDC authorization flow
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pprint
import datetime
import requests
from requests import exceptions

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getProvidersForInstance, getProviderInfo
from DIRAC.ConfigurationSystem.Client.Utilities import getAuthAPI
from DIRAC.Resources.ProxyProvider.ProxyProvider import ProxyProvider
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory

# from DIRAC.FrameworkSystem.Utilities.OAuth2 import OAuth2
from DIRAC.FrameworkSystem.Client.AuthManagerClient import gSessionManager
from DIRAC.FrameworkSystem.Client.AuthManagerData import gAuthManagerData

__RCSID__ = "$Id$"


class OAuth2ProxyProvider(ProxyProvider):

  def __init__(self, parameters=None):
    # TODO: need do self.idpObj -- idP in contex(access tokens) we do request
    super(OAuth2ProxyProvider, self).__init__(parameters)
    self.__idps = IdProviderFactory()

  def setParameters(self, parameters):
    self.parameters = parameters
    self.proxy_endpoint = self.parameters['GetProxyEndpoint']
    self.idProviders = self.parameters['IdProvider'] or []  # TODO: Supported ID Providers
    if not isinstance(self.parameters['IdProvider'], list):
      self.idProviders = [self.parameters['IdProvider']]
    if not self.idProviders:
      result = getProvidersForInstance('Id', providerType='OAuth2')  # TODO: Its not need
      if not result['OK']:
        return result
      self.idProviders = result['Value']

  def checkStatus(self, userDN):
    """ Read ready to work status of proxy provider

        :param str userDN: user DN

        :return: S_OK(dict)/S_ERROR() -- dictionary contain fields:
                 - 'Status' with ready to work status[ready, needToAuth]
                 - 'AccessTokens' with list of access token
    """
    result = gAuthManagerData.getIDsForDN(userDN, provider=self.parameters['ProviderName'])
    if not result['OK']:
      self.log.error(result['Message'])
      return result
    uid = result['Value'][0]
    # TODO: authManagerService must get token throgh iDP with requested lifetime
    result = gSessionManager.getTokenByUserIDAndProvider(uid, self.idProviders[0])
    if not result['OK']:
      self.log.error(result['Message'])
      return result
    token = result['Value']
    if not token:
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
      result = self.getProxy(userDN, token=token)
      if not result['OK']:
        self.log.error(result['Message'])
        return result

    return S_OK({'Status': 'ready'})

  def getProxy(self, userDN, token=None):
    """ Generate user proxy with OIDC flow authentication

        :param str userDN: user DN
        :param list sessions: sessions

        :return: S_OK/S_ERROR, Value is a proxy string
    """
    if not token:
      result = gAuthManagerData.getIDsForDN(userDN, provider=self.parameters['ProviderName'])
      if not result['OK']:
        self.log.error(result['Message'])
        return result
      uid = result['Value'][0]
      # TODO: authManagerService must get token throgh iDP with requested lifetime
      result = gSessionManager.getTokenByUserIDAndProvider(uid, self.idProviders[0])
      if not result['OK']:
        self.log.error(result['Message'])
        return result
      token = result['Value']
    if not token:
      return S_ERROR('Token not found for proxy request.')

    self.log.verbose('For proxy request use token:\n', pprint.pformat(token))

    # Get proxy request
    result = self.__getProxyRequest(token)
    # if not result['OK']:
    #   # expire token to refresh
    #   token['expires_at'] = 1
    #   result = self.__getProxyRequest(token)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('Returned proxy is empty.')

    self.log.info('Proxy is taken')
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

    return S_OK(chain) if result['OK'] else result  # {'proxy': proxyStr, 'DN': DN})

  def __getProxyRequest(self, token):
    """ Get user proxy from proxy provider

        :param str session: access token

        :return: S_OK(basestring)/S_ERROR()
    """
    result = self.__idps.getIdProvider(self.idProviders[0])
    if not result['OK']:
      return result
    provObj = result['Value']

    provObj.token = token
    url = '%s?access_type=offline' % self.proxy_endpoint
    url += '&proxylifetime=%s' % self.parameters.get('MaxProxyLifetime', 3600 * 24)
    url += '&client_id=%s&client_secret=%s' % (provObj.client_id, provObj.client_secret)

    # Get proxy request
    self.log.verbose('Send proxy request to %s, with token:\n' % self.proxy_endpoint, pprint.pformat(provObj.token))
    self.log.debug("GET ", url)

    r = None
    try:
      r = provObj.request('GET', url, withhold_token=True)
      r.raise_for_status()
      return S_OK(r.text)
    except provObj.exceptions.RequestException as e:
      return S_ERROR("%s: %s" % (repr(e), r.text if r else ''))
