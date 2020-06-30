""" ProxyProvider implementation for a per-user sub-proxy(PUSP) proxy generation using
    PUSP proxy server.
    More details about PUSP here: https://wiki.egi.eu/wiki/Usage_of_the_per_user_sub_proxy_in_EGI
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from six.moves.urllib.request import urlopen

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Resources.ProxyProvider.ProxyProvider import ProxyProvider

__RCSID__ = "$Id$"


class PUSPProxyProvider(ProxyProvider):

  def __init__(self, parameters=None):

    super(PUSPProxyProvider, self).__init__(parameters)

  def checkStatus(self, userDN):
    """ Read ready to work status of proxy provider

        :param str userDN: user DN

        :return: S_OK()/S_ERROR()
    """
    # Search a unique identifier(username) to use as cn-label to generate PUSP
    if not userDN.split(":")[-1]:
      return S_ERROR('Can not found user label for DN: %s' % userDN)
    if not self.parameters.get('ServiceURL'):
      return S_ERROR('Can not determine PUSP service URL')

    return S_OK()

  def getProxy(self, userDN):
    """ Generate user proxy

        :param str userDN: user DN

        :return: S_OK(str)/S_ERROR() -- contain a proxy string
    """
    result = self.checkStatus(userDN)
    if not result['OK']:
      return result

    puspURL = self.parameters['ServiceURL']
    puspURL += "?proxy-renewal=false&disable-voms-proxy=true&rfc-proxy=true"
    puspURL += "&cn-label=user:%s" % userDN.split(":")[-1]

    try:
      proxy = urlopen(puspURL).read()
    except Exception:
      return S_ERROR('Failed to get proxy from the PUSP server')

    chain = X509Chain()
    chain.loadChainFromString(proxy)
    chain.loadKeyFromString(proxy)

    result = chain.getCredentials()
    if not result['OK']:
      return S_ERROR('Failed to get a valid PUSP proxy')
    credDict = result['Value']
    if credDict['identity'] != userDN:
      return S_ERROR('Requested DN does not match the obtained one in the PUSP proxy')

    # Store proxy in proxy manager
    result = self.proxyManager._storeProxy(userDN, chain)

    return S_OK(chain) if result['OK'] else result
