""" ProxyProvider implementation for the proxy generation using local a PUSP
    proxy server
"""

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

        :return: S_OK(dict)/S_ERROR() -- dictionary contain fields:
                  - 'Status' with ready to work status[ready, needToAuth]
    """
    if not userDN.split(":")[-1]:
      return S_ERROR('Can not found user label for DN: %s' % userDN)
    if not puspServiceURL:
      return S_ERROR('Can not determine PUSP service URL')

    return S_OK({'Status': 'ready'})

  def getProxy(self, userDN):
    """ Generate user proxy

        :param str userDN: user DN

        :return: S_OK(dict)/S_ERROR() -- dict contain 'proxy' field with is a proxy string
    """
    result = self.checkStatus(userDN)
    if not result['OK']:
      return result

    puspURL = self.parameters.get('ServiceURL')
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

    result = chain.generateProxyToString(lifeTime=credDict['secondsLeft'])

    return S_OK({'proxy': result['Value']}) if result['OK'] else result
