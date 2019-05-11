""" ProxyProvider implementation for the proxy generation using local a PUSP
    proxy server
"""

import urllib

from DIRAC import S_OK, S_ERROR
from DIRAC.Resources.ProxyProvider.ProxyProvider import ProxyProvider
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

__RCSID__ = "$Id$"

class PUSPProxyProvider(ProxyProvider):

  def __init__(self, parameters=None):

    super(PUSPProxyProvider, self).__init__(parameters)

  def getProxy(self, userDict):
    """ Generate user proxy

    :param dict userDict: user description dictionary with possible fields:
                          FullName, UserName, DN, EMail, DiracGroup

    :return: S_OK/S_ERROR, Value is a proxy string
    """

    result = Registry.getGroupsForDN(userDN)
    if not result['OK']:
      return result

    validGroups = result['Value']
    if diracGroup not in validGroups:
      return S_ERROR('Invalid group %s for user' % diracGroup)

    voName = Registry.getVOForGroup(diracGroup)
    if not voName:
      return S_ERROR('Can not determine VO for group %s' % diracGroup)

    retVal = self.__getVOMSAttribute(diracGroup, requestedVOMSAttr)
    if not retVal['OK']:
      return retVal
    vomsAttribute = retVal['Value']['attribute']
    vomsVO = retVal['Value']['VOMSVO']

    puspServiceURL = Registry.getVOOption(voName, 'PUSPServiceURL')
    if not puspServiceURL:
      return S_ERROR('Can not determine PUSP service URL for VO %s' % voName)

    user = userDN.split(":")[-1]

    puspURL = "%s?voms=%s:%s&proxy-renewal=false&disable-voms-proxy=false" \
              "&rfc-proxy=true&cn-label=user:%s" % (puspServiceURL, vomsVO, vomsAttribute, user)

    try:
      proxy = urllib.urlopen(puspURL).read()
    except Exception as e:
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
    timeLeft = credDict['secondsLeft']

    result = chain.generateProxyToString(lifeTime=timeLeft,
                                         diracGroup=diracGroup)
    if not result['OK']:
      return result
    proxyString = result['Value']
    return S_OK((proxyString, timeLeft))