""" ProxyManagemerClient has the function to "talk" to the ProxyManagemer service
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import six
import datetime
import time
time.sleep(5)
print('====>> -1')
from DIRAC import S_OK, S_ERROR, gLogger
time.sleep(5)
print('====>> -2')
from DIRAC.FrameworkSystem.Client.ProxyManagerData import gProxyManagerData
time.sleep(5)
print('====>> -3')
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOMSAttributeForGroup,\
    getUsernameForDN, getDNsForUsernameInGroup
time.sleep(5)
print('====>> -4')
from DIRAC.Core.Utilities import ThreadSafe, DIRACSingleton
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Security.ProxyFile import multiProxyArgument, deleteMultiProxy
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Core.Security.X509Request import X509Request  # pylint: disable=import-error
from DIRAC.Core.Security.VOMS import VOMS
from DIRAC.Core.Security import Locations
from DIRAC.Core.DISET.RPCClient import RPCClient

__RCSID__ = "$Id$"

gProxiesSync = ThreadSafe.Synchronizer()


class ProxyManagerClient(object):
  """ Proxy manager client
  """
  __metaclass__ = DIRACSingleton.DIRACSingleton

  def __init__(self, **kwargs):
    self.__extArgs = kwargs
    self.__proxiesCache = DictCache()
    self.__filesCache = DictCache(self.__deleteTemporalFile)
    self.rpcClient = RPCClient("Framework/ProxyManager", timeout=120, **self.__extArgs)

  def __deleteTemporalFile(self, filename):
    """ Delete temporal file

        :param str filename: path to file
    """
    try:
      os.remove(filename)
    except Exception:
      pass

  def clearCaches(self):
    """ Clear caches
    """
    self.__proxiesCache.purgeAll()

  def userHasProxy(self, user, group, validSeconds=0):
    """ Check if a user-group has a proxy in the proxy management
        Updates internal cache if needed to minimize queries to the service

        :param str user: user name
        :param str group: user group
        :param int validSeconds: proxy valid time in a seconds

        :return: S_OK()/S_ERROR()
    """
    dn = user
    if not user.startswith('/'):
      result = getDNForUsernameInGroup(user, userGroup)
      if not result['OK']:
        return result
      dn = result['Value']

    result = gProxyManagerData.userHasProxy(dn, group, validSeconds)
    if not result['OK'] or result['Value'] or user.startswith('/'):
      return result

    result = self.getGroupsStatusByUsername(user, [group])
    return S_OK(True if result['Value'][group]['Status'] == "ready" else False) if result['OK'] else result

  def uploadProxy(self, proxy=None, restrictLifeTime=0, rfcIfPossible=False):
    """ Upload a proxy to the proxy management service using delegation

        :param X509Chain proxy: proxy as a chain
        :param int restrictLifeTime: proxy live time in a seconds
        :param bool rfcIfPossible: make rfc proxy if possible

        :return: S_OK(dict)/S_ERROR() -- dict contain proxies
    """
    # Discover proxy location
    proxyLocation = proxy if isinstance(proxy, six.string_types) else ""
    if isinstance(proxy, X509Chain):
      chain = proxy
    else:
      if not proxyLocation:
        proxyLocation = Locations.getProxyLocation()
        if not proxyLocation:
          return S_ERROR("Can't find a valid proxy")
      chain = X509Chain()
      result = chain.loadProxyFromFile(proxyLocation)
      if not result['OK']:
        return S_ERROR("Can't load %s: %s " % (proxyLocation, result['Message']))

    # Make sure it's valid
    if chain.hasExpired().get('Value'):
      return S_ERROR("Proxy %s has expired" % proxyLocation)
    if chain.getDIRACGroup().get('Value') or chain.isVOMS().get('Value'):
      return S_ERROR("Cannot upload proxy with DIRAC group or VOMS extensions")

    # Get a delegation request
    result = self.rpcClient.requestDelegationUpload()
    if not result['OK']:
      return result
    reqDict = result['Value']
    # Generate delegated chain
    chainLifeTime = chain.getRemainingSecs()['Value'] - 60
    chainLifeTime = min(restrictLifeTime, chainLifeTime) if restrictLifeTime else chainLifeTime
    retVal = chain.generateChainFromRequestString(reqDict['request'], lifetime=chainLifeTime,
                                                  rfc=rfcIfPossible)
    if not retVal['OK']:
      return retVal
    # Upload!
    return self.rpcClient.completeDelegationUpload(reqDict['id'], retVal['Value'])

  @gProxiesSync
  def __getProxy(self, user, userGroup, limited=False, requiredTimeLeft=1200, cacheTime=14400,
                 proxyToConnect=None, token=None, voms=None, personal=False):
    """ Get a proxy Chain from the proxy manager

        :param str user: user name or DN
        :param str group: user group
        :param bool limited: if need limited proxy
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param int cacheTime: store in a cache time in a seconds
        :param X509Chain proxyToConnect: proxy as a chain
        :param str token: valid token to get a proxy
        :param bool voms: for VOMS proxy
        :param bool personal: get personal proxy

        :return: S_OK(X509Chain)/S_ERROR()
    """
    if voms and not getVOMSAttributeForGroup(userGroup):
      return S_ERROR("No mapping defined for group %s in the CS" % userGroup)

    dn = None
    if user.startswith('/'):
      dn = user
      result = getUsernameForDN(dn)
      if result['OK']:
        user = result['Value']
        result = getDNsForUsernameInGroup(user, userGroup)
      if not result['OK']:
        return result
      if dn not in result['Value']:
        return S_ERROR('"%s" DN not match with %s user, %s group.' % (dn, user, userGroup))

    cacheKey = (dn or user, userGroup, voms, limited)
    if self.__proxiesCache.exists(cacheKey, requiredTimeLeft):
      return S_OK(self.__proxiesCache.get(cacheKey))

    req = X509Request()
    req.generateProxyRequest(limited=limited)
    if proxyToConnect:
      rpcClient = RPCClient("Framework/ProxyManager", proxyChain=proxyToConnect, timeout=120,
                            **self.__extArgs)
    else:
      rpcClient = self.rpcClient

    retVal = rpcClient.getProxy(dn or user, userGroup, req.dumpRequest()['Value'],
                                int(cacheTime + requiredTimeLeft), token, voms, personal)
    if not retVal['OK']:
      return retVal

    chain = X509Chain(keyObj=req.getPKey())
    retVal = chain.loadChainFromString(retVal['Value'])
    if not retVal['OK']:
      return retVal
    self.__proxiesCache.add(cacheKey, chain.getRemainingSecs()['Value'], chain)
    return S_OK(chain)

  def downloadPersonalProxy(self, user, group, requiredTimeLeft=1200, voms=False):
    """ Get a proxy Chain from the proxy management

        :param str user: user name or DN
        :param str group: user group
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param bool voms: for VOMS proxy

        :return: S_OK(X509Chain)/S_ERROR()
    """
    return self.__getProxy(user, group, requiredTimeLeft=requiredTimeLeft,
                           voms=voms, personal=True)

  def downloadProxy(self, user, group, limited=False, requiredTimeLeft=1200, cacheTime=14400,
                    proxyToConnect=None, token=None, personal=False):
    """ Get a proxy Chain from the proxy management

        :param str user: user name or DN
        :param str group: user group
        :param bool limited: if need limited proxy
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param int cacheTime: store in a cache time in a seconds
        :param X509Chain proxyToConnect: proxy as a chain
        :param str token: valid token to get a proxy
        :param bool personal: get personal proxy

        :return: S_OK(X509Chain)/S_ERROR()
    """
    return self.__getProxy(user, group, limited=limited, requiredTimeLeft=requiredTimeLeft,
                           cacheTime=cacheTime, proxyToConnect=proxyToConnect, token=token, personal=personal)

  def downloadProxyToFile(self, user, group, limited=False, requiredTimeLeft=1200, cacheTime=14400,
                          filePath=None, proxyToConnect=None, token=None, personal=False):
    """ Get a proxy Chain from the proxy management and write it to file

        :param str user: user name or DN
        :param str group: user group
        :param bool limited: if need limited proxy
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param int cacheTime: store in a cache time in a seconds
        :param str filePath: path to save proxy
        :param X509Chain proxyToConnect: proxy as a chain
        :param str token: valid token to get a proxy
        :param bool personal: get personal proxy

        :return: S_OK(X509Chain)/S_ERROR()
    """
    retVal = self.downloadProxy(user, group, limited, requiredTimeLeft, cacheTime, proxyToConnect, token, personal)
    if retVal['OK']:
      chain = retVal['Value']
      retVal = self.dumpProxyToFile(chain, filePath)
      if retVal['OK']:
        retVal['chain'] = chain
    return retVal

  def downloadVOMSProxy(self, user, group, limited=False, requiredTimeLeft=1200,
                        cacheTime=14400, proxyToConnect=None, token=None, personal=False):
    """ Download a proxy if needed and transform it into a VOMS one

        :param str user: user name or DN
        :param str group: user group
        :param bool limited: if need limited proxy
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param int cacheTime: store in a cache time in a seconds
        :param X509Chain proxyToConnect: proxy as a chain
        :param str token: valid token to get a proxy
        :param bool personal: get personal proxy

        :return: S_OK(X509Chain)/S_ERROR()
    """
    return self.__getProxy(user, group, limited=limited, requiredTimeLeft=requiredTimeLeft, voms=True,
                           cacheTime=cacheTime, proxyToConnect=proxyToConnect, token=token, personal=personal)

  def downloadVOMSProxyToFile(self, user, group, limited=False, requiredTimeLeft=1200,
                              cacheTime=14400, filePath=None, proxyToConnect=None, token=None, personal=False):
    """ Download a proxy if needed, transform it into a VOMS one and write it to file

        :param str user: user name or DN
        :param str group: user group
        :param bool limited: if need limited proxy
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param int cacheTime: store in a cache time in a seconds
        :param str filePath: path to save proxy
        :param X509Chain proxyToConnect: proxy as a chain
        :param str token: valid token to get a proxy
        :param bool personal: get personal proxy

        :return: S_OK(X509Chain)/S_ERROR()
    """
    retVal = self.downloadVOMSProxy(user, group, limited, requiredTimeLeft, cacheTime,
                                    proxyToConnect, token, personal)
    if retVal['OK']:
      chain = retVal['Value']
      retVal = self.dumpProxyToFile(chain, filePath)
      if retVal['OK']:
        retVal['chain'] = chain
    return retVal

  def downloadCorrectProxy(self, user, group, requiredTimeLeft=43200, proxyToConnect=None,
                           token=None, personal=False):
    """ Download a proxy with VOMS extensions depending on the group or simple proxy
        if group without VOMS extensions

        :param str user: user name or DN
        :param str group: user group
        :param int requiredTimeLeft: required proxy live time in a seconds
        :param X509Chain proxyToConnect: proxy as a chain
        :param str token: valid token to get a proxy
        :param bool personal: get personal proxy

        :return: S_OK(X509Chain)/S_ERROR()
    """
    if not getVOMSAttributeForGroup(group):
      gLogger.verbose("No voms attribute assigned to group %s when requested proxy" % group)
      return self.downloadProxy(user, group, limited=False, requiredTimeLeft=requiredTimeLeft,
                                proxyToConnect=proxyToConnect, personal=personal)
    return self.downloadVOMSProxy(user, group, limited=False, requiredTimeLeft=requiredTimeLeft,
                                  proxyToConnect=proxyToConnect, personal=personal)

  def dumpProxyToFile(self, chain, destinationFile=None, requiredTimeLeft=600):
    """ Dump a proxy to a file. It's cached so multiple calls won't generate extra files

        :param X509Chain chain: proxy as a chain
        :param str destinationFile: path to store proxy
        :param int requiredTimeLeft: required proxy live time in a seconds

        :return: S_OK(str)/S_ERROR()
    """
    result = chain.hash()
    if not result['OK']:
      return result
    cHash = result['Value']
    if self.__filesCache.exists(cHash, requiredTimeLeft):
      filepath = self.__filesCache.get(cHash)
      if filepath and os.path.isfile(filepath):
        return S_OK(filepath)
      self.__filesCache.delete(cHash)
    retVal = chain.dumpAllToFile(destinationFile)
    if not retVal['OK']:
      return retVal
    filename = retVal['Value']
    self.__filesCache.add(cHash, chain.getRemainingSecs()['Value'], filename)
    return S_OK(filename)

  def deleteGeneratedProxyFile(self, chain):
    """ Delete a file generated by a dump

        :param X509Chain chain: proxy as a chain

        :return: S_OK()
    """
    self.__filesCache.delete(chain)
    return S_OK()

  def requestToken(self, requester, requesterGroup, numUses=1):
    """ Request a number of tokens. usesList must be a list of integers and each integer is the number of uses a token
        must have

        :param str requester: user name
        :param str requesterGroup: user group
        :param int numUses: number of uses

        :return: S_OK(tuple)/S_ERROR() -- tuple contain token, number uses
    """
    return self.rpcClient.generateToken(requester, requesterGroup, numUses)

  def renewProxy(self, proxyToBeRenewed=None, minLifeTime=3600, newProxyLifeTime=43200, proxyToConnect=None):
    """ Renew a proxy using the ProxyManager

        :param X509Chain proxyToBeRenewed: proxy to renew
        :param int minLifeTime: if proxy life time is less than this, renew. Skip otherwise
        :param int newProxyLifeTime: life time of new proxy
        :param X509Chain proxyToConnect: proxy to use for connecting to the service

        :return: S_OK(X509Chain)/S_ERROR()
    """
    retVal = multiProxyArgument(proxyToBeRenewed)
    if not retVal['Value']:
      return retVal
    proxyToRenewDict = retVal['Value']

    secs = proxyToRenewDict['chain'].getRemainingSecs()['Value']
    if secs > minLifeTime:
      deleteMultiProxy(proxyToRenewDict)
      return S_OK()

    if not proxyToConnect:
      proxyToConnectDict = {'chain': False, 'tempFile': False}
    else:
      retVal = multiProxyArgument(proxyToConnect)
      if not retVal['Value']:
        deleteMultiProxy(proxyToRenewDict)
        return retVal
      proxyToConnectDict = retVal['Value']

    userDN = proxyToRenewDict['chain'].getIssuerCert()['Value'].getSubjectDN()['Value']
    retVal = proxyToRenewDict['chain'].getDIRACGroup()
    if not retVal['OK']:
      deleteMultiProxy(proxyToRenewDict)
      deleteMultiProxy(proxyToConnectDict)
      return retVal
    userGroup = retVal['Value']
    result = getUsernameForDN(userDN)
    if not result['OK']:
      return result
    userName = result['Value']
    limited = proxyToRenewDict['chain'].isLimitedProxy()['Value']

    voms = VOMS()
    retVal = voms.getVOMSAttributes(proxyToRenewDict['chain'])
    if not retVal['OK']:
      deleteMultiProxy(proxyToRenewDict)
      deleteMultiProxy(proxyToConnectDict)
      return retVal

    if retVal['Value']:
      retVal = self.downloadVOMSProxy(userName, userGroup, limited=limited,
                                      requiredTimeLeft=newProxyLifeTime,
                                      proxyToConnect=proxyToConnectDict['chain'])
    else:
      retVal = self.downloadProxy(userName, userGroup, limited=limited,
                                  requiredTimeLeft=newProxyLifeTime,
                                  proxyToConnect=proxyToConnectDict['chain'])

    deleteMultiProxy(proxyToRenewDict)
    deleteMultiProxy(proxyToConnectDict)

    if not retVal['OK']:
      return retVal
    chain = retVal['Value']

    if not proxyToRenewDict['tempFile']:
      return chain.dumpAllToFile(proxyToRenewDict['file'])

    return S_OK(chain)

  def getVOMSAttributes(self, chain):
    """ Get the voms attributes for a chain

        :param X509Chain chain: proxy as a chain

        :return: S_OK(str)/S_ERROR()
    """
    return VOMS().getVOMSAttributes(chain)

  def getDBContents(self, condDict={}, start=0, limit=0):
    """ Get the contents of the db

        :param dict condDict: search condition
        :param int start: search limit start
        :param int start: search limit amount

        :return: S_OK(dict)/S_ERROR() -- dict contain fields, record list, total records
    """
    return self.rpcClient.getContents(condDict, [['UserDN', 'DESC']], 0, 0)

  def getUploadedProxiesDetails(self, user=None, group=None):
    """ Get the details about an uploaded proxy

        :param str user: user name
        :param str group: group name

        :return: S_OK(dict)/S_ERROR() -- dict contain fields, record list, total records
    """
    return self.getDBContents({'UserName': user, 'UserGroup': group})

  def getUploadedProxyLifeTime(self, user, group):
    """ Get the remaining seconds for an uploaded proxy

        :param str user: user name
        :param str group: group name

        :return: S_OK(int)/S_ERROR()
    """
    result = self.getUploadedProxiesDetails(user, group)
    if not result['OK']:
      return result
    for proxyDict in result['Value']['Dictionaries']:
      if user == proxyDict['user'] and group == proxyDict['group']:
        td = proxyDict['expirationtime'] - datetime.datetime.utcnow()
        secondsLeft = td.days * 86400 + td.seconds
        return S_OK(max(0, secondsLeft))
    return S_OK(0)

  def getUserProxiesInfo(self):
    """ Get the user proxies uploaded info

        :return: S_OK(dict)/S_ERROR()
    """
    result = self.rpcClient.getUserProxiesInfo()
    if 'rpcStub' in result:
      result.pop('rpcStub')
    return result

  def deleteProxy(self, userDNs):
    """ Delete proxy

        :param list userDNs: user DNs

        :return: S_OK()/S_ERROR()
    """
    return self.rpcClient.deleteProxyBundle(userDNs)

  def getGroupsStatusByUsername(self, username, groups=None):
    """ Get status of every group for DIRAC user:

        :param str username: user name

        :return: S_OK(dict)/S_ERROR()
    """
    # # { <user>: {
    # #       <group>: [
    # #         {
    # #           Status: ..,
    # #           Comment: ..,
    # #           DN: ..,
    # #           Action: {
    # #             <fn>: { <opns> }
    # #           }
    # #         },
    # #         { ... }
    # #       ],
    # #       <group2>: [ ... ]
    # # } }
    return self.rpcClient.getGroupsStatusByUsername(username, groups)

gProxyManager = ProxyManagerClient()
