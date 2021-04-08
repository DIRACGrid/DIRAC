""" ProxyManager is the implementation of the ProxyManagement service in the DISET framework

    .. literalinclude:: ../ConfigTemplate.cfg
      :start-after: ##BEGIN ProxyManager:
      :end-before: ##END
      :dedent: 2
      :caption: ProxyManager options
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Security import Properties
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers import Registry


class ProxyManagerHandler(RequestHandler):

  __maxExtraLifeFactor = 1.5
  __proxyDB = None

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    useMyProxy = cls.srv_getCSOption("UseMyProxy", False)
    try:
      result = ObjectLoader().loadObject('FrameworkSystem.DB.ProxyDB')
      if not result['OK']:
        gLogger.error('Failed to load ProxyDB class: %s' % result['Message'])
        return result
      dbClass = result['Value']

      cls.__proxyDB = dbClass(useMyProxy=useMyProxy)

    except RuntimeError as excp:
      return S_ERROR("Can't connect to ProxyDB: %s" % excp)
    gThreadScheduler.addPeriodicTask(900, cls.__proxyDB.purgeExpiredTokens, elapsedTime=900)
    gThreadScheduler.addPeriodicTask(900, cls.__proxyDB.purgeExpiredRequests, elapsedTime=900)
    gThreadScheduler.addPeriodicTask(21600, cls.__proxyDB.purgeLogs)
    gThreadScheduler.addPeriodicTask(3600, cls.__proxyDB.purgeExpiredProxies)
    if useMyProxy:
      gLogger.info("MyProxy: %s\n MyProxy Server: %s" % (useMyProxy, cls.__proxyDB.getMyProxyServer()))
    return S_OK()

  def __generateUserProxiesInfo(self):
    """ Generate information dict about user proxies

        :return: dict
    """
    proxiesInfo = {}
    credDict = self.getRemoteCredentials()
    result = Registry.getDNForUsername(credDict['username'])
    if not result['OK']:
      return result
    selDict = {'UserDN': result['Value']}
    result = self.__proxyDB.getProxiesContent(selDict, {})
    if not result['OK']:
      return result
    contents = result['Value']
    userDNIndex = contents['ParameterNames'].index("UserDN")
    userGroupIndex = contents['ParameterNames'].index("UserGroup")
    expirationIndex = contents['ParameterNames'].index("ExpirationTime")
    for record in contents['Records']:
      userDN = record[userDNIndex]
      if userDN not in proxiesInfo:
        proxiesInfo[userDN] = {}
      userGroup = record[userGroupIndex]
      proxiesInfo[userDN][userGroup] = record[expirationIndex]
    return proxiesInfo

  def __addKnownUserProxiesInfo(self, retDict):
    """ Given a S_OK/S_ERR add a proxies entry with info of all the proxies a user has uploaded

        :return: S_OK(dict)/S_ERROR()
    """
    retDict['proxies'] = self.__generateUserProxiesInfo()
    return retDict

  auth_getUserProxiesInfo = ['authenticated']
  types_getUserProxiesInfo = []

  def export_getUserProxiesInfo(self):
    """ Get the info about the user proxies in the system

        :return: S_OK(dict)
    """
    return S_OK(self.__generateUserProxiesInfo())

  # WARN: Since v7r1 requestDelegationUpload method use only first argument!
  # WARN:   Second argument for compatibility with older versions
  types_requestDelegationUpload = [six.integer_types]

  def export_requestDelegationUpload(self, requestedUploadTime, diracGroup=None):
    """ Request a delegation. Send a delegation request to client

        :param int requestedUploadTime: requested live time

        :return: S_OK(dict)/S_ERROR() -- dict contain id and proxy as string of the request
    """
    if diracGroup:
      self.log.warn("Since v7r1 requestDelegationUpload method use only first argument!")
    credDict = self.getRemoteCredentials()
    user = '%s:%s' % (credDict['username'], credDict['group'])
    result = self.__proxyDB.generateDelegationRequest(credDict['x509Chain'], credDict['DN'])
    if result['OK']:
      gLogger.info("Upload request by %s given id %s" % (user, result['Value']['id']))
    else:
      gLogger.error("Upload request failed", "by %s : %s" % (user, result['Message']))
    return result

  types_completeDelegationUpload = [six.integer_types, six.string_types]

  def export_completeDelegationUpload(self, requestId, pemChain):
    """ Upload result of delegation

        :param int requestId: identity number
        :param str pemChain: certificate as string

        :return: S_OK(dict)/S_ERROR() -- dict contain proxies
    """
    credDict = self.getRemoteCredentials()
    userId = "%s:%s" % (credDict['username'], credDict['group'])
    retVal = self.__proxyDB.completeDelegation(requestId, credDict['DN'], pemChain)
    if not retVal['OK']:
      gLogger.error("Upload proxy failed", "id: %s user: %s message: %s" % (requestId, userId, retVal['Message']))
      return self.__addKnownUserProxiesInfo(retVal)
    gLogger.info("Upload %s by %s completed" % (requestId, userId))
    return self.__addKnownUserProxiesInfo(S_OK())

  types_getRegisteredUsers = []

  def export_getRegisteredUsers(self, validSecondsRequired=0):
    """ Get the list of users who have a valid proxy in the system

        :param int validSecondsRequired: required seconds the proxy is valid for

        :return: S_OK(list)/S_ERROR() -- list contain dicts with user name, DN, group
                                         expiration time, persistent flag
    """
    credDict = self.getRemoteCredentials()
    if Properties.PROXY_MANAGEMENT not in credDict['properties']:
      return self.__proxyDB.getUsers(validSecondsRequired, userMask=credDict['username'])
    return self.__proxyDB.getUsers(validSecondsRequired)

  def __checkProperties(self, requestedUserDN, requestedUserGroup):
    """ Check the properties and return if they can only download limited proxies if authorized

        :param str requestedUserDN: user DN
        :param str requestedUserGroup: DIRAC group

        :return: S_OK(boolean)/S_ERROR()
    """
    credDict = self.getRemoteCredentials()
    if Properties.FULL_DELEGATION in credDict['properties']:
      return S_OK(False)
    if Properties.LIMITED_DELEGATION in credDict['properties']:
      return S_OK(True)
    if Properties.PRIVATE_LIMITED_DELEGATION in credDict['properties']:
      if credDict['DN'] != requestedUserDN:
        return S_ERROR("You are not allowed to download any proxy")
      if Properties.PRIVATE_LIMITED_DELEGATION not in Registry.getPropertiesForGroup(requestedUserGroup):
        return S_ERROR("You can't download proxies for that group")
      return S_OK(True)
    # Not authorized!
    return S_ERROR("You can't get proxies!")

  types_getProxy = [six.string_types, six.string_types, six.string_types, six.integer_types]

  def export_getProxy(self, userDN, userGroup, requestPem, requiredLifetime):
    """ Get a proxy for a userDN/userGroup

        :param requestPem: PEM encoded request object for delegation
        :param requiredLifetime: Argument for length of proxy

          * Properties:
              * FullDelegation <- permits full delegation of proxies
              * LimitedDelegation <- permits downloading only limited proxies
              * PrivateLimitedDelegation <- permits downloading only limited proxies for one self
    """
    credDict = self.getRemoteCredentials()

    result = self.__checkProperties(userDN, userGroup)
    if not result['OK']:
      return result
    forceLimited = result['Value']

    self.__proxyDB.logAction("download proxy", credDict['DN'], credDict['group'], userDN, userGroup)
    return self.__getProxy(userDN, userGroup, requestPem, requiredLifetime, forceLimited)

  def __getProxy(self, userDN, userGroup, requestPem, requiredLifetime, forceLimited):
    """ Internal to get a proxy

        :param str userDN: user DN
        :param str userGroup: DIRAC group
        :param str requestPem: dump of request certificate
        :param int requiredLifetime: requested live time of proxy
        :param boolean forceLimited: limited proxy

        :return: S_OK(str)/S_ERROR()
    """
    retVal = self.__proxyDB.getProxy(userDN, userGroup, requiredLifeTime=requiredLifetime)
    if not retVal['OK']:
      return retVal
    chain, secsLeft = retVal['Value']
    # If possible we return a proxy 1.5 longer than requested
    requiredLifetime = int(min(secsLeft, requiredLifetime * self.__maxExtraLifeFactor))
    retVal = chain.generateChainFromRequestString(requestPem,
                                                  lifetime=requiredLifetime,
                                                  requireLimited=forceLimited)
    if not retVal['OK']:
      return retVal
    return S_OK(retVal['Value'])

  types_getVOMSProxy = [six.string_types, six.string_types,
                        six.string_types, six.integer_types,
                        [six.string_types, type(None), bool]]

  def export_getVOMSProxy(self, userDN, userGroup, requestPem, requiredLifetime, vomsAttribute=None):
    """ Get a proxy for a userDN/userGroup

        :param requestPem: PEM encoded request object for delegation
        :param requiredLifetime: Argument for length of proxy
        :param vomsAttribute: VOMS attr to add to the proxy

          * Properties :
              * FullDelegation <- permits full delegation of proxies
              * LimitedDelegation <- permits downloading only limited proxies
              * PrivateLimitedDelegation <- permits downloading only limited proxies for one self
    """
    credDict = self.getRemoteCredentials()

    result = self.__checkProperties(userDN, userGroup)
    if not result['OK']:
      return result
    forceLimited = result['Value']

    self.__proxyDB.logAction("download voms proxy", credDict['DN'], credDict['group'], userDN, userGroup)
    return self.__getVOMSProxy(userDN, userGroup, requestPem, requiredLifetime, vomsAttribute, forceLimited)

  def __getVOMSProxy(self, userDN, userGroup, requestPem, requiredLifetime, vomsAttribute, forceLimited):
    retVal = self.__proxyDB.getVOMSProxy(userDN, userGroup,
                                         requiredLifeTime=requiredLifetime,
                                         requestedVOMSAttr=vomsAttribute)
    if not retVal['OK']:
      return retVal
    chain, secsLeft = retVal['Value']
    # If possible we return a proxy 1.5 longer than requested
    requiredLifetime = int(min(secsLeft, requiredLifetime * self.__maxExtraLifeFactor))
    return chain.generateChainFromRequestString(requestPem,
                                                lifetime=requiredLifetime,
                                                requireLimited=forceLimited)

  types_setPersistency = [six.string_types, six.string_types, bool]

  def export_setPersistency(self, userDN, userGroup, persistentFlag):
    """ Set the persistency for a given dn/group

        :param str userDN: user DN
        :param str userGroup: DIRAC group
        :param boolean persistentFlag: if proxy persistent

        :return: S_OK()/S_ERROR()
    """
    retVal = self.__proxyDB.setPersistencyFlag(userDN, userGroup, persistentFlag)
    if not retVal['OK']:
      return retVal
    credDict = self.getRemoteCredentials()
    self.__proxyDB.logAction("set persistency to %s" % bool(persistentFlag),
                             credDict['DN'], credDict['group'], userDN, userGroup)
    return S_OK()

  types_deleteProxyBundle = [(list, tuple)]

  def export_deleteProxyBundle(self, idList):
    """ delete a list of id's

        :param list,tuple idList: list of identity numbers

        :return: S_OK(int)/S_ERROR()
    """
    errorInDelete = []
    deleted = 0
    for _id in idList:
      if len(_id) != 2:
        errorInDelete.append("%s doesn't have two fields" % str(_id))
      retVal = self.export_deleteProxy(_id[0], _id[1])
      if not retVal['OK']:
        errorInDelete.append("%s : %s" % (str(_id), retVal['Message']))
      else:
        deleted += 1
    if errorInDelete:
      return S_ERROR("Could not delete some proxies: %s" % ",".join(errorInDelete))
    return S_OK(deleted)

  types_deleteProxy = [(list, tuple)]

  def export_deleteProxy(self, userDN, userGroup):
    """ Delete a proxy from the DB

        :param str userDN: user DN
        :param str userGroup: DIRAC group

        :return: S_OK()/S_ERROR()
    """
    credDict = self.getRemoteCredentials()
    if Properties.PROXY_MANAGEMENT not in credDict['properties']:
      if userDN != credDict['DN']:
        return S_ERROR("You aren't allowed!")
    retVal = self.__proxyDB.deleteProxy(userDN, userGroup)
    if not retVal['OK']:
      return retVal
    self.__proxyDB.logAction("delete proxy", credDict['DN'], credDict['group'], userDN, userGroup)
    return S_OK()

  types_getContents = [dict, (list, tuple), six.integer_types, six.integer_types]

  def export_getContents(self, selDict, sortDict, start, limit):
    """ Retrieve the contents of the DB

        :param dict selDict: selection fields
        :param list,tuple sortDict: sorting fields
        :param int start: search limit start
        :param int start: search limit amount

        :return: S_OK(dict)/S_ERROR() -- dict contain fields, record list, total records
    """
    credDict = self.getRemoteCredentials()
    if Properties.PROXY_MANAGEMENT not in credDict['properties']:
      selDict['UserName'] = credDict['username']
    return self.__proxyDB.getProxiesContent(selDict, sortDict, start, limit)

  types_getLogContents = [dict, (list, tuple), six.integer_types, six.integer_types]

  def export_getLogContents(self, selDict, sortDict, start, limit):
    """ Retrieve the contents of the DB

        :param dict selDict: selection fields
        :param list,tuple sortDict: search filter
        :param int start: search limit start
        :param int start: search limit amount

        :return: S_OK(dict)/S_ERROR() -- dict contain fields, record list, total records
    """
    return self.__proxyDB.getLogsContent(selDict, sortDict, start, limit)

  types_generateToken = [six.string_types, six.string_types, six.integer_types]

  def export_generateToken(self, requesterDN, requesterGroup, tokenUses):
    """ Generate tokens for proxy retrieval

        :param str requesterDN: user DN
        :param str requesterGroup: DIRAC group
        :param int tokenUses: number of uses

        :return: S_OK(tuple)/S_ERROR() -- tuple contain token, number uses
    """
    credDict = self.getRemoteCredentials()
    self.__proxyDB.logAction("generate tokens", credDict['DN'], credDict['group'], requesterDN, requesterGroup)
    return self.__proxyDB.generateToken(requesterDN, requesterGroup, numUses=tokenUses)

  types_getProxyWithToken = [six.string_types, six.string_types, six.string_types, six.integer_types, six.string_types]

  def export_getProxyWithToken(self, userDN, userGroup, requestPem, requiredLifetime, token):
    """ Get a proxy for a userDN/userGroup

        :param requestPem: PEM encoded request object for delegation
        :param requiredLifetime: Argument for length of proxy
        :param token: Valid token to get a proxy

          * Properties:
              * FullDelegation <- permits full delegation of proxies
              * LimitedDelegation <- permits downloading only limited proxies
              * PrivateLimitedDelegation <- permits downloading only limited proxies for one self
    """
    credDict = self.getRemoteCredentials()
    result = self.__proxyDB.useToken(token, credDict['DN'], credDict['group'])
    gLogger.info("Trying to use token %s by %s:%s" % (token, credDict['DN'], credDict['group']))
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR("Proxy token is invalid")
    self.__proxyDB.logAction("used token", credDict['DN'], credDict['group'], userDN, userGroup)

    result = self.__checkProperties(userDN, userGroup)
    if not result['OK']:
      return result
    self.__proxyDB.logAction("download proxy with token", credDict['DN'], credDict['group'], userDN, userGroup)
    return self.__getProxy(userDN, userGroup, requestPem, requiredLifetime, True)

  types_getVOMSProxyWithToken = [six.string_types, six.string_types,
                                 six.string_types, six.integer_types,
                                 [six.string_types, type(None)]]

  def export_getVOMSProxyWithToken(self, userDN, userGroup, requestPem, requiredLifetime, token, vomsAttribute=None):
    """ Get a proxy for a userDN/userGroup

        :param requestPem: PEM encoded request object for delegation
        :param requiredLifetime: Argument for length of proxy
        :param vomsAttribute: VOMS attr to add to the proxy

          * Properties :
              * FullDelegation <- permits full delegation of proxies
              * LimitedDelegation <- permits downloading only limited proxies
              * PrivateLimitedDelegation <- permits downloading only limited proxies for one self
    """
    credDict = self.getRemoteCredentials()
    result = self.__proxyDB.useToken(token, credDict['DN'], credDict['group'])
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR("Proxy token is invalid")
    self.__proxyDB.logAction("used token", credDict['DN'], credDict['group'], userDN, userGroup)

    result = self.__checkProperties(userDN, userGroup)
    if not result['OK']:
      return result
    self.__proxyDB.logAction("download voms proxy with token", credDict['DN'], credDict['group'], userDN, userGroup)
    return self.__getVOMSProxy(userDN, userGroup, requestPem, requiredLifetime, vomsAttribute, True)
