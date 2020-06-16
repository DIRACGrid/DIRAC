""" ProxyManager is the implementation of the ProxyManagement service
    in the DISET framework
"""

__RCSID__ = "$Id$"

from past.builtins import long

import os
import six
import pickle
import pprint
import threading

from DIRAC import gLogger, S_OK, S_ERROR, rootPath, gConfig
from DIRAC.Core.Security import Properties
from DIRAC.Core.Security.ProxyFile import writeChainToProxyFile
from DIRAC.Core.Security.VOMSService import VOMSService
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import ThreadSafe
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.Resources.ProxyProvider.ProxyProviderFactory import ProxyProviderFactory

gVOMSCacheSync = ThreadSafe.Synchronizer()
gVOMSFileSync = ThreadSafe.Synchronizer()


class ProxyManagerHandler(RequestHandler):
  """ Proxy manager service

      Contain __VOMSesUsersCache cache, with next structure:
        Key: VOMS VO name
        Value: S_OK(dict)/S_ERROR() -- dictionary contain:
            { <user DN>: {
                Roles: [<list of roles>],
                suspended: bool
                certSuspended: bool,
                ...
              }
            }
  """

  __notify = NotificationClient()
  __VOMSesUsersCache = DictCache()
  __maxExtraLifeFactor = 1.5
  __proxyDB = None

  @classmethod
  @gVOMSCacheSync
  def saveVOMSInfoToCache(cls, vo, infoDict):
    """ Save cache to file

        :param str vo: VO name
        :param dict infoDict: dictionary with information about users
    """
    cls.__VOMSesUsersCache.add(vo, 3600 * 24, infoDict)

  @classmethod
  @gVOMSFileSync
  def saveVOMSInfoToFile(cls, vo, infoDict):
    """ Save cache to file

        :param str vo: VO name
        :param dict infoDict: dictionary with information about users
    """
    if not os.path.exists(cls.__workDir):
      os.makedirs(cls.__workDir)
    with open(os.path.join(cls.__workDir, vo + '.pkl'), 'wb+') as f:
      pickle.dump(infoDict, f, pickle.HIGHEST_PROTOCOL)

  @classmethod
  @gVOMSFileSync
  def getVOMSInfoFromFile(cls, vo):
    """ Load VO cache from file

        :param str vo: VO name

        :return: S_OK(dict)/S_ERROR() -- dictionary with information about users
    """
    try:
      with open(os.path.join(cls.__workDir, vo + '.pkl'), 'rb') as f:
        return S_OK(pickle.load(f))
    except Exception as e:
      return S_ERROR(str(e))

  @classmethod
  @gVOMSFileSync
  def getVOMSInfoFromCache(cls, vo=None):
    """ Load VO cache from file

        :param str vo: VO name

        :return: S_OK(dict)/S_ERROR() -- dictionary with information about users
    """
    return cls.__VOMSesUsersCache.get(vo) if vo else cls.__VOMSesUsersCache.getDict()

  @classmethod
  def __refreshVOMSesUsersCache(cls, voList=None):
    """ Update cache with information about active users from supported VOs

        :param list voList: VOs to update

        :return: S_OK()/S_ERROR()
    """
    def getVOInfo(vo):
      """ Process to get information from VOMS API

          :param str vo: VO name
      """
      usersDict = {}
      result = S_ERROR('Cannot found administrators for %s VOMS VO' % vo)
      voAdmins = Registry.getVOOption(vo, "VOAdmin", [])

      for group in Registry.getGroupsForVO(vo).get('Value') or []:
        for user in voAdmins:
          # Try to get proxy for any VO admin
          result = cls.__proxyDB.getProxy(user, group, 1800)
          if result['OK']:
            # Now we have a proxy, lets dump it to file
            result = writeChainToProxyFile(result['Value'][0], '/tmp/x509_syncTmp')
            if result['OK']:
              # Get users from VOMS
              result = VOMSService(vo=vo).getUsers(result['Value'])
              if result['OK']:
                cls.saveVOMSInfoToCache(vo, result)
                cls.saveVOMSInfoToFile(vo, result)
                return

      gLogger.error(result['Message'])
      if not cls.getVOMSInfoFromCache(vo) or not cls.getVOMSInfoFromCache(vo)['OK']:
        cls.saveVOMSInfoToCache(vo, result)
      # ################# getVOInfo ###################### #

    gLogger.info('Update VOMSes information..')
    if not voList:
      result = Registry.getVOsWithVOMS()
      if not result['OK']:
        return result
      voList = result['Value']

    for vo in voList:
      processThread = threading.Thread(target=getVOInfo, args=[vo])
      processThread.start()

    # if diracAdminsNotifyDict:
    #   subject = '[ProxyManager] Cannot update users from %s VOMS VOs.' % ', '.join(diracAdminsNotifyDict.keys())
    #   body = pprint.pformat(diracAdminsNotifyDict)
    #   body += "\n------\n This is a notification from the DIRAC ProxyManager service, please do not reply."
    #   #cls.__notify.sendMail(getEmailsForGroup('dirac_admin'), subject, body)
    return S_OK()

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    """ Initialization

        :param dict serviceInfoDict: service information dictionary

        :return: S_OK()/S_ERROR()
    """
    cls.__workDir = os.path.join(gConfig.getValue('/LocalSite/InstancePath', rootPath), 'work/ProxyManager')
    useMyProxy = cls.srv_getCSOption("UseMyProxy", False)
    try:
      result = ObjectLoader().loadObject('FrameworkSystem.DB.ProxyDB', 'ProxyDB')
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
    gThreadScheduler.addPeriodicTask(3600 * 24, cls.__refreshVOMSesUsersCache)
    gLogger.info("MyProxy: %s\n MyProxy Server: %s" % (useMyProxy, cls.__proxyDB.getMyProxyServer()))
    return cls.__refreshVOMSesUsersCache()

  types_getVOMSesUsers = []

  def export_getVOMSesUsers(self):
    """ Return fresh info from service about VOMSes

        :return: S_OK(dict)/S_ERROR()
    """
    VOMSesUsers = self.getVOMSInfoFromCache()
    result = Registry.getVOs()
    if not result['OK']:
      return result
    for vo in result['Value']:
      if vo not in VOMSesUsers:
        result = self.getVOMSInfoFromFile(vo)
        if result['OK']:
          VOMSesUsers[vo] = result['Value']
          continue
        VOMSesUsers[vo] = S_ERROR('No information from "%s" VOMS VO' % vo)
    return S_OK(VOMSesUsers)

  def __generateUserProxiesInfo(self):
    """ Generate information dict about user proxies

        :return: dict
    """
    proxiesInfo = {}
    credDict = self.getRemoteCredentials()
    result = Registry.getDNsForUsername(credDict['username'])
    if not result['OK']:
      return result
    result = self.__proxyDB.getProxiesContent({'UserDN': result['Value']})
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

  auth_getUserProxiesInfo = ['authenticated']
  types_getUserProxiesInfo = []

  def export_getUserProxiesInfo(self):
    """ Get the info about the user proxies in the system

        :return: S_OK(dict)
    """
    return S_OK(self.__generateUserProxiesInfo())

  # WARN: Since v7r1 requestDelegationUpload method not use arguments!
  auth_requestDelegationUpload = ['authenticated']
  types_requestDelegationUpload = []

  def export_requestDelegationUpload(self, requestedUploadTime=None, diracGroup=None):
    """ Request a delegation. Send a delegation request to client

        :return: S_OK(dict)/S_ERROR() -- dict contain id and proxy as string of the request
    """
    if diracGroup:
      # WARN: Since v7r1, DIRAC has implemented the ability to store only one proxy and
      # WARN:   dynamically add a group at the request of a proxy. This means that group extensions
      # WARN:   doesn't need for storing proxies.
      return S_ERROR("Proxy with DIRAC group or VOMS extensions not allowed to be uploaded.")

    credDict = self.getRemoteCredentials()
    result = self.__proxyDB.generateDelegationRequest(credDict)
    if result['OK']:
      gLogger.info("Upload request by %s:%s given id %s" %
                   (credDict['username'], credDict['group'], result['Value']['id']))
    else:
      gLogger.error("Upload request failed", "by %s:%s : %s" %
                    (credDict['username'], credDict['group'], result['Message']))
    return result

  types_completeDelegationUpload = [six.integer_types, basestring]

  def export_completeDelegationUpload(self, requestId, pemChain):
    """ Upload result of delegation

        :param requestId: identity number
        :type requestId: int or long
        :param str pemChain: certificate as string

        :return: S_OK(dict)/S_ERROR() -- dict contain proxies
    """

    credDict = self.getRemoteCredentials()
    userId = "%s:%s" % (credDict['username'], credDict['group'])
    retVal = self.__proxyDB.completeDelegation(requestId, credDict['DN'], pemChain)
    if not retVal['OK']:
      gLogger.error("Upload proxy failed", "id: %s user: %s message: %s" % (requestId, userId, retVal['Message']))
      return retVal
    gLogger.info("Upload %s by %s completed" % (requestId, userId))
    return S_OK(self.__generateUserProxiesInfo())

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

  def __checkProperties(self, requestedUsername, requestedUserGroup, credDict, personal):
    """ Check the properties and return if they can only download limited proxies if authorized

        :param str requestedUsername: user name
        :param str requestedUserGroup: DIRAC group
        :param dict credDict: remote credentials
        :param bool personal: get personal proxy

        :return: S_OK(bool)/S_ERROR()
    """
    if personal:
      csSection = PathFinder.getServiceSection('Framework/ProxyManager')
      if requestedUsername != credDict['username'] or requestedUserGroup != credDict['group']:
        return S_ERROR("You can't get %s@%s proxy!" % (credDict['username'], credDict['group']))
      elif not gConfig.getValue('%s/downloadablePersonalProxy' % csSection, False):
        return S_ERROR("You can't get proxy, configuration settings not allow to do that.")
      else:
        return S_OK(False)

    if Properties.FULL_DELEGATION in credDict['properties']:
      return S_OK(False)
    if Properties.LIMITED_DELEGATION in credDict['properties']:
      return S_OK(True)
    if Properties.PRIVATE_LIMITED_DELEGATION in credDict['properties']:
      if credDict['username'] != requestedUsername:
        return S_ERROR("You are not allowed to download any proxy")
      if Properties.PRIVATE_LIMITED_DELEGATION not in Registry.getPropertiesForGroup(requestedUserGroup):
        return S_ERROR("You can't download proxies for that group")
      return S_OK(True)
    # Not authorized!
    return S_ERROR("You can't get proxies!")

  types_getProxy = [basestring, basestring, basestring, six.integer_types]

  def export_getProxy(self, instance, group, requestPem, requiredLifetime,
                      token=None, vomsAttribute=None, personal=False):
    """ Get a proxy for a user/group

        :param str instance: user name or DN
        :param str group: DIRAC group
        :param str requestPem: PEM encoded request object for delegation
        :param int requiredLifetime: Argument for length of proxy
        :param str token: token that need to use
        :param bool vomsAttribute: make proxy with VOMS extension
        :param bool personal: get personal proxy

          * Properties:
              * FullDelegation <- permits full delegation of proxies
              * LimitedDelegation <- permits downloading only limited proxies
              * PrivateLimitedDelegation <- permits downloading only limited proxies for one self

          * Properties for personal proxy:
              * NormalUser <- permits full delegation of proxies

        :return: S_OK(str)/S_ERROR()
    """
    # Test that group enable to download
    if not Registry.isDownloadableGroup(group):
      return S_ERROR('"%s" group is disable to download.' % group)

    # Read arguments
    result = self.__getDNAndUsername(instance, group)
    if not result['OK']:
      return result
    user, userDN = result['Value']

    credDict = self.getRemoteCredentials()

    if token:
      result = self.__proxyDB.useToken(token, credDict['username'], credDict['group'])
      if not result['OK']:
        return result
      if not result['Value']:
        return S_ERROR("Proxy token is invalid")

    result = self.__checkProperties(user, group, credDict, personal)
    if not result['OK']:
      return result
    forceLimited = True if token else result['Value']

    log = "download %sproxy%s" % ('VOMS ' if vomsAttribute else '', 'with token' if token else '')
    self.__proxyDB.logAction(log, credDict['username'], credDict['group'], user, group)

    retVal = self.__proxyDB.getProxy(userDN, group, requiredLifeTime=requiredLifetime, voms=vomsAttribute)
    if not retVal['OK']:
      return retVal
    chain, secsLeft = retVal['Value']
    # If possible we return a proxy 1.5 longer than requested
    requiredLifetime = int(min(secsLeft, requiredLifetime * self.__maxExtraLifeFactor))
    return chain.generateChainFromRequestString(requestPem, lifetime=requiredLifetime,
                                                requireLimited=forceLimited)

  types_deleteProxyBundle = [(list, tuple)]

  def export_deleteProxyBundle(self, idList):
    """ Delete a list of id's

        :param idList: list of identity numbers
        :type idList: list or tuple

        :return: S_OK(int)/S_ERROR()
    """
    errorInDelete = []
    deleted = 0
    for _id in idList:
      if len(_id) != 2:
        errorInDelete.append("%s doesn't have two fields" % str(_id))
      if _id[0]:
        retVal = self.export_deleteProxy(_id[0], _id[1])
        if not retVal['OK']:
          errorInDelete.append("%s : %s" % (str(_id), retVal['Message']))
        else:
          deleted += 1
    if errorInDelete:
      return S_ERROR("Could not delete some proxies: %s" % ",".join(errorInDelete))
    return S_OK(deleted)

  types_deleteProxy = [str]

  def export_deleteProxy(self, instance, userGroup=None):
    """ Delete a proxy from the DB

        :param str instance: user name or DN
        :param str userGroup: DIRAC group

        :return: S_OK()/S_ERROR()
    """
    result = self.__getDNAndUsername(instance, userGroup)
    if not result['OK']:
      return result
    username, userDN = result['Value']
    if not userDN:
      return S_ERROR('Not DN found for %s user in %s group' % (username, userGroup))

    credDict = self.getRemoteCredentials()
    if Properties.PROXY_MANAGEMENT not in credDict['properties']:
      if username != credDict['username']:
        return S_ERROR("You aren't allowed!")
    retVal = self.__proxyDB.deleteProxy(userDN)
    if not retVal['OK']:
      return retVal
    self.__proxyDB.logAction("delete proxy", credDict['username'], credDict['group'], username, userGroup)
    return S_OK()

  types_getContents = [dict, (list, tuple), six.integer_types, six.integer_types]

  def export_getContents(self, selDict, conn, start=0, limit=0):
    """ Retrieve the contents of the DB

        :param dict selDict: selection fields
        :param list conn: filters
        :param int start: search limit start
        :param int start: search limit amount

        :return: S_OK(dict)/S_ERROR() -- dict contain fields, record list, total records
    """
    credDict = self.getRemoteCredentials()
    if Properties.PROXY_MANAGEMENT not in credDict['properties']:
      selDict['UserName'] = credDict['username']

    return self.__proxyDB.getProxiesContent(selDict, conn, start=start, limit=limit)

  types_getLogContents = [dict, (list, tuple), six.integer_types, six.integer_types]

  def export_getLogContents(self, selDict, sortDict, start, limit):
    """ Retrieve the contents of the DB

        :param dict selDict: selection fields
        :param list,tuple sortDict: search filter
        :param int,long start: search limit start
        :param int,long start: search limit amount

        :return: S_OK(dict)/S_ERROR() -- dict contain fields, record list, total records
    """
    return self.__proxyDB.getLogsContent(selDict, sortDict, start, limit)

  types_generateToken = [basestring, basestring, six.integer_types]

  def export_generateToken(self, requester, requesterGroup, tokenUses):
    """ Generate tokens for proxy retrieval

        :param basestring requester: user name or DN
        :param basestring requesterGroup: DIRAC group
        :param int,long tokenUses: number of uses

        :return: S_OK(tuple)/S_ERROR() -- tuple contain token, number uses
    """
    result = self.__getDNAndUsername(instance)
    if not result['OK']:
      return result
    requesterUsername, _ = result['Value']

    credDict = self.getRemoteCredentials()
    self.__proxyDB.logAction("generate tokens", credDict['username'], credDict['group'],
                             requesterUsername, requesterGroup)
    return self.__proxyDB.generateToken(requesterUsername, requesterGroup, numUses=tokenUses)

  types_getVOMSProxyWithToken = [basestring, basestring, basestring, six.integer_types, [basestring, type(None)]]
  @deprecated("This method is deprecated, you can use export_getProxy with token and vomsAttribute parameter")
  def export_getVOMSProxyWithToken(self, user, userGroup, requestPem, requiredLifetime, token, vomsAttribute=None):
    """ Get a proxy with VOMS extension for a user/userGroup by using token

        :param str user: user name
        :param str userGroup: DIRAC group
        :param str requestPem: PEM encoded request object for delegation
        :param int requiredLifetime: Argument for length of proxy
        :param str token: Valid token to get a proxy

        :return: S_OK(str)/S_ERROR()
    """
    return self.export_getProxy(user, userGroup, requestPem, requiredLifetime, token=token, vomsAttribute=vomsAttribute)

  types_getProxyWithToken = [basestring, basestring, basestring, six.integer_types, basestring]
  @deprecated("This method is deprecated, you can use export_getProxy with token parameter")
  def export_getProxyWithToken(self, user, userGroup, requestPem, requiredLifetime, token):
    """ Get a proxy for a user/userGroup by using token

        :param str user: user name
        :param str userGroup: DIRAC group
        :param str requestPem: PEM encoded request object for delegation
        :param int requiredLifetime: Argument for length of proxy
        :param str token: Valid token to get a proxy

        :return: S_OK(str)/S_ERROR()
    """
    return self.export_getProxy(user, userGroup, requestPem, requiredLifetime, token=token)

  types_getVOMSProxy = [basestring, basestring, basestring, six.integer_types, [basestring, type(None)]]
  @deprecated("This method is deprecated, you can use export_getProxy with vomsAttribute parameter")
  def export_getVOMSProxy(self, user, userGroup, requestPem, requiredLifetime, vomsAttribute=None):
    """ Get a proxy with VOMS extension for a user/userGroup

        :param str user: user name
        :param str userGroup: DIRAC group
        :param str requestPem: PEM encoded request object for delegation
        :param int requiredLifetime: Argument for length of proxy
        :param str token: Valid token to get a proxy

        :return: S_OK(str)/S_ERROR()
    """
    return self.export_getProxy(user, userGroup, requestPem, requiredLifetime, vomsAttribute=vomsAttribute)

  types_getGroupsStatusByUsername = [str]

  def export_getGroupsStatusByUsername(self, username, groups=None):
    """ Get status of every group for DIRAC user:
          {
            <group>: {
              Status: ..,
              Comment: ..,
              DN: ..,
              Action: [ <fn>, [ <opns> ] ]
            },
            { ... }
            <group2>: {} ... }
          }

        :param str username: user name

        :return: S_OK(dict)/S_ERROR()
    """
    statusDict = {}
    if not groups:
      result = Registry.getGroupsForUser(username)
      if not result['OK']:
        return result
      groups = result['Value']

    provDict = {}
    groupDict = {}
    # Sort user DNs for groups and proxy providers
    for group in groups:
      result = Registry.getDNsForUsernameInGroup(username, group)
      if not result['OK']:
        if group not in statusDict:
          statusDict[group] = [{'Status': 'fail', 'Comment': result['Message']}]
        continue
      for dn in [result['Value'][0]]:  # we get only fist DN for now
        result = self.__proxyDB.getProxyProviderForDN(dn, username=username)
        if not result['OK']:
          return result
        pProvider = result['Value']
        if group not in groupDict:
          groupDict[group] = []
        if pProvider not in provDict:
          provDict[pProvider] = []
        provDict[pProvider] = list(set(provDict[pProvider] + [dn]))
        groupDict[group] = list(set(groupDict[group] + [dn]))

    # Check VOMS VO
    for group, dns in groupDict.items():
      if group not in statusDict:
        statusDict[group] = []

      vo = Registry.getGroupOption(group, 'VO')

      result = Registry.getVOsWithVOMS(voList=[vo])
      if not result['OK']:
        return result
      if not result['Value']:
        continue

      result = Registry.getVOMSServerInfo(vo)
      if not result['OK']:
        return result
      vomsServers = result['Value'][vo]["Servers"] if result['Value'] else {}
      vomsServerURL = 'https://%s:8443/voms/register/start.action' % vomsServers.keys()[0]

      result = self.getVOMSInfoFromCache(vo)
      if not result:
        result = self.getVOMSInfoFromFile(vo)
        if result['OK']:
          result = result['Value']
        result = S_ERROR('No information from "%s" VOMS VO' % vo)

      if not result or not result['OK']:
        err = '' if not result else result.get('Message', '')
        st = {'Status': 'unknown',
              "Comment": 'Fail to get %s VOMS VO information depended for this group: %s' % (vo, err)}
        for dn in dns:
          if dn in groupDict[group]:
            groupDict[group].remove(dn)
          st['DN'] = dn  
          statusDict[group].append(st)
        continue

      voData = result['Value']
      for dn in dns:
        if dn not in voData:
          if dn in groupDict[group]:
            groupDict[group].remove(dn)
          st = {'Status': 'failed', 'DN': dn, 'Action': ['openURL', [vomsServerURL]],
                'Comment': 'Make sure you(%s) are a member of the %s VOMS VO depended for this group. ' % (dn, vo)}
          statusDict[group].append(st)
          continue
        
        role = getGroupOption(group, 'VOMSRole')
        if not role:
          if voData[dn]['Suspended']:
            if dn in groupDict[group]:
              groupDict[group].remove(dn)
            st = {'Status': 'suspended', 'DN': dn, 'Action': ['openURL', [vomsServerURL]],
                  'Comment': 'It seems you(%s) are suspended in the %s VOMS VO depended for this group. ' % (dn, vo)}
            statusDict[group].append(st)
            continue
        else: 
          if role not in voData[dn]['VOMSRoles']:
            if dn in groupDict[group]:
              groupDict[group].remove(dn)
            st = {'Status': 'failed', 'DN': dn, 'Action': ['openURL', [vomsServerURL]],
                  'Comment': 'It seems you(%s) have no %s role in %s VOMS VO depended for this group. ' % (dn, role, vo)}
            statusDict[group].append(st)
            continue
          if role in voData[dn]['SuspendedRoles']:
            if dn in groupDict[group]:
              groupDict[group].remove(dn)
            st = {'Status': 'suspended', 'DN': dn, 'Action': ['openURL', [vomsServerURL]],
                  'Comment': 'It seems you(%s) are suspended for %s role in the %s VOMS VO depended for this group. ' % (dn, role, vo)}
            statusDict[group].append(st)
            continue

    # Check DNs by proxy providers
    for prov, dns in provDict.items():
      dns = list(set(dns))

      # Cut off existed DNs in DB
      result = self.__proxyDB.getValidDNs(dns)
      if not result['OK']:
        return result
      for dn, time, group in result['Value']:
        if dn in dns:
          dns.remove(dn)
        st = {'Status': 'ready', 'DN': dn,
              "Comment": 'proxy uploaded end valid to %s' % time}
        for _group, _dns in groupDict.items():
          if not group or group == _group:
            if _group not in statusDict:
              statusDict[_group] = []
            if dn in _dns:
              statusDict[_group].append(st)
      
      # If for DN not found proxy provider
      if not prov or prov == 'Certificate':
        for dn in dns:
          st = {'Status': 'not ready', 'DN': dn, "Action": ['upload proxy'],
                "Comment": 'You have no proxy with(%s) uploaded to DIRAC.' % dn}
          for group, dns in groupDict.items():
            if group not in statusDict:
              statusDict[group] = []
            if dn in dns:
              statusDict[group].append(st)
        continue
      
      # If proxy provider exist for DN
      result = ProxyProviderFactory().getProxyProvider(prov, proxyManager=self.__proxyDB)
      if not result['OK']:
        return result
      pProvObj = result['Value']
      for dn in dns:
        result = pProvObj.checkStatus(dn)
        st = result['Value'] if result['OK'] else {'Status': 'unknown', "Comment": result['Message']}
        st['DN'] = dn
        for group, dns in groupDict.items():
          if group not in statusDict:
            statusDict[group] = []
          if dn in dns:
            statusDict[group].append(st)
    
    resD = {}
    for group, statuses in statusDict.items():
      for stat in statuses:
        if stat['Status'] not in ["ready", "unknown"]:
          resD[group] = stat
          break
      if group not in resD:
        for stat in statuses:
          if stat['Status'] == "ready":
            resD[group] = stat
            break
      if group not in resD:
        resD[group] = statuses[0]

    return S_OK(resD)

  def __getDNAndUsername(self, instance, group=None):
    """ Parse instance to understand if it's DN and find username

        :param str instance: user name or DN
        :param str group: user group

        :return S_OK(tuple)/S_ERROR() -- tuple contain username and userDN
    """
    # Is instance user DN?
    if instance.startswith('/'):
      result = Registry.getUsernameForDN(instance)
      if not result['OK']:
        return result
      return S_OK((result['Value'], instance))
    # Is instance user name?
    result = Registry.getDNForUsernameInGroup(instance, group) if group else S_OK()
    if not result['OK']:
      return result
    return S_OK((instance, result['Value']))

  types_setPersistency = []
  @deprecated("Unuse")
  def export_setPersistency(self, user, userGroup, persistentFlag):
    """ Set the persistency for a given DN/group """
    return S_OK(True)
