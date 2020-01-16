""" ProxyManager is the implementation of the ProxyManagement service
    in the DISET framework
"""

__RCSID__ = "$Id$"

from past.builtins import long

import os
import six
import pickle
import pprint

from DIRAC import gLogger, S_OK, S_ERROR, rootPath, gConfig
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Security import Properties
from DIRAC.Core.Security.ProxyFile import writeChainToProxyFile
from DIRAC.Core.Security.VOMSService import VOMSService
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient


class ProxyManagerHandler(RequestHandler):

  __notify = NotificationClient()
  __VOMSesUsersCache = DictCache()
  __maxExtraLifeFactor = 1.5
  __proxyDB = None

  @classmethod
  def saveVOCacheToFile(cls, vo, infoDict):
    """ Save cache to file

        :param basestring vo: VO name
        :param dict infoDict: dictionary with information about users
    """
    if not os.path.exists(cls.__workDir):
      os.makedirs(cls.__workDir)
    with open(os.path.join(cls.__workDir, vo + '.pkl'), 'wb+') as f:
      pickle.dump(infoDict, f, pickle.HIGHEST_PROTOCOL)

  @classmethod
  def loadVOCacheFromFile(cls, vo):
    """ Load VO cache from file

        :param basestring vo: VO name
        
        :return: S_OK(dict)/S_ERROR() -- dictionary with information about users
    """
    try:
      with open(os.path.join(cls.__workDir, vo + '.pkl'), 'rb') as f:
        return S_OK(pickle.load(f))
    except Exception as e:
      return S_ERROR('Cannot read saved cahe: %s' % str(e))

  @classmethod
  def __refreshVOMSesUsersCache(cls, vos=None):
    """ Update cache with information about active users from supported VOs

        :param list vos: list of VOs that need to update, if None - update all VOs

        :return: S_OK()/S_ERROR()
    """    
    diracAdminsNotifyDict = {}
    absentAdminsProxies = []
    gLogger.info('Update VOMSes information..')
    if not vos:
      result = Registry.getVOs()  # Actualy need VOMS VOs, need to check for every VO VOMSName option exist
      if not result['OK']:
        return result
      vos = result['Value']

    for vo in vos:
      DNs = []
      # Get VO admin DNs from CS
      for user in Registry.getVOOption(vo, "VOAdmin", []):
        result = Registry.getDNsForUsername(user)
        if not result['OK']:
          gLogger.error(result['Message'])
          continue
        DNs += result['Value']
      # WARN: When has been used global sync server need to get VO admin DNs there
      if not DNs:
        diracAdminsNotifyDict[vo] = 'Cannot found administrators for %s VOMS VO' % vo
        gLogger.error('Cannot update users from "%s" VO.' % vo, 'No admin user found.')
        continue

      proxyChain = None
      for dn in DNs:
        # Try to get proxy for any VO admin user DN
        # WARN: For old version of DB, in new version has been used Clean_Proxies table without group
        result = cls.__proxyDB.getProxiesContent({'UserDN': dn, 'Group': Registry.getGroupsForVO(vo).get('Value') or []}, {})
        if result['OK']:
          if not result['Value']['Records']:
            result = S_ERROR('No administrators proxies found for "%s" VO.' % vo)
            continue
          for record in result['Value']['Records']:
            # WARN: For old version of DB, in new version has been used Clean_Proxies table without group
            result = cls.__proxyDB.getProxy(record[1], record[2], 1800)
            if result['OK'] and result['Value'][0]:
              proxyChain = result['Value'][0]
              break
        if proxyChain:
          # Now we have a proxy, lets dump it to file
          result = writeChainToProxyFile(proxyChain, '/tmp/x509_syncTmp')
          if result['OK']:
            # Get users from VOMS
            result = VOMSService(vo=vo).getUsers(result['Value'])
            if result['OK']:
              break
      if not proxyChain:
        absentAdminsProxies.append(vo)
        gLogger.error('Cannot update users from "%s" VO.' % vo, 'Need to upload admin proxy!')
        continue
      if not result['OK']:
        diracAdminsNotifyDict[vo] = result['Message']
        gLogger.error('Cannot update users from "%s" VO.' % vo, result['Message'])
        continue
      
      # Parse response
      voAllUsersDict = result['Value']
      voActiveUsersDict = {}
      for dn, dnInfo in voAllUsersDict.items():
        if dnInfo['suspended']:
          continue
        voActiveUsersDict[dn] = dnInfo
      cls.saveVOCacheToFile(vo, voActiveUsersDict)
      cls.__VOMSesUsersCache.add(vo, 3600 * 24, voActiveUsersDict)
    if diracAdminsNotifyDict:
      subject = '[ProxyManager] Cannot update users from %s VOMS VOs.' % ', '.join(diracAdminsNotifyDict.keys())
      body = pprint.pformat(diracAdminsNotifyDict)
      body += "\n------\n This is a notification from the DIRAC ProxyManager service, please do not reply."
      #cls.__notify.sendMail(Registry.getEmailsForGroup('dirac_admin'), subject, body)
    for vo in absentAdminsProxies:
      subject = '[DIRAC] Proxy of VO administrator is absent.'
      body = "Dear VO administrator,"
      body += "   please, upload your proxy."
      body += "\n------\n This is a notification from the DIRAC ProxyManager service, please do not reply."
      #cls.__notify.sendMail( Here need to get voadmin email or use dirac admins email , subject, body)
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
    VOMSesUsers = self.__VOMSesUsersCache.getDict()
    result = Registry.getVOs()
    if not result['OK']:
      return result
    for vo in result['Value']:
      if vo not in VOMSesUsers:
        result = self.loadVOCacheFromFile(vo)
        if result['OK']:
          VOMSesUsers[vo] = result['Value']
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
      self.log.warn("Proxy with DIRAC group or VOMS extensions must be not allowed to be uploaded.")

    credDict = self.getRemoteCredentials()
    result = self.__proxyDB.generateDelegationRequest(credDict)
    if result['OK']:
      gLogger.info("Upload request by %s:%s given id %s" % (credDict['username'], credDict['group'], result['Value']['id']))
    else:
      gLogger.error("Upload request failed", "by %s:%s : %s" % (credDict['username'], credDict['group'], result['Message']))
    return result

  types_completeDelegationUpload = [six.integer_types, basestring]

  def export_completeDelegationUpload(self, requestId, pemChain):
    """ Upload result of delegation

        :param int,long requestId: identity number
        :param basestring pemChain: certificate as string

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
      csSection = PathFinder.getServiceSection('Framework/Notification')
      if user != credDict['username'] or userGroup != credDict['group']:
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

  def export_getProxy(self, user, userGroup, requestPem, requiredLifetime, token=None, vomsAttribute=None, personal=False):
    """ Get a proxy for a user/userGroup

        :param str user: user name
        :param str userGroup: DIRAC group
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
    # WARN: Next block for compatability
    if not user.find("/"):  # Is it DN?
      result = Registry.getUsernameForDN(user)
      if not result['OK']:
        return result
      user = result['Value']

    credDict = self.getRemoteCredentials()

    if token:
      result = self.__proxyDB.useToken(token, credDict['username'], credDict['group'])
      if not result['OK']:
        return result
      if not result['Value']:
        return S_ERROR("Proxy token is invalid")

    result = self.__checkProperties(user, userGroup, credDict, personal)
    if not result['OK']:
      return result
    forceLimited = True if token else result['Value']

    log = "download %sproxy%s" % ('VOMS ' if vomsAttribute else '', 'with token' if token else '')
    self.__proxyDB.logAction(log, credDict['username'], credDict['group'], user, userGroup)

    retVal = self.__proxyDB.getProxy(user, userGroup, requiredLifeTime=requiredLifetime, voms=vomsAttribute)
    if not retVal['OK']:
      return retVal
    chain, secsLeft = retVal['Value']
    # If possible we return a proxy 1.5 longer than requested
    requiredLifetime = int(min(secsLeft, requiredLifetime * self.__maxExtraLifeFactor))
    return chain.generateChainFromRequestString(requestPem, lifetime=requiredLifetime,
                                                requireLimited=forceLimited)

  types_setPersistency = [basestring, basestring, bool]

  def export_setPersistency(self, user, userGroup, persistentFlag):
    """ Set the persistency for a given DN/group

        :param basestring user: user name
        :param basestring userGroup: DIRAC group
        :param bool persistentFlag: if proxy persistent

        :return: S_OK()/S_ERROR()
    """
    userDN = user
    # WARN: Next block for compatability
    if not user.find("/"):  # Is it DN?
      result = Registry.getUsernameForDN(user)
      if not result['OK']:
        return result
      user = result['Value']
    else:
      result = Registry.getDNForUsernameInGroup(user, userGroup)
      if not result['OK']:
        return result
      userDN = result['Value']

    retVal = self.__proxyDB.setPersistencyFlag(userDN, userGroup, persistentFlag)
    if not retVal['OK']:
      return retVal
    credDict = self.getRemoteCredentials()
    self.__proxyDB.logAction("set persistency to %s" % bool(persistentFlag),
                             credDict['username'], credDict['group'], user, userGroup)
    return S_OK()

  types_deleteProxyBundle = [(list, tuple)]

  def export_deleteProxyBundle(self, idList):
    """ Delete a list of id's

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

        :param basestring userDN: user DN
        :param basestring userGroup: DIRAC group

        :return: S_OK()/S_ERROR()
    """
    result = Registry.getUsernameForDN(userDN)
    if not result['OK']:
      return result
    username = result['Value']

    credDict = self.getRemoteCredentials()
    if Properties.PROXY_MANAGEMENT not in credDict['properties']:
      if username != credDict['username']:
        return S_ERROR("You aren't allowed!")
    retVal = self.__proxyDB.deleteProxy(userDN, userGroup)
    if not retVal['OK']:
      return retVal
    self.__proxyDB.logAction("delete proxy", credDict['username'], credDict['group'], username, userGroup)
    return S_OK()

  types_getContents = [dict, (list, tuple), six.integer_types, six.integer_types]

  def export_getContents(self, selDict, sortDict, start, limit):
    """ Retrieve the contents of the DB

        :param dict selDict: selection fields
        :param list,tuple sortDict: sorting fields
        :param int,long start: search limit start
        :param int,long start: search limit amount

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
        :param int,long start: search limit start
        :param int,long start: search limit amount

        :return: S_OK(dict)/S_ERROR() -- dict contain fields, record list, total records
    """
    return self.__proxyDB.getLogsContent(selDict, sortDict, start, limit)

  types_generateToken = [basestring, basestring, six.integer_types]

  def export_generateToken(self, requesterUsername, requesterGroup, tokenUses):
    """ Generate tokens for proxy retrieval

        :param basestring requesterUsername: user name
        :param basestring requesterGroup: DIRAC group
        :param int,long tokenUses: number of uses

        :return: S_OK(tuple)/S_ERROR() -- tuple contain token, number uses
    """
    # WARN: Next block for compatability
    if not requesterUsername.find("/"):  # Is it DN?
      result = Registry.getUsernameForDN(requesterUsername)
      if not result['OK']:
        return result
      requesterUsername = result['Value']

    credDict = self.getRemoteCredentials()
    self.__proxyDB.logAction("generate tokens", credDict['username'], credDict['group'], requesterUsername, requesterGroup)
    return self.__proxyDB.generateToken(requesterUsername, requesterGroup, numUses=tokenUses)

  types_getVOMSProxyWithToken = [basestring, basestring, basestring, six.integer_types, [basestring, type(None)]]

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
