""" This class is located between the client and server part and designed to cache user information requested
    from the server part.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime
import six

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import ThreadSafe, DIRACSingleton
from DIRAC.Core.Utilities.DictCache import DictCache

__RCSID__ = "$Id$"

gUsersSync = ThreadSafe.Synchronizer()
gVOMSUsersSync = ThreadSafe.Synchronizer()


@six.add_metaclass(DIRACSingleton.DIRACSingleton)
class ProxyManagerData(object):
  """ Proxy manager client
  """
  # # __usersCache cache, with following:
  # #   Key: (userDN, group)
  # #   Value: dict
  # #       {
  # #         'DN': <certificate DN>,
  # #         'user': <user name>,
  # #         'groups': [<list of groups>], <-- TODOAL: current group
  # #         'expirationtime': <date time>,
  # #         'provider': <proxy provider>
  # #       }

  # # __VOMSesUsersCache cache, with next structure:
  # #   Key: VOMS VO name
  # #   Value: S_OK(dict)/S_ERROR() -- request VOMS information result that contain
  # #     dictionary with following:
  # #       { <user DN>: {
  # #           Suspended: bool,
  # #           VOMSRoles: [<all roles>],
  # #           ActiveRoles: [<active roles>],
  # #           SuspendedRoles: [<suspended roles>]
  # #         }
  # #       }

  def __init__(self):
    self.rpc = None
    self.__usersCache = DictCache()
    self.__VOMSesUsersCache = DictCache()

  @gUsersSync
  @gVOMSUsersSync
  def clearCaches(self):
    """ Clear caches
    """
    self.__usersCache.purgeAll()
    self.__VOMSesUsersCache.purgeAll()

  @gUsersSync
  def __getUsersCache(self, mask=None, time=None):
    """ Get cache information

        :param str mask: user ID
        :param int time: lifetime

        :return: dict
    """
    if mask:
      return self.__usersCache.get(mask, time) or {}
    return self.__usersCache.getDict()

  @gUsersSync
  def __addUsersCache(self, data, time=3600 * 24):
    """ Add cache information

        :param dict data: ID information data
        :param int time: lifetime
    """
    for oid, info in data.items():
      self.__usersCache.add(oid, time, value=info)

  def __getSecondsLeftToExpiration(self, expiration, utc=True):
    """ Get time left to expiration in a seconds

        :param datetime expiration:
        :param bool utc: time in utc

        :return: datetime
    """
    if utc:
      td = expiration - datetime.datetime.utcnow()
    else:
      td = expiration - datetime.datetime.now()
    return td.days * 86400 + td.seconds

  def __getRPC(self):
    """ Get RPC
    """
    if not self.rpc:
      from DIRAC.Core.Base.Client import Client
      self.rpc = Client()._getRPC(url="Framework/ProxyManager", timeout=120)
    return self.rpc

  def __refreshUserCache(self, validSeconds=0):
    """ Refresh user cache

        :param int validSeconds: required seconds the proxy is valid for

        :return: S_OK()/S_ERROR()
    """
    retVal = self.__getRPC().getRegisteredUsers(validSeconds)
    if not retVal['OK']:
      return retVal
    # Update the cache
    resDict = {}
    for record in retVal['Value']:
      for group in record['groups']:
        cacheKey = (record['DN'], group)
        resDict[cacheKey] = record
        self.__addUsersCache({cacheKey: record}, self.__getSecondsLeftToExpiration(record['expirationtime']))
    return S_OK(resDict)

  @gVOMSUsersSync
  def __getVOMSUsersDict(self):
    """ Get users dictionary from cache

        :return: dict
    """
    return self.__VOMSesUsersCache.getDict()

  @gVOMSUsersSync
  def __setVOMSUsersDict(self, usersDict):
    """ Set dictionary to cache

        :param dict usersDict: dictionary with VOMS users
    """
    for vo, userInfo in usersDict.items():
      self.__VOMSesUsersCache.add(vo, 3600 * 24, value=userInfo)
    self.__VOMSesUsersCache.add('Fresh', 3600 * 12, value=True)

  def __refreshVOMSesCache(self):
    """ Get fresh info from service about VOMSes

        :return: S_OK()/S_ERROR()
    """
    result = self.__getRPC().getVOMSesUsers()
    if result['OK']:
      self.__setVOMSUsersDict(result['Value'])
    return result

  def getActualVOMSesDNs(self, voList=None, dnList=None):
    """ Return actual/not suspended DNs from VOMSes

        :param list voList: VOs to get
        :param list dnList: DNs to get

        :return: S_OK(dict)/S_ERROR()
    """
    vomsUsers = self.__getVOMSUsersDict()
    if not vomsUsers.get('Fresh'):
      result = self.__refreshVOMSesCache()
      if not result['OK']:
        return result
      vomsUsers = result['Value']
    vomsUsers.pop('Fresh', None)
    res = {}
    if not vomsUsers:
      # use simulation here for tests
      return S_ERROR('VOMSes has not been updated.')
    for vo, voInfo in vomsUsers.items():
      if voList and vo not in voList:
        continue
      if not voInfo['OK']:
        res[vo] = voInfo
        continue
      res[vo] = S_OK({})
      for dn, data in voInfo['Value'].items():
        if dnList and dn not in dnList:
          continue
        if dn not in res[vo]['Value']:
          res[vo]['Value'][dn] = {'Suspended': data['suspended'],
                                  'VOMSRoles': [],
                                  'ActiveRoles': [],
                                  'SuspendedRoles': []}
        res[vo]['Value'][dn]['VOMSRoles'] = list(set(res[vo]['Value'][dn]['VOMSRoles'] + data['Roles']))
        if data['certSuspended'] or data['suspended']:
          res[vo]['Value'][dn]['SuspendedRoles'] = list(set(res[vo]['Value'][dn]['SuspendedRoles'] + data['Roles']))
        else:
          res[vo]['Value'][dn]['ActiveRoles'] = list(set(res[vo]['Value'][dn]['ActiveRoles'] + data['Roles']))
    return S_OK(res)

  def userHasProxy(self, userDN, group, validSeconds=0):
    """ Check if a user-group has a proxy in the proxy management
        Updates internal cache if needed to minimize queries to the service

        :param str userDN: user DN
        :param str group: user group
        :param int validSeconds: proxy valid time in a seconds

        :return: S_OK(bool)/S_ERROR()
    """
    cacheKey = (userDN, group)
    if self.__getUsersCache(cacheKey, validSeconds):
      return S_OK(True)
    # Get list of users from the DB with proxys at least 300 seconds
    gLogger.verbose("Updating list of users in proxy management")
    result = self.__refreshUserCache(validSeconds)
    if result.get('Value', {}).get(cacheKey):
      return S_OK(bool(result['Value'].get(cacheKey)))
    return result


gProxyManagerData = ProxyManagerData()
