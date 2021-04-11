""" This class is located between the client and server part and designed to cache user information requested
    from the server part.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
from pprint import pprint

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import ThreadSafe, DIRACSingleton
from DIRAC.Core.Utilities.DictCache import DictCache

__RCSID__ = "$Id$"


gCacheProfiles = ThreadSafe.Synchronizer()


@six.add_metaclass(DIRACSingleton.DIRACSingleton)
class AuthManagerData(object):
  """ Authentication manager
  """
  __cacheIdPToIDs = DictCache()
  # # {
  # #   <IdP1>: [ <IDs> ],
  # #   <IdP2>: ...
  # # }

  __cacheProfiles = DictCache()
  # # {
  # #   <ID1>: {
  # #     DNs: {
  # #       <DN1>: {
  # #         ProxyProvider: [ <proxy providers> ],
  # #         VOMSRoles: [ <VOMSRoles> ],
  # #         ...
  # #       },
  # #       <DN2>: { ... },
  # #     }
  # #   },
  # #   <ID2>: { ... }
  # # }

  __service = DictCache()
  # # {
  # #   crash: bool
  # # }

  def __init__(self):
    self.rpc = None

  @gCacheProfiles
  def getProfiles(self, userID=None):
    """ Get cache information

        :param str userID: user ID

        :return: dict
    """
    if userID:
      return self.__cacheProfiles.get(userID) or {}
    return self.__cacheProfiles.getDict()

  @gCacheProfiles
  def updateProfiles(self, userID, data, time=3600 * 24):
    """ Add cache information

        :param str userID: user ID
        :param dict data: ID information data
        :param int time: lifetime
    """
    profileDict = self.__cacheProfiles.get(userID) or {}
    print('================== CLI DATA updateProfiles ==================')
    print('User ID: %s' % userID)
    pprint(profileDict)
    pprint(data)
    for k, v in data.items():
      if v is not None:
        profileDict[k] = v
    self.__cacheProfiles.add(userID, time, value=profileDict)
    ids = self.__cacheIdPToIDs.get(profileDict['Provider'])
    if isinstance(ids, list) and userID not in ids:
      self.__cacheIdPToIDs.add(profileDict['Provider'], time, userID)

  def __getRPC(self):
    """ Get RPC
    """
    if not self.rpc:
      from DIRAC.Core.Base.Client import Client
      self.rpc = Client()._getRPC(url="Framework/AuthManager", timeout=10)
    return self.rpc

  def resfreshProfiles(self, userID=None):
    """ Refresh profiles cache from service

        :param str userID: userID to update

        :return: S_OK()/S_ERROR()
    """
    print('==== resfreshProfiles ====')
    servCrash = self.__service.get('crash')
    if servCrash and servCrash[1] > 2:
      return servCrash[0]
    result = self.__getRPC().getIdProfiles(userID)
    # If the AuthManager service is down client will ignore it 1 minute
    if result.get('Errno', 0) == 1112:
      crash = self.__service.get('crash')
      self.__service.add('crash', 60, value=(result, (crash[1] + 1) if crash else 1))
    if result['OK'] and result['Value']:
      for uid, data in result['Value'].items():
        if data:
          self.updateProfiles(uid, data)
    return result

  def getIDsForDN(self, dn, provider=None):
    """ Find ID for DN

        :param str dn: user DN

        :return: S_OK(list)
    """
    userIDs = []
    profile = self.getProfiles()
    for resfreshed in [0, 1]:
      for uid, data in profile.items():
        if dn not in data.get('DNs', []) or (provider and data['DNs'][dn]['ProxyProvider'] != provider):
          continue
        userIDs.append(uid)
      if userIDs or resfreshed:
        break
      result = self.resfreshProfiles()
      if not result['OK']:
        return result
      profile = result['Value']

    return S_OK(userIDs)

  def getDNsForID(self, uid):
    """ Find DNs for ID

        :param str uid: user ID

        :return: S_OK(list)/S_ERROR()
    """
    print('==== getDNsForID ====')
    profile = self.getProfiles(userID=uid)
    if not profile:
      result = self.resfreshProfiles(userID=uid)
      if not result['OK']:
        return result
      profile = result['Value'].get(uid, {})
    pprint(profile)
    print('=====================')
    return S_OK(profile.get('DNs', []))

  def getDNOptionForID(self, uid, dn, option):
    """ Find option for DN

        :param str uid: user ID
        :param str dn: user DN
        :param str option: option to find

        :return: S_OK()/S_ERROR()
    """
    profile = self.getProfiles(userID=uid)
    if not profile:
      result = self.resfreshProfiles(userID=uid)
      if not result['OK']:
        return result
      profile = result['Value'].get(uid, {})

    if dn in profile.get('DNs', []):
      return S_OK(profile['DNs'][dn].get(option))
    return S_OK(None)


gAuthManagerData = AuthManagerData()
