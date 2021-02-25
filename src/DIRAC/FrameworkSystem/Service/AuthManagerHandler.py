""" The OAuth service provides a toolkit to authenticate through an OIDC session.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import six
import time
import pprint
import threading
from authlib.jose import jwt  # TODO: need to add authlib to DIRACOS

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities import ThreadSafe
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getProviderInfo, getProvidersForInstance
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForID, getIDsForUsername, getEmailsForGroup
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
from DIRAC.ConfigurationSystem.Client.Utilities import getAuthAPI
from DIRAC.FrameworkSystem.private.authorization.utils.Sessions import Session

from DIRAC.FrameworkSystem.DB.AuthDB import AuthDB

# from DIRAC.FrameworkSystem.DB.AuthServerHandler import AuthServerHandler

__RCSID__ = "$Id$"


gCacheProfiles = ThreadSafe.Synchronizer()


class AuthManagerHandler(RequestHandler):
  """ Authentication manager
  """
  __cahceIdPIDs = DictCache()
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

  __db = None

  @classmethod
  @gCacheProfiles
  def __getProfiles(cls, userID=None):
    """ Get cache information

        :param str userID: user ID

        :return: dict
    """
    if userID:
      return {userID: cls.__cacheProfiles.get(userID) or {}}
    return cls.__cacheProfiles.getDict()

  @classmethod
  @gCacheProfiles
  def __addProfiles(cls, data, time=3600 * 24):
    """ Caching information

        :param dict data: ID information data
        :param int time: lifetime
    """
    if data:
      for oid, info in data.items():
        cls.__cacheProfiles.add(oid, time, value=info)

  @classmethod
  def __cleanAuthDB(cls):
    """ Check AuthDB for zombie sessions and clean

        :return: S_OK()/S_ERROR()
    """
    # cls.log.info("Kill zombie sessions")
    # result = cls.__db.getZombieSessions()
    # if not result['OK']:
    #   gLogger.error('Cannot clean zombies: %s' % result['Message'])
    #   return result
    # for idP, sessions in result['Value'].items():
    #   result = cls.__idps.getIdProvider(idP, sessionManager=cls.__db)
    #   if not result['OK']:
    #     for session in sessions:
    #       cls.log.error('%s session, with %s IdP, cannot log out:' % (sessions, idP), result['Message'])
    #       cls.__db.killSession(session)
    #     continue
    #   provObj = result['Value']
    #   for session in sessions:
    #     result = provObj.logOut(session)
    #     if not result['OK']:
    #       cls.log.error('%s session, with %s IdP, cannot log out:' % (session, idP), result['Message'])
    #     cls.__db.killSession(session)

    # cls.log.notice("Cleaning is done!")
    return S_OK()

  @classmethod
  def initializeHandler(cls, serviceInfo):
    """ Handler initialization
    """
    cls.__db = AuthDB()
    cls.__idps = IdProviderFactory()
    #gThreadScheduler.addPeriodicTask(3600, cls.__cleanAuthDB)
    #result = cls.__cleanAuthDB()
    return cls.__refreshProfiles() #if result['OK'] else result

  @classmethod
  def __refreshProfiles(cls):
    """ Refresh users profiles

        :return: S_OK()/S_ERROR()
    """
    def refreshIdP(idP):
      """ Process to get information from VOMS API

          :param str vo: VO name
      """
      result = cls.__idps.getIdProvider(idP, sessionManager=cls.__db)
      if result['OK']:
        provObj = result['Value']
        result = provObj.getIDsMetadata()
        if result['OK']:
          cls.__addProfiles(result['Value'])
      if not result['OK']:
        return result

    result = getProvidersForInstance('Id')
    if not result['OK']:
      return result
    for idP in result['Value']:
      processThread = threading.Thread(target=refreshIdP, args=[idP])
      processThread.start()

    return S_OK()

  def __checkAuth(self):
    """ Check authorization rules

        :return: S_OK(tuple)/S_ERROR() -- tuple contain username and IDs
    """
    credDict = self.getRemoteCredentials()
    if credDict['group'] == 'hosts':
      return S_OK((None, 'all'))

    user = credDict["username"]
    userIDs = getIDsForUsername(user)
    if not userIDs:
      return S_ERROR('No registred IDs for %s user.' % user)

    return S_OK((user, userIDs))

  # types_updateProfile = []
  # auth_updateProfile = ["authenticated", "TrustedHost"]

  # def export_updateProfile(self, userID=None):
  #   """ Return fresh info from identity providers about users with actual sessions

  #       :params: str userID: user ID

  #       :return: S_OK(dict)/S_ERROR()
  #   """
  #   result = self.__checkAuth()
  #   if not result['OK']:
  #     return result
  #   user, ids = result["Value"]

  #   # For host
  #   if ids == 'all':
  #     return S_OK(self.__getProfiles(userID=userID))

  #   # For user
  #   if userID:
  #     if userID not in ids:
  #       return S_ERROR('%s user not have access to %s ID information.' % (user, userID))
  #     return S_OK(self.__getProfiles(userID=userID))

  #   data = {}
  #   for uid in ids:
  #     idDict = self.__getProfiles(userID=uid)
  #     if idDict:
  #       data[uid] = idDict

  #   return S_OK(data)

  types_getIdProfiles = []
  auth_getIdProfiles = ["authenticated", "TrustedHost"]

  def export_getIdProfiles(self, userID=None):
    """ Return fresh info from identity providers about users with actual sessions

        :params: str userID: user ID

        :return: S_OK(dict)/S_ERROR()
    """
    result = self.__checkAuth()
    if not result['OK']:
      return result
    user, ids = result["Value"]

    print('================== export_getIdProfiles ==================')
    print('CREDS:')
    pprint.pprint(self.getRemoteCredentials())
    print('userID: %s' % userID)
    p = self.__getProfiles()
    pprint.pprint(p)

    # For host
    if ids == 'all':
      print('all')
      pprint.pprint(self.__getProfiles(userID=userID))
      return S_OK(self.__getProfiles(userID=userID))

    # For user
    if userID:
      if userID not in ids:
        return S_ERROR('%s user not have access to %s ID information.' % (user, userID))
      print('For user')
      pprint.pprint(self.__getProfiles(userID=userID))
      return S_OK(self.__getProfiles(userID=userID))

    data = {}
    for uid in ids:
      idDict = self.__getProfiles(userID=uid)
      if idDict.get(uid):
        data.update(idDict)
    print('Else')
    pprint.pprint(data)
    return S_OK(data)

  types_parseAuthResponse = [six.string_types, dict, dict]  #, six.string_types, dict]

  def export_parseAuthResponse(self, providerName, response, sessionDict):  #, username, userProfile):
    """ Fill session by user profile, tokens, comment, OIDC authorize status, etc.
        Prepare dict with user parameters, if DN is absent there try to get it.
        Create new or modify existing DIRAC user and store the session

        :param str providerName: provider name
        :param dict response: authorization response
        :param dict sessionDict: session number

        :return: S_OK(tuple)/S_ERROR() -- tuple contain username, profile and session
    """
    print('==== export_parseAuthResponse')
    # Parse response
    result = self.__idps.getIdProvider(providerName, sessionManager=self.__db)
    if result['OK']:
      result = result['Value'].parseAuthResponse(response, Session(sessionDict))
    if not result['OK']:
      return result
    # FINISHING with IdP auth result
    username, userProfile, session = result['Value']

    # Is ID registred?
    result = getUsernameForID(userProfile['ID'])
    if not result['OK']:
      comment = '%s ID is not registred in the DIRAC.' % userProfile['ID']
      result = self.__registerNewUser(providerName, username, userProfile)
      if result['OK']:
        comment += ' Administrators have been notified about you.'
      else:
        comment += ' Please, contact the DIRAC administrators.'
      return S_ERROR(comment)
    self.__addProfiles({userProfile['ID']: userProfile})
    
    print('================== export_parseAuthResponse ==================')
    print('userID: %s' % userProfile['ID'])
    print('profile: %s' % userProfile)
    pprint.pprint(self.__getProfiles())
    print('==================  ==================')
    return S_OK((result['Value'], userProfile, dict(session)))

  def __registerNewUser(self, provider, parseDict):
    """ Register new user

        :param str provider: provider
        :param dict parseDict: user information dictionary

        :return: S_OK()/S_ERROR()
    """
    from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

    mail = {}
    mail['subject'] = "[SessionManager] User %s to be added." % parseDict['username']
    mail['body'] = 'User %s was authenticated by ' % parseDict['UserOptions']['FullName']
    mail['body'] += provider
    mail['body'] += "\n\nAuto updating of the user database is not allowed."
    mail['body'] += " New user %s to be added," % parseDict['username']
    mail['body'] += "with the following information:\n"
    mail['body'] += "\nUser name: %s\n" % parseDict['username']
    mail['body'] += "\nUser profile:\n%s" % pprint.pformat(parseDict['UserOptions'])
    mail['body'] += "\n\n------"
    mail['body'] += "\n This is a notification from the DIRAC AuthManager service, please do not reply.\n"
    result = S_OK()
    for addresses in getEmailsForGroup('dirac_admin'):
      result = NotificationClient().sendMail(addresses, mail['subject'], mail['body'], localAttempt=False)
      if not result['OK']:
        self.log.error(result['Message'])
    if result['OK']:
      self.log.info(result['Value'], "administrators have been notified of a new user.")
    return result

  types_createClient = [dict]
  auth_createClient = []#"authenticated", "TrustedHost"]

  def export_createClient(self, kwargs):
    """ Generates a state string to be used in authorizations

        :param str provider: provider
        :param str session: session number

        :return: S_OK(str)/S_ERROR()
    """
    return self.__db.addClient(**kwargs)

  # types_getClientByID = [six.string_types]
  # auth_getClientByID = []  # "authenticated", "TrustedHost"]

  # def export_getClientByID(self, clientID, metadata):
  #   """ Generates a state string to be used in authorizations

  #       :param str provider: provider
  #       :param str session: session number

  #       :return: S_OK(str)/S_ERROR()
  #   """
  #   return self.__db.getClientByID(clientID, **metadata)
  
  types_storeToken = [dict]
  auth_storeToken = ["authenticated"]
  def export_storeToken(self, kwargs):
    """ Generates a state string to be used in authorizations

        :param str provider: provider
        :param str session: session number

        :return: S_OK(str)/S_ERROR()
    """
    return self.__db.storeToken(kwargs)
  
  types_updateToken = []
  auth_updateToken = ["authenticated"]
  def export_updateToken(self, token, refreshToken):
    """ Generates a state string to be used in authorizations

        :param str provider: provider
        :param str session: session number

        :return: S_OK(str)/S_ERROR()
    """
    result = self.__db.updateToken(token, refreshToken)
    return S_OK(dict(result['Value'])) if result['OK'] else result

  types_getTokenByUserIDAndProvider = [six.string_types, six.string_types]
  auth_getTokenByUserIDAndProvider = ["authenticated"]
  def export_getTokenByUserIDAndProvider(self, uid, provider):
    """ Generates a state string to be used in authorizations

        :param str provider: provider
        :param str session: session number

        :return: S_OK(str)/S_ERROR()
    """
    # if provider:
    #   result = self.__idps.getIdProvider(provider, sessionManager=cls.__db)
    #   if result['OK']:
    #     provObj = result['Value']
    #     result = provObj.getTokenByUserID(uid)
    # else:
    result = self.__db.getTokenByUserIDAndProvider(uid, provider)
    return result
