from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from time import time
from pprint import pprint

from DIRAC import gLogger
from DIRAC.Core.Utilities import ThreadSafe
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import OAuth2Token

__RCSID__ = "$Id$"

gCacheSession = ThreadSafe.Synchronizer()


class Session(dict):
  """ A dict instance to represent a authentication session object.

      :param session:
      :type session: str or dict
  """

  def __init__(self, session, data=None, exp=300):
    if isinstance(session, Session):
      session = dict(session)

    data = data or {}
    if isinstance(session, dict):
      session.update(data)
      data = session
    else:
      data['id'] = session
    if not data.get('id'):
      raise KeyError('Missing "id" for a session.')
    if not data.get('expires_at'):
      data['expires_at'] = int(time()) + exp
    if not data.get('created'):
      data['created'] = int(time())
    super(Session, self).__init__(**data)
    self.id = data['id']
    self.created = self['created']

  @property
  def status(self):
    """ Session status

        :return: int
    """
    return self.get('Status', 'submited')

  @property
  def age(self):
    """ Session age

        :return: int
    """
    return int(time()) - self.created

  @property
  def token(self):
    """ Tokens

        :return: object
    """
    return self.get('token') and OAuth2Token(self['token'])

  def update(self, data=None, **kwargs):
    """ Update session

        :param dict data: dictionary with new values

        :return: object
    """
    kwargs.update(data or {})
    super(Session, self).update(kwargs)
    print('updated done')
    return self


class SessionManager(object):
  """ Authentication sessions cache manager """

  def __init__(self, database, addTime=300, maxAge=3600 * 12):
    """ Con'r

        :param int addTime: additional time added to session life
        :param int maxAge: max session age
    """
    # self.__sessions = DictCache()
    self.__db = database
    self.__addTime = addTime
    self.__maxAge = maxAge

  # @gCacheSession
  def addSession(self, session, exp=None, **kwargs):
    """ Add session to cache

        :param session: session
        :type session: str, dict or Session object
        :param int exp: expired time
    """
    # print('-- addSession')
    # pprint(session)
    # exp = min(exp or self.__addTime, self.__maxAge)
    # session = Session(session, data=kwargs, exp=exp)
    # pprint(session)

    # if session.age > self.__maxAge:
    #   return self.__sessions.delete(session.id)
    # print('ADD SESSION: %s' % session.id)
    return self.__db.addSession(dict(session))
    # return self.__sessions.add(session.id, exp, session)

  # @gCacheSession
  def getSession(self, session):
    """ Get session from cache

        :param session: session
        :type session: str, Session object

        :return: Session object
    """
    print('-- getSession')
    pprint(session)
    return self.__db.getSession(session)
    # return self.__sessions.get(session.id if isinstance(session, Session) else session)

  # # @gCacheSession
  # def getSessions(self):
  #   """ Get all sessions from cache

  #       :return: dict
  #   """
  #   return self.__sessions.getDict()

  # @gCacheSession
  def removeSession(self, session):
    """ Remove session from cache

        :param session: session
        :type session: str, Session object
    """
    print('-- removeSession')
    pprint(session)
    return self.__db.removeSession(session)
    # self.__sessions.delete(session.id if isinstance(session, Session) else session)

  def updateSession(self, session, exp=None, createIfNotExist=None, **kwargs):
    """ Update session in cache

        :param session: session
        :type session: str, Session object
        :param int exp: expiration time
    """
    print('-- updateSession')
    pprint(session)
    sessionID = session.id if isinstance(session, Session) else session
    session = self.getSession(sessionID)
    pprint(session)
    exp = exp or self.__addTime
    if session and session.age < self.__maxAge:
      if (session.age + exp) > self.__maxAge:
        exp = self.__maxAge - session.age
      if exp:
        print('UPDATE SESSION: %s' % session.id)
        self.addSession(session.update(kwargs), exp)
    elif createIfNotExist:
      print('UPDATE hard SESSION: %s' % sessionID)
      self.addSession(sessionID, exp, **kwargs)

  def getSessionByOption(self, key, value):
    """ Search session by the option

        :param str key: option name
        :param str value: option value

        :return: str, Session
    """
    if key and value:
      sessions = self.getSessions()
      for session, data in sessions.items():
        if data.get(key) == value:
          return session, data
    return None, None
