""" Auth class is a front-end to the Auth Database
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
from time import time
from pprint import pprint
from authlib.oauth2.rfc6749.wrappers import OAuth2Token
from authlib.integrations.sqla_oauth2 import OAuth2ClientMixin, OAuth2TokenMixin
from sqlalchemy import Column, Integer, Text, BigInteger, String
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.types import TypeDecorator, VARCHAR
from sqlalchemy.ext.mutable import Mutable
from sqlalchemy.ext.declarative import declarative_base

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Base.SQLAlchemyDB import SQLAlchemyDB
from DIRAC.ConfigurationSystem.Client.Utilities import getAuthClients

__RCSID__ = "$Id$"


Model = declarative_base()


class Token(Model, OAuth2TokenMixin):
  __tablename__ = 'Tokens'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}
  # access_token too large for varchar(255)
  # 767 bytes is the stated prefix limitation for InnoDB tables in MySQL version 5.6
  # https://stackoverflow.com/questions/1827063/mysql-error-key-specification-without-a-key-length
  access_token = Column(Text, nullable=False)
  # client_id too large
  client_id = Column(String(255))
  provider = Column(Text)
  user_id = Column(String(255), nullable=False, unique=True, primary_key=True)
  expires_at = Column(Integer, nullable=False, default=0)
  id_token = Column(Text, nullable=False)


class AuthSession(Model):
  __tablename__ = 'Sessions'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}
  id = Column(String(255), unique=True, primary_key=True, nullable=False)
  state = Column(String(255))
  uri = Column(String(255))
  client_id = Column(String(255))
  user_id = Column(String(255))
  username = Column(String(255))
  expires_at = Column(Integer, nullable=False, default=0)
  expires_in = Column(Integer, nullable=False, default=0)
  interval = Column(Integer, nullable=False, default=5)
  verification_uri = Column(String(255))
  verification_uri_complete = Column(String(255))
  user_code = Column(String(255))
  device_code = Column(String(255))
  scope = Column(String(255))


class AuthDB(SQLAlchemyDB):
  """ AuthDB class is a front-end to the OAuth Database
  """
  # TODO: provide logging instead of print
  def __init__(self):
    """ Constructor
    """
    super(AuthDB, self).__init__()
    self._initializeConnection('Framework/AuthDB')
    result = self.__initializeDB()
    if not result['OK']:
      raise Exception("Can't create tables: %s" % result['Message'])
    self.session = scoped_session(self.sessionMaker_o)

  def __initializeDB(self):
    """ Create the tables
    """
    tablesInDB = self.inspector.get_table_names()

    # Tokens
    if 'Tokens' not in tablesInDB:
      try:
        Token.__table__.create(self.engine)  # pylint: disable=no-member
      except Exception as e:
        return S_ERROR(e)

    # Sessions
    if 'Sessions' not in tablesInDB:
      try:
        AuthSession.__table__.create(self.engine)  # pylint: disable=no-member
      except Exception as e:
        return S_ERROR(e)

    return S_OK()

  def storeToken(self, metadata):
    """ Save token

        :param dict metadata: token info

        :return: S_OK(str)/S_ERROR()
    """
    attrts = {}
    print('========= STORE TOKEN')
    pprint(metadata)
    print('---------------------')
    for k, v in metadata.items():
      if k not in Token.__dict__.keys():
        self.log.warn('%s is not expected as token attribute.' % k)
      else:
        attrts[k] = v
    session = self.session()
    try:
      session.add(Token(**attrts))
    except Exception as e:
      return self.__result(session, S_ERROR('Could not add Token: %s' % e))
    return self.__result(session, S_OK('Token successfully added'))

  def updateToken(self, token, refreshToken):
    """ Update token

        :param dict token: token info
        :param str refreshToken: refresh token

        :return: S_OK(object)/S_ERROR()
    """
    self.removeToken(refresh_token=refreshToken)
    return self.storeToken(token)

  def removeToken(self, access_token=None, refresh_token=None):
    """ Remove token

        :param str access_token: access token
        :param str refresh_token: refresh token

        :return: S_OK(object)/S_ERROR()
    """
    session = self.session()
    try:
      if access_token:
        session.query(Token).filter(Token.access_token == access_token).delete()
      elif refresh_token:
        session.query(Token).filter(Token.refresh_token == refresh_token).delete()
    except Exception as e:
      return self.__result(session, S_ERROR(str(e)))
    return self.__result(session, S_OK('Token successfully removed'))

  def getTokenByRefreshToken(self, refresh_token):
    session = self.session()
    try:
      token = session.query(Token).filter(Token.refresh_token == refresh_token).first()
    except NoResultFound:
      return self.__result(session, S_ERROR("Token not found."))
    except Exception as e:
      return self.__result(session, S_ERROR(str(e)))
    return self.__result(session, S_OK(self.__rowToDict(token)))

  def getTokenByUserIDAndProvider(self, userID, provider):
    session = self.session()
    try:
      token = session.query(Token).filter(Token.user_id == userID, Token.provider == provider).first()
    except NoResultFound:
      return self.__result(session, S_ERROR("Token not found."))
    except Exception as e:
      return self.__result(session, S_ERROR(str(e)))
    return self.__result(session, S_OK(self.__rowToDict(token)))

  def getIdPTokens(self, IdP, userIDs=None):
    session = self.session()
    try:
      if userIDs:
        tokens = session.query(Token).filter(Token.provider == IdP).filter(Token.user_id.in_(set(userIDs))).all()
      else:
        tokens = session.query(Token).filter(Token.provider == IdP).all()
    except NoResultFound:
      return self.__result(session, S_ERROR("Tokens not found."))
    except Exception as e:
      return self.__result(session, S_ERROR(str(e)))
    return self.__result(session, S_OK([OAuth2Token(self.__rowToDict(t)) for t in tokens]))

  def _addSession(self, data):
    """ Add new session

        :param dict data: session metadata

        :return: S_OK(dict)/S_ERROR()
    """
    print('============ addSession ============')
    pprint(data)
    session = self.session()
    # newAuthSession = Session(**data)
    try:
      session.add(Session(**data))
    except Exception as e:
      return self.__result(session, S_ERROR('Could not add Session: %s' % e))
    return self.__result(session, S_OK())

  def addSession(self, data):
    result = self._addSession(data)
    if not result['OK']:
      result = self.updateSession(data)
    return result

  def updateSession(self, data):
    """ Update session

        :param dict data: session data with 'id' key

        :return: S_OK(object)/S_ERROR()
    """
    session = self.session()
    try:
      session.update(AuthSession(**data)).where(AuthSession.id == data['id'])
    except MultipleResultsFound:
      return self.__result(session, S_ERROR("%s is not unique." % sessionID))
    except Exception as e:
      return self.__result(session, S_ERROR(str(e)))
    return self.__result(session, S_OK())

  def removeSession(self, sessionID):
    """ Remove session

        :param str sessionID: session id

        :return: S_OK()/S_ERROR()
    """
    session = self.session()
    try:
      session.query(AuthSession).filter(AuthSession.id == sessionID).delete()
    except Exception as e:
      return self.__result(session, S_ERROR(str(e)))
    return self.__result(session, S_OK())

  def getSession(self, sessionID):
    """ Get client

        :param str sessionID: session id

        :return: S_OK(dict)/S_ERROR()
    """
    session = self.session()
    try:
      resData = session.query(AuthSession).filter(AuthSession.id == sessionID).first()
    except MultipleResultsFound:
      return self.__result(session, S_ERROR("%s is not unique ID." % sessionID))
    except NoResultFound:
      return self.__result(session, S_ERROR("%s session is expired." % sessionID))
    except Exception as e:
      return self.__result(session, S_ERROR(str(e)))
    return self.__result(session, S_OK(self.__rowToDict(resData)))
  
  def getSessionByUserCode(self, userCode):
    """ Get client

        :param str userCode: user code

        :return: S_OK(dict)/S_ERROR()
    """
    session = self.session()
    try:
      resData = session.query(AuthSession).filter(AuthSession.user_code == userCode).first()
    except MultipleResultsFound:
      return self.__result(session, S_ERROR("%s is not unique ID." % sessionID))
    except NoResultFound:
      return self.__result(session, S_ERROR("%s session is expired." % sessionID))
    except Exception as e:
      return self.__result(session, S_ERROR(str(e)))
    return self.__result(session, S_OK(self.__rowToDict(resData)))

  def __result(self, session, result=None):
    try:
      if not result['OK']:
        session.rollback()
      else:
        session.commit()
    except Exception as e:
      session.rollback()
      result = S_ERROR('Could not commit: %s' % (e))
    session.close()
    return result

  def __rowToDict(self, row):
    """ Convert sqlalchemy row to dictionary

        :param object row: sqlalchemy row

        :return: dict
    """
    return {c.name: str(getattr(row, c.name)) for c in row.__table__.columns} if row else {}
