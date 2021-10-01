""" Auth class is a front-end to the Auth Database
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import jwt
import time
import pprint

from sqlalchemy import Column, Integer, Text, String
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.ext.declarative import declarative_base

from authlib.integrations.sqla_oauth2 import OAuth2TokenMixin

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.SQLAlchemyDB import SQLAlchemyDB
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import OAuth2Token

__RCSID__ = "$Id$"


Model = declarative_base()


class Token(Model, OAuth2TokenMixin):
    __tablename__ = "Token"
    __table_args__ = {"mysql_engine": "InnoDB", "mysql_charset": "utf8"}
    # access_token too large for varchar(255)
    # 767 bytes is the stated prefix limitation for InnoDB tables in MySQL version 5.6
    # https://stackoverflow.com/questions/1827063/mysql-error-key-specification-without-a-key-length
    id = Column(Integer, autoincrement=True, primary_key=True)
    kid = Column(String(255))
    user_id = Column(String(255))
    provider = Column(String(255))
    expires_at = Column(Integer, nullable=False, default=0)
    access_token = Column(Text, nullable=False)
    refresh_token = Column(Text, nullable=False)
    rt_expires_at = Column(Integer, nullable=False, default=0)


class TokenDB(SQLAlchemyDB):
    """TokenDB class is a front-end to the OAuth Database"""

    def __init__(self):
        """Constructor"""
        super(TokenDB, self).__init__()
        self._initializeConnection("Framework/TokenDB")
        result = self.__initializeDB()
        if not result["OK"]:
            raise Exception("Can't create tables: %s" % result["Message"])
        self.session = scoped_session(self.sessionMaker_o)

    def __initializeDB(self):
        """Create the tables"""
        tablesInDB = self.inspector.get_table_names()

        # Token
        if "Token" not in tablesInDB:
            try:
                Token.__table__.create(self.engine)  # pylint: disable=no-member
            except Exception as e:
                return S_ERROR(e)

        return S_OK()

    def getTokenForUserProvider(self, userID, provider):
        """Get token for user ID and provider name

        :param str userID: user ID
        :param str provider: provider

        :return: S_OK(dict)/S_ERROR()
        """
        session = self.session()
        try:
            token = (
                session.query(Token)
                .filter(Token.rt_expires_at > time.time())
                .filter(Token.user_id == userID)
                .filter(Token.provider == provider)
                .first()
            )
        except Exception as e:
            return self.__result(session, S_ERROR(str(e)))
        return self.__result(session, S_OK(OAuth2Token(self.__rowToDict(token)) if token else None))

    def updateToken(self, token, userID, provider, rt_expired_in):
        """Update tokens

        :param dict token: token info
        :param str userID: user ID
        :param str provider: provider
        :param int rt_expired_in: refresh token lifetime

        :return: S_OK(list)/S_ERROR()
        """
        token["user_id"] = userID
        token["provider"] = provider
        if not token.get("rt_expires_at"):
            try:
                token["rt_expires_at"] = int(
                    jwt.decode(token["refresh_token"], options=dict(verify_signature=False, verify_aud=False))["exp"]
                )
            except Exception as e:
                self.log.debug("Cannot get refresh token expires time: %s" % repr(e))

        token["rt_expires_at"] = int(token.get("rt_expires_at", rt_expired_in + int(time.time())))
        if token["rt_expires_at"] < time.time():
            return S_ERROR("Cannot store expired refresh token.")

        attrts = dict((k, v) for k, v in dict(token).items() if k in list(Token.__dict__.keys()))
        self.log.debug("Store token:", pprint.pformat(attrts))
        session = self.session()
        try:
            session.query(Token).filter(Token.expires_at < time.time()).delete()
            oldTokens = session.query(Token).filter(Token.user_id == userID).filter(Token.provider == provider).all()
            session.add(Token(**attrts))
            session.query(Token).filter(Token.user_id == userID).filter(Token.provider == provider).filter(
                Token.access_token != token["access_token"]
            ).delete()
        except Exception as e:
            self.log.exception(e)
            return self.__result(session, S_ERROR("Could not add Token: %s" % repr(e)))
        self.log.info("Token successfully added for %s user, %s provider" % (token["user_id"], token["provider"]))
        return self.__result(session, S_OK([self.__rowToDict(t) for t in oldTokens] if oldTokens else []))

    def removeToken(self, access_token=None, refresh_token=None, user_id=None):
        """Remove token

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
            elif user_id:
                session.query(Token).filter(Token.user_id == user_id).delete()
        except Exception as e:
            return self.__result(session, S_ERROR(str(e)))
        return self.__result(session, S_OK("Token successfully removed"))

    def getTokensByUserID(self, userID):
        session = self.session()
        try:
            tokens = session.query(Token).filter(Token.user_id == userID).all()
        except NoResultFound:
            return self.__result(session, S_OK([]))
        except Exception as e:
            return self.__result(session, S_ERROR(str(e)))
        return self.__result(session, S_OK([OAuth2Token(self.__rowToDict(t)) for t in tokens]))

    def __result(self, session, result=None):
        try:
            if not result["OK"]:
                session.rollback()
            else:
                session.commit()
        except Exception as e:
            session.rollback()
            result = S_ERROR("Could not commit: %s" % (e))
        session.close()
        return result

    def __rowToDict(self, row):
        """Convert sqlalchemy row to dictionary

        :param object row: sqlalchemy row

        :return: dict
        """
        return {c.name: str(getattr(row, c.name)) for c in row.__table__.columns} if row else {}
