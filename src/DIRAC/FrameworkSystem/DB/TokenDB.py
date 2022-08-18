"""Token class is a front-end to the TokenDB Database.
Long-term user tokens are stored here, which can be used to obtain new tokens.
"""
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


Model = declarative_base()


class Token(Model, OAuth2TokenMixin):
    """This class describe token fields"""

    __tablename__ = "Token"
    __table_args__ = {"mysql_engine": "InnoDB", "mysql_charset": "utf8"}
    # access_token too large for varchar(255)
    # 767 bytes is the stated prefix limitation for InnoDB tables in MySQL version 5.6
    # https://stackoverflow.com/questions/1827063/mysql-error-key-specification-without-a-key-length
    id = Column(Integer, autoincrement=True, primary_key=True)  # Unique token ID
    kid = Column(String(255))  # Unique secret key ID for token encryption
    user_id = Column(String(255))  # User identificator that registred in an identity provider, token owner
    provider = Column(String(255))  # Provider name registred in DIRAC
    expires_at = Column(Integer, nullable=False, default=0)  # When the access token is expired
    access_token = Column(Text, nullable=False)
    refresh_token = Column(Text, nullable=False)
    rt_expires_at = Column(Integer, nullable=False, default=0)  # When the refresh token is expired


class TokenDB(SQLAlchemyDB):
    """TokenDB class is a front-end to the TokenDB Database"""

    def __init__(self, *args, **kwargs):
        """Constructor"""
        super().__init__(*args, **kwargs)
        self._initializeConnection("Framework/TokenDB")
        result = self.__initializeDB()
        if not result["OK"]:
            raise Exception("Can't create tables: %s" % result["Message"])
        self.session = scoped_session(self.sessionMaker_o)

    def __initializeDB(self):
        """Create the tables

        :return: S_OK()/S_ERROR()
        """
        tablesInDB = self.inspector.get_table_names()

        # Token
        if "Token" not in tablesInDB:
            try:
                Token.__table__.create(self.engine)  # pylint: disable=no-member
            except Exception as e:
                return S_ERROR(e)

        return S_OK()

    def getTokenForUserProvider(self, userID, provider):
        """Get token for user ID and identity provider name

        :param str userID: user ID
        :param str provider: provider name

        :return: S_OK(OAuth2Token)/S_ERROR() -- return an OAuth2Token object, which is also a dict
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

    def updateToken(self, token: dict, userID: str, provider: str, rt_expired_in: int):
        """Store or update an existing token in the database.
        Before saving, the token is checked for expiration.
        Also, the database cannot contain several user tokens signed by one provider,
        only one with the maximum possible permissions is enough.

        :param token: token information dictionary
        :param userID: user ID (token owner)
        :param provider: provider name that issued the token
        :param rt_expired_in: refresh token expiration time, will be applied if the rt_expires_at value is missing

        :return: S_OK(list)/S_ERROR() -- return old tokens that should be revoked.
        """
        if not token["refresh_token"]:
            return S_ERROR("Cannot store absent refresh token.")

        # Let's collect the necessary attributes of the token
        token["user_id"] = userID
        token["provider"] = provider
        # If the expiration time of the token refresh is not specified, we will try to determine it
        if not token.get("rt_expires_at"):
            try:
                # If the refresh token is encoded as JWT (https://datatracker.ietf.org/doc/html/rfc7519),
                # then we will be able to read it
                decodedDict = jwt.decode(token["refresh_token"], options=dict(verify_signature=False, verify_aud=False))
                if decodedDict.get("exp"):
                    token["rt_expires_at"] = decodedDict["exp"]
            except Exception as e:
                self.log.debug("Cannot get refresh token expiration time:")
                self.log.exception(e)
        # if the rt_expires_at value is missing, we will try to calculate it
        token["rt_expires_at"] = int(token.get("rt_expires_at", rt_expired_in + int(time.time())))
        # We ignore expired tokens
        if token["rt_expires_at"] < time.time():
            return S_ERROR("Cannot store expired refresh token.")

        attrts = {k: v for k, v in dict(token).items() if k in list(Token.__dict__.keys())}
        self.log.debug("Store token:", pprint.pformat(attrts))
        session = self.session()
        try:
            # Let's delete expired tokens
            session.query(Token).filter(Token.expires_at < time.time()).delete()
            # When we update existing tokens, the old tokens should be revoked
            oldTokens = session.query(Token).filter(Token.user_id == userID).filter(Token.provider == provider).all()
            session.add(Token(**attrts))
            session.query(Token).filter(Token.user_id == userID).filter(Token.provider == provider).filter(
                Token.access_token != token["access_token"]
            ).delete()
        except Exception as e:
            self.log.exception(e)
            return self.__result(session, S_ERROR("Could not add Token: %s" % repr(e)))
        self.log.info("Token successfully added for {} user, {} provider".format(token["user_id"], token["provider"]))
        return self.__result(session, S_OK([self.__rowToDict(t) for t in oldTokens] if oldTokens else []))

    def removeToken(self, access_token=None, refresh_token=None, user_id=None):
        """Remove token from DB

        :param str access_token: access token
        :param str refresh_token: refresh token

        :return: S_OK(str)/S_ERROR()
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
        """Return tokens for user ID

        :param str userID: user ID that return identity provider

        :return: S_OK(list)/S_ERROR() -- tokens as OAuth2Token objects
        """
        session = self.session()
        try:
            tokens = session.query(Token).filter(Token.user_id == userID).all()
        except NoResultFound:
            return self.__result(session, S_OK([]))
        except Exception as e:
            return self.__result(session, S_ERROR(str(e)))
        return self.__result(session, S_OK([OAuth2Token(self.__rowToDict(t)) for t in tokens]))

    def __result(self, session, result=None):
        """Helper method

        :param session: session instance
        :param result: DIRAC result

        :return: S_OK()/S_ERROR()
        """
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
