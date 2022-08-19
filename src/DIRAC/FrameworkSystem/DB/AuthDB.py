""" AuthDB class is a front-end to the AuthDB MySQL Database (via SQLAlchemy)
"""
import json
import time
import pprint

from sqlalchemy import Column, Integer, Text, String
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.ext.declarative import declarative_base

from authlib.jose import KeySet, JsonWebKey
from authlib.common.security import generate_token

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.SQLAlchemyDB import SQLAlchemyDB
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import OAuth2Token


Model = declarative_base()


class RefreshToken(Model):
    __tablename__ = "RefreshToken"
    __table_args__ = {"mysql_engine": "InnoDB", "mysql_charset": "utf8"}
    jti = Column(String(255), nullable=False, primary_key=True)
    issued_at = Column(Integer, nullable=False, default=0)
    access_token = Column(Text, nullable=False)
    refresh_token = Column(Text)


class JWK(Model):
    __tablename__ = "JWK"
    __table_args__ = {"mysql_engine": "InnoDB", "mysql_charset": "utf8"}
    kid = Column(String(255), unique=True, primary_key=True, nullable=False)
    key = Column(Text, nullable=False)
    expires_at = Column(Integer, nullable=False, default=0)


class AuthSession(Model):
    __tablename__ = "AuthSession"
    __table_args__ = {"mysql_engine": "InnoDB", "mysql_charset": "utf8"}
    id = Column(String(255), unique=True, primary_key=True, nullable=False)
    uri = Column(String(255))
    state = Column(String(255))
    scope = Column(String(255))
    user_id = Column(String(255))
    username = Column(String(255))
    client_id = Column(String(255))
    user_code = Column(String(255))
    device_code = Column(String(255))
    interval = Column(Integer, nullable=False, default=5)
    expires_at = Column(Integer, nullable=False, default=0)
    expires_in = Column(Integer, nullable=False, default=0)
    verification_uri = Column(String(255))
    verification_uri_complete = Column(String(255))


class AuthDB(SQLAlchemyDB):
    """AuthDB class is a front-end to the OAuth Database"""

    def __init__(self):
        """Constructor"""
        super().__init__()
        self._initializeConnection("Framework/AuthDB")
        result = self.__initializeDB()
        if not result["OK"]:
            raise Exception("Can't create tables: %s" % result["Message"])
        self.session = scoped_session(self.sessionMaker_o)

    def __initializeDB(self):
        """Create the tables"""
        tablesInDB = self.inspector.get_table_names()

        # RefreshToken
        if "RefreshToken" not in tablesInDB:
            try:
                RefreshToken.__table__.create(self.engine)  # pylint: disable=no-member
            except Exception as e:
                return S_ERROR(e)

        # JWK
        if "JWK" not in tablesInDB:
            try:
                JWK.__table__.create(self.engine)  # pylint: disable=no-member
            except Exception as e:
                return S_ERROR(e)

        # AuthSession
        if "AuthSession" not in tablesInDB:
            try:
                AuthSession.__table__.create(self.engine)  # pylint: disable=no-member
            except Exception as e:
                return S_ERROR(e)

        return S_OK()

    def storeRefreshToken(self, token, tokenID=None):
        """Store refresh token

        :param dict token: tokens as dict
        :param str tokenID: token ID

        :return: S_OK(dict)/S_ERROR()
        """
        iat = int(time.time())
        jti = tokenID or generate_token(10)
        self.log.debug("Store %s token:\n" % jti, pprint.pformat(token))

        session = self.session()
        try:
            session.add(
                RefreshToken(
                    jti=jti, issued_at=iat, access_token=token["access_token"], refresh_token=token.get("refresh_token")
                )
            )
        except Exception as e:
            return self.__result(session, S_ERROR("Could not add refresh token: %s" % repr(e)))

        self.log.info("Token with %s ID successfully added:\n" % jti, pprint.pformat(token))
        return S_OK(dict(jti=jti, iat=iat))

    def revokeRefreshToken(self, tokenID):
        """Revoke refresh token

        :param str tokenID: refresh token ID

        :return: S_OK()/S_ERROR()
        """
        session = self.session()
        try:
            session.query(RefreshToken).filter(RefreshToken.jti == tokenID).delete()
        except Exception as e:
            return self.__result(session, S_ERROR(str(e)))
        return S_OK()

    def getCredentialByRefreshToken(self, tokenID):
        """Get refresh token credential

        :param str tokenID: refresh token ID

        :return: S_OK(dict)/S_ERROR()
        """
        session = self.session()
        try:
            token = session.query(RefreshToken).filter(RefreshToken.jti == tokenID).first()
            session.query(RefreshToken).filter(RefreshToken.jti == tokenID).delete()
        except Exception as e:
            return self.__result(session, S_ERROR(str(e)))
        return self.__result(session, S_OK(OAuth2Token(self.__rowToDict(token)) if token else None))

    def generateRSAKeys(self):
        """Generate an RSA keypair with an exponent of 65537 in PEM format

        :return: S_OK/S_ERROR
        """
        key = JsonWebKey.generate_key("RSA", 1024, is_private=True)
        # as_dict has no arguments for authlib < 1.0.0
        # for authlib >= 1.0.0
        keyDict = dict(
            key=json.dumps(key.as_dict(True)), kid=key.thumbprint(), expires_at=time.time() + (30 * 24 * 3600)
        )
        session = self.session()
        try:
            session.add(JWK(**keyDict))
        except Exception as e:
            return self.__result(session, S_ERROR("Could not generate keys: %s" % e))
        return self.__result(session, S_OK(keyDict))

    def getKeySet(self):
        """Get key set

        :return: S_OK(obj)/S_ERROR()
        """
        result = self.getActiveKeys()
        if result["OK"] and not result["Value"]:
            result = self.generateRSAKeys()
            if result["OK"]:
                result["Value"] = [result["Value"]]
        if not result["OK"]:
            return result
        return S_OK(KeySet([JsonWebKey.import_key(json.loads(key["key"])) for key in result["Value"]]))

    def getPrivateKey(self, kid=None):
        """Get private key

        :param str kid: key ID

        :return: S_OK(obj)/S_ERROR()
        """
        result = self.getActiveKeys(kid)
        if not result["OK"]:
            return result
        jwks = result["Value"]
        if kid:
            strkey = jwks[0]["key"]
            return S_OK(JsonWebKey.import_key(json.loads(jwks[0]["key"])))
        newer = {}
        for jwk in jwks:
            if int(jwk["expires_at"]) > int(newer.get("expires_at", time.time() + (24 * 3600))):
                newer = jwk
        if not newer.get("key"):
            result = self.generateRSAKeys()
            if not result["OK"]:
                return result
            newer = result["Value"]
        return S_OK(JsonWebKey.import_key(json.loads(newer["key"])))

    def getActiveKeys(self, kid=None):
        """Get active keys

        :param str kid: key ID

        :return: S_OK(list)/S_ERROR()
        """
        session = self.session()
        try:
            # Remove all expired jwks
            session.query(JWK).filter(JWK.expires_at < time.time()).delete()
            jwks = session.query(JWK).filter(JWK.expires_at > time.time()).all()
            if kid:
                jwks = [jwk for jwk in jwks if jwk.kid == kid]
        except NoResultFound:
            return self.__result(session, S_OK([]))
        except Exception as e:
            return self.__result(session, S_ERROR(str(e)))
        return self.__result(session, S_OK([self.__rowToDict(jwk) for jwk in jwks]))

    def removeKeys(self):
        """Get active keys

        :return: S_OK(list)/S_ERROR()
        """
        session = self.session()
        try:
            session.query(JWK).delete()
        except Exception as e:
            return self.__result(session, S_ERROR(str(e)))
        return self.__result(session, S_OK())

    def addSession(self, data):
        """Add new session

        :param dict data: session metadata

        :return: S_OK(dict)/S_ERROR()
        """
        attrts = {}
        if not data.get("expires_at"):
            data["expires_at"] = data["expires_in"] + time.time()
        self.log.debug("Add authorization session:", data)
        for k, v in data.items():
            if k not in AuthSession.__dict__.keys():
                self.log.warn("%s is not expected as authentication session attribute." % k)
            else:
                attrts[k] = v
        session = self.session()
        try:
            session.add(AuthSession(**attrts))
        except Exception as e:
            return self.__result(session, S_ERROR("Could not add Token: %s" % e))
        return self.__result(session, S_OK("Token successfully added"))

    def updateSession(self, data, sessionID):
        """Update session data

        :param dict data: data info
        :param str sessionID: sessionID

        :return: S_OK(object)/S_ERROR()
        """
        self.removeSession(sessionID=sessionID)
        return self.addSession(data)

    def removeSession(self, sessionID):
        """Remove session

        :param str sessionID: session id

        :return: S_OK()/S_ERROR()
        """
        session = self.session()
        try:
            # Remove all expired sessions
            session.query(AuthSession).filter(AuthSession.expires_at < time.time()).delete()
            session.query(AuthSession).filter(AuthSession.id == sessionID).delete()
        except Exception as e:
            return self.__result(session, S_ERROR(str(e)))
        return self.__result(session, S_OK())

    def getSession(self, sessionID):
        """Get client

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
        """Get client

        :param str userCode: user code

        :return: S_OK(dict)/S_ERROR()
        """
        session = self.session()
        try:
            resData = session.query(AuthSession).filter(AuthSession.user_code == userCode).first()
        except MultipleResultsFound:
            return self.__result(session, S_ERROR("%s is not unique ID." % userCode))
        except NoResultFound:
            return self.__result(session, S_ERROR("Session for %s user code is expired." % userCode))
        except Exception as e:
            return self.__result(session, S_ERROR(str(e)))
        return self.__result(session, S_OK(self.__rowToDict(resData)))

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
