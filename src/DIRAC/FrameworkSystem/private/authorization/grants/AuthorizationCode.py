""" This class describe Authorization Code grant type
"""
from time import time
from authlib.jose import JsonWebSignature
from authlib.oauth2.base import OAuth2Error
from authlib.oauth2.rfc6749.grants import AuthorizationCodeGrant as _AuthorizationCodeGrant
from authlib.common.encoding import json_b64encode, urlsafe_b64decode, json_loads


class OAuth2Code(dict):
    """This class describe Authorization Code object"""

    def __init__(self, params):
        """C'or"""
        params["auth_time"] = params.get("auth_time", int(time()))
        super().__init__(params)

    @property
    def user(self):
        return self.get("user_id")

    @property
    def code_challenge(self):
        return self.get("code_challenge")

    @property
    def code_challenge_method(self):
        return self.get("code_challenge_method", "pain")

    def is_expired(self):
        return self.get("auth_time") + 300 < time()

    def get_redirect_uri(self):
        return self.get("redirect_uri")

    def get_scope(self):
        return self.get("scope", "")

    def get_auth_time(self):
        return self.get("auth_time")

    def get_nonce(self):
        return self.get("nonce")


class AuthorizationCodeGrant(_AuthorizationCodeGrant):
    """See :class:`authlib.oauth2.rfc6749.grants.AuthorizationCodeGrant`"""

    TOKEN_ENDPOINT_AUTH_METHODS = ["client_secret_basic", "client_secret_post", "none"]

    def save_authorization_code(self, code, request):
        pass

    def delete_authorization_code(self, authorization_code):
        pass

    def query_authorization_code(self, code, client):
        """Parse authorization code

        :param code: authorization code as JWS
        :param client: client

        :return: OAuth2Code or None
        """
        self.server.log.debug("Query authorization code:", code)
        jws = JsonWebSignature(algorithms=["RS256"])
        result = self.server.db.getKeySet()
        if not result["OK"]:
            raise Exception(result["Message"])
        err = None
        data = None
        for key in result["Value"].keys:
            try:
                data = jws.deserialize_compact(code, key)
            except Exception as e:
                err = e
        if err:
            self.server.log.error("Cannot get authorization code:", repr(err))
            return None
        try:
            item = OAuth2Code(json_loads(urlsafe_b64decode(data["payload"])))
            self.server.log.debug("Authorization code scope:", item.get_scope())
        except Exception as e:
            self.server.log.error("Cannot read authorization code:", repr(e))
            return None
        if not item.is_expired():
            return item

    def authenticate_user(self, authorization_code):
        """Authenticate the user related to this authorization_code.

        :param authorization_code: authorization code
        """
        return authorization_code.user

    def generate_authorization_code(self):
        """The method to generate "code" value for authorization code data.

        :return: str
        """
        self.server.log.debug("Generate authorization code for credentials:", self.request.user)
        jws = JsonWebSignature(algorithms=["RS256"])
        protected = {"alg": "RS256"}
        code = OAuth2Code(
            {
                "user_id": self.request.user["ID"],
                # These scope already contain DIRAC groups
                "scope": self.request.data["scope"],
                "redirect_uri": self.request.args["redirect_uri"],
                "client_id": self.request.args["client_id"],
                "code_challenge": self.request.args.get("code_challenge"),
                "code_challenge_method": self.request.args.get("code_challenge_method"),
            }
        )
        self.server.log.debug("Authorization code generated:", dict(code))
        result = self.server.db.getPrivateKey()
        if not result["OK"]:
            raise OAuth2Error("Cannot generate authorization code: %s" % result["Message"])
        return jws.serialize_compact(protected, json_b64encode(dict(code)), result["Value"])
