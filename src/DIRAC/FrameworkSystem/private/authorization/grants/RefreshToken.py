from authlib.oauth2.base import OAuth2Error
from authlib.oauth2.rfc6749.grants import RefreshTokenGrant as _RefreshTokenGrant

from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN, wrapIDAsDN


class RefreshTokenGrant(_RefreshTokenGrant):
    """See :class:`authlib.oauth2.rfc6749.grants.RefreshTokenGrant`"""

    DEFAULT_EXPIRES_AT = 12 * 3600
    TOKEN_ENDPOINT_AUTH_METHODS = ["client_secret_basic", "client_secret_post", "none"]

    def authenticate_refresh_token(self, refresh_token):
        """Get credential for token

        :param str refresh_token: refresh token

        :return: dict or None
        """
        result = self.server.readToken(refresh_token)
        if not result["OK"]:
            raise OAuth2Error(result["Message"])
        rtDict = result["Value"]
        result = self.server.db.getCredentialByRefreshToken(rtDict["jti"])
        if not result["OK"]:
            raise OAuth2Error(result["Message"])
        credential = result["Value"]

        if int(rtDict["iat"]) != int(credential["issued_at"]):
            # An attempt to reuse the refresh token was detected
            prov = self.server.idps.getIdProvider(rtDict["provider"])
            if prov["OK"]:
                prov["Value"].revokeToken(credential["refresh_token"])
                prov["Value"].revokeToken(credential["access_token"], "access_token")
            return None

        credential.update(rtDict)
        return credential

    def authenticate_user(self, credential):
        """Authorize user

        :param dict credential: credential (token payload)

        :return: str or bool
        """
        result = getUsernameForDN(wrapIDAsDN(credential["sub"]))
        if not result["OK"]:
            self.server.log.error(result["Message"])
        return result.get("Value")

    def issue_token(self, user, credential):
        """Refresh tokens

        :param user: unuse
        :param dict credential: token credential

        :return: dict
        """
        if credential["refresh_token"]:
            result = self.server.idps.getIdProvider(credential["provider"])
            if result["OK"]:
                result = result["Value"].refreshToken(credential["refresh_token"])
        else:
            result = self.server.tokenCli.getToken(user, self.server._getScope(credential["scope"], "g"))
        if result["OK"]:
            token = result["Value"]
            result = self.server.registerRefreshToken(credential, token)
        if not result["OK"]:
            raise OAuth2Error(result["Message"])
        return result["Value"]

    def revoke_old_credential(self, credential):
        """Remove old credential"""
        pass
