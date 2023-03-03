from authlib.oauth2.base import OAuth2Error
from authlib.oauth2.rfc7009 import RevocationEndpoint as _RevocationEndpoint


class RevocationEndpoint(_RevocationEndpoint):
    """See :class:`authlib.oauth2.rfc7009.RevocationEndpoint`"""

    def query_token(self, token, token_type_hint, client):
        """Query requested token from database.

        :param str token: token
        :param str token_type_hint: token type
        :param client: client

        :return: dict
        """
        if token_type_hint == "refresh_token":
            result = self.server.readToken(token)
            if not result["OK"]:
                raise OAuth2Error(result["Message"])
            rtDict = result["Value"]
            result = self.server.db.getCredentialByRefreshToken(rtDict["jti"])
            if not result["OK"]:
                raise OAuth2Error(result["Message"])
            return result["Value"]
        return {token_type_hint: token}

    def revoke_token(self, token):
        """Mark the give token as revoked.

        :param dict token: token dict
        """
        result = self.server.idps.getIdProviderFromToken(token["access_token"])
        if not result["OK"]:
            raise OAuth2Error(result["Message"])
        if result["OK"]:
            for tokenType in token:
                result = result["Value"].revokeToken(token[tokenType], tokenType)
                if not result["OK"]:
                    self.server.log.error(result["Message"])
        if not result["OK"]:
            raise OAuth2Error(result["Message"])
