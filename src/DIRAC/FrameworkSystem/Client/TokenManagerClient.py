""" The TokenManagerClient is a class representing the client of the DIRAC
:py:mod:`TokenManager <DIRAC.FrameworkSystem.Service.TokenManagerHandler>` service.
"""
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import ThreadSafe
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import OAuth2Token

gTokensSync = ThreadSafe.Synchronizer()


@createClient("Framework/TokenManager")
class TokenManagerClient(Client):
    """Client exposing the TokenManager Service."""

    DEFAULT_RT_EXPIRATION_TIME = 24 * 3600

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.setServer("Framework/TokenManager")
        self.__tokensCache = DictCache()
        self.idps = IdProviderFactory()

    @gTokensSync
    def getToken(
        self,
        username: str,
        userGroup: str = None,
        scope: str = None,
        audience: str = None,
        identityProvider: str = None,
        requiredTimeLeft: int = 0,
    ):
        """Get an access token for a user/group keeping the local cache

        :param username: user name
        :param userGroup: group name
        :param scope: scope
        :param audience: audience
        :param identityProvider: identity Provider
        :param requiredTimeLeft: required time

        :return: S_OK(dict)/S_ERROR()
        """
        if not identityProvider and userGroup:
            identityProvider = Registry.getIdPForGroup(userGroup)
        if not identityProvider:
            return S_ERROR(f"The {userGroup} group belongs to a VO that is not tied to any Identity Provider.")

        # prepare the client instance of the appropriate IdP
        result = self.idps.getIdProvider(identityProvider)
        if not result["OK"]:
            return result
        idpObj = result["Value"]

        if userGroup and (result := idpObj.getGroupScopes(userGroup)):
            # What scope correspond to the requested group?
            scope = list(set((scope or []) + result))

        # Set the scope
        idpObj.scope = " ".join(scope)

        # Let's check if there are corresponding tokens in the cache
        cacheKey = (username, idpObj.scope, audience, identityProvider)
        if self.__tokensCache.exists(cacheKey, requiredTimeLeft):
            # Well we have a fresh record containing a Token object
            token = self.__tokensCache.get(cacheKey)
            # Let's check if the access token is fresh
            if not token.is_expired(requiredTimeLeft):
                return S_OK(token)

        result = self.executeRPC(
            username, userGroup, scope, audience, identityProvider, requiredTimeLeft, call="getToken"
        )

        if result["OK"]:
            token = OAuth2Token(dict(result["Value"]))
            self.__tokensCache.add(
                cacheKey,
                token.get_claim("exp", "refresh_token") or self.DEFAULT_RT_EXPIRATION_TIME,
                token,
            )

        return result


gTokenManager = TokenManagerClient()
