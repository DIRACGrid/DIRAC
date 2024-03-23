""" The TokenManagerClient is a class representing the client of the DIRAC
:py:mod:`TokenManager <DIRAC.FrameworkSystem.Service.TokenManagerHandler>` service.
"""

import time

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import ThreadSafe
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import OAuth2Token
from DIRAC.FrameworkSystem.Utilities.TokenManagementUtilities import (
    getIdProviderClient,
    getCachedKey,
    getCachedToken,
    DEFAULT_AT_EXPIRATION_TIME,
    DEFAULT_RT_EXPIRATION_TIME,
)

gTokensSync = ThreadSafe.Synchronizer()


@createClient("Framework/TokenManager")
class TokenManagerClient(Client):
    """Client exposing the TokenManager Service."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.setServer("Framework/TokenManager")
        self.__tokensCache = DictCache()
        self.idps = IdProviderFactory()

    def getToken(
        self,
        username: str = None,
        userGroup: str = None,
        scope: list[str] = None,
        audience: str = None,
        identityProvider: str = None,
        requiredTimeLeft: int = 0,
        useCache: bool = True,
    ):
        """Get an access token for a user/group

        :param username: user name
        :param userGroup: group name
        :param scope: scope
        :param audience: audience
        :param identityProvider: identity Provider
        :param requiredTimeLeft: required time
        :param cacheToken: if True (default) save the token in cache.
                Otherwise it is not cached but it avoids the lock

        :return: S_OK(dict)/S_ERROR()
        """
        meth = self.getTokenWithCache if useCache else self.getTokenWithoutCache

        return meth(
            username=username,
            userGroup=userGroup,
            scope=scope,
            audience=audience,
            identityProvider=identityProvider,
            requiredTimeLeft=requiredTimeLeft,
        )

    def getTokenWithoutCache(
        self,
        username: str = None,
        userGroup: str = None,
        scope: list[str] = None,
        audience: str = None,
        identityProvider: str = None,
        requiredTimeLeft: int = 0,
    ):
        """Get an access token for a user/group without caching it

        :param username: user name
        :param userGroup: group name
        :param scope: scope
        :param audience: audience
        :param identityProvider: identity Provider
        :param requiredTimeLeft: required time

        :return: S_OK(dict)/S_ERROR()
        """
        # Get an IdProvider Client instance
        result = getIdProviderClient(userGroup, identityProvider)
        if not result["OK"]:
            return result
        idpObj = result["Value"]

        # No token in cache: get a token from the server
        return self.executeRPC(username, userGroup, scope, audience, idpObj.name, requiredTimeLeft, call="getToken")

    @gTokensSync
    def getTokenWithCache(
        self,
        username: str = None,
        userGroup: str = None,
        scope: list[str] = None,
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
        # Get an IdProvider Client instance
        result = getIdProviderClient(userGroup, identityProvider)
        if not result["OK"]:
            return result
        idpObj = result["Value"]

        # Search for an existing token in tokensCache
        cachedKey = getCachedKey(idpObj, username, userGroup, scope, audience)
        result = getCachedToken(self.__tokensCache, cachedKey, requiredTimeLeft)
        if result["OK"]:
            # A valid token has been found and is returned
            return result

        # No token in cache: get a token from the server
        result = self.executeRPC(username, userGroup, scope, audience, idpObj.name, requiredTimeLeft, call="getToken")

        # Save token in cache
        if result["OK"]:
            token = OAuth2Token(dict(result["Value"]))

            # Get the date at which the token will expire (it is expressed as a Unix timestamp)
            # If the refresh token is present, we use it as we can easily generate an access token from it
            duration = token.get_claim("exp", "access_token") or DEFAULT_AT_EXPIRATION_TIME
            if token.get("refresh_token"):
                duration = token.get_claim("exp", "refresh_token") or DEFAULT_RT_EXPIRATION_TIME

            self.__tokensCache.add(
                cachedKey,
                duration - time.time(),
                token,
            )

        return result


gTokenManager = TokenManagerClient()
