"""TokenManager service is a HTTPs-exposed service responsible for token management, namely storing, updating,
requesting new tokens for DIRAC components that have the appropriate permissions.

.. note:: As a newly created service, it will not support the old DIPS protocol, which is living to its age.

.. literalinclude:: ../ConfigTemplate.cfg
    :start-after: ##BEGIN TokenManager:
    :end-before: ##END
    :dedent: 2
    :caption: TokenManager options

The most common use of this service is to obtain user tokens with certain scope to return to the user for its purposes,
or to provide to the DIRAC service to perform asynchronous tasks on behalf of the user.
This is mainly about the :py:meth:`export_getToken` method.

.. image:: /_static/Systems/FS/TokenManager_getToken.png
    :alt: https://dirac.readthedocs.io/en/integration/_images/TokenManager_getToken.png (source https://github.com/TaykYoku/DIRACIMGS/raw/main/TokenManagerService_getToken.ai)

The service and its client have a mechanism for caching the received tokens.
This helps reduce the number of requests to both the service and the Identity Provider (IdP).

If the client has a valid **access token** in the cache, it is used until it expires.
After that you need to update. The client can update it independently if on the server where it is in ``dirac.cfg``
``client_id`` and ``client_secret`` of the Identity Provider client are registered.

Otherwise, the client makes an RPC call to the **TornadoManager** service.
It in turn checks the cache and if the ``access token`` is already invalid tries to update it using a ``refresh token``.
If the required token is not in the cache, then the ``refresh token`` from :py:class:`TokenDB <DIRAC.FrameworkSystem.DB.TokenDB.TokenDB>`
is taken and the **exchange token** request to Identity Provider is made. The received tokens are cached.
"""

import pprint

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security import Properties
from DIRAC.Core.Utilities import ThreadSafe
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC.FrameworkSystem.DB.TokenDB import TokenDB
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory

# Used to synchronize the cache with user tokens
gTokensSync = ThreadSafe.Synchronizer()


class TokenManagerHandler(TornadoService):

    DEFAULT_AUTHORIZATION = ["authenticated"]
    DEFAULT_RT_EXPIRATION_TIME = 24 * 3600

    @classmethod
    def initializeHandler(cls, *args):
        """Initialization

        :return: S_OK()/S_ERROR()
        """
        # Let's try to connect to the database
        try:
            cls.__tokenDB = TokenDB(parentLogger=cls.log)
        except Exception as e:
            cls.log.exception(e)
            return S_ERROR(f"Could not connect to the database {repr(e)}")

        # Cache containing tokens from scope requested by the client
        cls.__tokensCache = DictCache()

        # The service plays an important OAuth 2.0 role, namely it is an Identity Provider client.
        # This allows you to manage tokens without the involvement of their owners.
        cls.idps = IdProviderFactory()
        return S_OK()

    def export_getUserTokensInfo(self):
        """Generate information dict about user tokens

        :return: dict
        """
        tokensInfo = []
        credDict = self.getRemoteCredentials()
        result = Registry.getDNForUsername(credDict["username"])
        if not result["OK"]:
            return result
        for dn in result["Value"]:
            result = Registry.getIDFromDN(dn)
            if result["OK"]:
                result = self.__tokenDB.getTokensByUserID(result["Value"])
                if not result["OK"]:
                    return result
                tokensInfo += result["Value"]
        return S_OK(tokensInfo)

    auth_getUsersTokensInfo = [Properties.PROXY_MANAGEMENT]

    def export_getUsersTokensInfo(self, users: list):
        """Get the info about the user tokens in the database

        :param users: user names

        :return: S_OK(list) -- return list of tokens dictionaries
        """
        tokensInfo = []
        for user in users:
            # Find the user ID among his DNs
            result = Registry.getDNForUsername(user)
            if not result["OK"]:
                return result
            for dn in result["Value"]:
                uid = Registry.getIDFromDN(dn).get("Value")
                if uid:
                    result = self.__tokenDB.getTokensByUserID(uid)
                    if not result["OK"]:
                        self.log.error(result["Message"])
                    else:
                        for tokenDict in result["Value"]:
                            if tokenDict not in tokensInfo:
                                # The database does not contain a username,
                                # as it is a unique user ID exclusively for DIRAC
                                # and is not associated with a token.
                                tokenDict["username"] = user
                                tokensInfo.append(tokenDict)
        return S_OK(tokensInfo)

    def export_updateToken(self, token: dict, userID: str, provider: str, rt_expired_in: int = 24 * 3600):
        """Using this method, you can transfer user tokens for storage in the TokenManager.

        It is important to note that TokenManager saves only one token per user and, accordingly,
        the Identity Provider from which it was issued. So when a new token is delegated,
        keep in mind that the old token will be deleted.

        :param token: token
        :param userID: user ID
        :param provider: provider name
        :param rt_expired_in: refresh token expires time (in seconds)

        :return: S_OK(list)/S_ERROR() -- list contain uploaded tokens info as dictionaries
        """
        self.log.verbose(f"Update {userID} user token issued by {provider}:\n", pprint.pformat(token))
        # prepare the client instance of the appropriate IdP to revoke the old tokens
        result = self.idps.getIdProvider(provider)
        if not result["OK"]:
            return result
        idPObj = result["Value"]
        # overwrite old tokens with new ones
        result = self.__tokenDB.updateToken(token, userID, provider, rt_expired_in)
        if not result["OK"]:
            return result
        # revoke the old tokens
        for oldToken in result["Value"]:
            if "refresh_token" in oldToken and oldToken["refresh_token"] != token["refresh_token"]:
                self.log.verbose("Revoke old refresh token:\n", pprint.pformat(oldToken))
                idPObj.revokeToken(oldToken["refresh_token"])
        # Let's return to the current situation with the storage of user tokens
        return self.__tokenDB.getTokensByUserID(userID)

    def __checkProperties(self, requestedUserDN: str, requestedUserGroup: str):
        """Check the properties and return if they can only download limited tokens if authorized

        :param requestedUserDN: user DN
        :param requestedUserGroup: DIRAC group

        :return: S_OK(bool)/S_ERROR()
        """
        credDict = self.getRemoteCredentials()
        if Properties.FULL_DELEGATION in credDict["properties"]:
            return S_OK(False)
        if Properties.LIMITED_DELEGATION in credDict["properties"]:
            return S_OK(True)
        if Properties.PRIVATE_LIMITED_DELEGATION in credDict["properties"]:
            if credDict["DN"] != requestedUserDN:
                return S_ERROR("You are not allowed to download any token")
            if Properties.PRIVATE_LIMITED_DELEGATION not in Registry.getPropertiesForGroup(requestedUserGroup):
                return S_ERROR("You can't download tokens for that group")
            return S_OK(True)
        # Not authorized!
        return S_ERROR("You can't get tokens!")

    @gTokensSync
    def export_getToken(
        self,
        username: str,
        userGroup: str = None,
        scope: str = None,
        audience: str = None,
        identityProvider: str = None,
        requiredTimeLeft: int = 0,
    ):
        """Get an access token for a user/group.

        * Properties:
            * FullDelegation <- permits full delegation of tokens
            * LimitedDelegation <- permits downloading only limited tokens
            * PrivateLimitedDelegation <- permits downloading only limited tokens for one self

        :paarm username: user name
        :param userGroup: user group
        :param scope: requested scope
        :param audience: requested audience
        :param identityProvider: Identity Provider name
        :param requiredTimeLeft: requested minimum life time

        :return: S_OK(dict)/S_ERROR()
        """
        if not identityProvider and userGroup:
            identityProvider = Registry.getIdPForGroup(userGroup)
        if not identityProvider:
            return S_ERROR("The %s group belongs to the VO that is not tied to any Identity Provider." % userGroup)

        # prepare the client instance of the appropriate IdP
        result = self.idps.getIdProvider(identityProvider)
        if not result["OK"]:
            return result
        idpObj = result["Value"]

        if userGroup and (result := idpObj.getGroupScopes(userGroup))["OK"]:
            # What scope correspond to the requested group?
            scope = list(set((scope or []) + result["Value"]))

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
            # It seems that it is no longer valid for us, but whether there is a refresh token?
            if token.get("refresh_token"):
                # Okay, so we can try to refresh tokens
                if (result := idpObj.refreshToken(token["refresh_token"]))["OK"]:
                    # caching new tokens
                    self.__tokensCache.add(
                        cacheKey,
                        result["Value"].get_claim("exp", "refresh_token") or self.DEFAULT_RT_EXPIRATION_TIME,
                        result["Value"],
                    )
                    return result
                self.log.verbose(f"Failed to get token with cached tokens: {result['Message']}")
                # Let's try to revoke broken token
                idpObj.revokeToken(token["refresh_token"])

        err = []
        # The cache did not help, so let's make an exchange token
        result = Registry.getDNForUsername(username)
        if not result["OK"]:
            return result
        for dn in result["Value"]:
            # For backward compatibility, the user ID is written as DN. So let's check if this DN contains a user ID
            result = Registry.getIDFromDN(dn)
            if result["OK"]:
                uid = result["Value"]
                # To do this, first find the refresh token stored in the database with the maximum scope
                result = self.__tokenDB.getTokenForUserProvider(uid, identityProvider)
                if result["OK"] and result["Value"]:
                    idpObj.token = result["Value"]
                    result = self.__checkProperties(dn, userGroup)
                    if result["OK"]:
                        # exchange token with requested scope
                        result = idpObj.exchangeToken()
                        if result["OK"]:
                            # caching new tokens
                            self.__tokensCache.add(
                                cacheKey,
                                result["Value"].get_claim("exp", "refresh_token") or self.DEFAULT_RT_EXPIRATION_TIME,
                                result["Value"],
                            )
                            return result
                # Not find any token associated with the found user ID
                err.append(result.get("Message", "No token found for %s." % uid))
        # Collect all errors when trying to get a token, or if no user ID is registered
        return S_ERROR("; ".join(err or ["No user ID found for %s" % username]))

    def export_deleteToken(self, userDN: str):
        """Delete a token from the DB

        :param userDN: user DN

        :return: S_OK()/S_ERROR()
        """
        # Delete it from cache
        credDict = self.getRemoteCredentials()
        if Properties.PROXY_MANAGEMENT not in credDict["properties"]:
            if userDN != credDict["DN"]:
                return S_ERROR("You aren't allowed!")
        result = Registry.getIDFromDN(userDN)
        return self.__tokenDB.removeToken(user_id=result["Value"]) if result["OK"] else result
