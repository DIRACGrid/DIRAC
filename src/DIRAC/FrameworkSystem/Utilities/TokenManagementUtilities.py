""" TokenManagementUtilties contains simple functions used by both TokenManagerClient and TokenManagerHandler."""

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory


DEFAULT_RT_EXPIRATION_TIME = 24 * 3600
DEFAULT_AT_EXPIRATION_TIME = 1200


def getIdProviderClient(userGroup: str, idProviderClientName: str = None, client_name_prefix: str = ""):
    """Get an IdProvider client

    :param userGroup: group name
    :param idProviderClientName: name of an identity provider in the DIRAC CS
    :param client_name_prefix: prefix of the client in the CS options
    """
    # Get IdProvider credentials from CS
    if not idProviderClientName and userGroup:
        idProviderClientName = Registry.getIdPForGroup(userGroup)
    if not idProviderClientName:
        return S_ERROR(f"The {userGroup} group belongs to the VO that is not tied to any Identity Provider.")

    # Prepare the client instance of the appropriate IdP
    return IdProviderFactory().getIdProvider(idProviderClientName, client_name_prefix=client_name_prefix)


def getCachedKey(
    idProviderClient,
    username: str = None,
    userGroup: str = None,
    scope: list[str] = None,
    audience: str = None,
):
    """Build the key to potentially retrieve a cached token given the provided parameters.

    :param cachedTokens: dictionary of cached tokens
    :param idProviderClient: name of an identity provider in the DIRAC CS
    :param username: user name
    :param userGroup: group name
    :param scope: scope
    :param audience: audience
    """
    # Get subject
    # if username is not defined, then we aim at fetching a client token
    # see https://www.rfc-editor.org/rfc/rfc6749#section-4.4 for further details
    subject = username
    if not subject:
        subject = idProviderClient.name

    # Get scope
    if userGroup and (result := idProviderClient.getGroupScopes(userGroup)):
        # What scope correspond to the requested group?
        scope = list(set((scope or []) + result))

    if scope:
        scope = " ".join(sorted(scope))

    return (subject, scope, audience, idProviderClient.name, idProviderClient.issuer)


def getCachedToken(cachedTokens: DictCache, cachedKey: str, requiredTimeLeft: int = 0):
    """Check whether a token related to the information provided is present in cache.

    :param cachedTokens: dictionary of cached tokens
    :param cachedKey: use to retrieve a potential valid token from cachedTokens
    :param requiredTimeLeft: required time
    """
    if not cachedTokens.exists(cachedKey, requiredTimeLeft):
        return S_ERROR("The key does not exist")

    # Well we have a fresh record containing a Token object
    token = cachedTokens.get(cachedKey)

    # Let's check if the access token is fresh
    if token.is_expired(requiredTimeLeft):
        return S_ERROR("Token found but expired")

    return S_OK(token)
