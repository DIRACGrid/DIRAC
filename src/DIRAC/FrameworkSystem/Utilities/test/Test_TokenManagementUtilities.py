""" Test IdProvider Factory"""
import pytest
import time

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.FrameworkSystem.private.authorization.utils.Tokens import OAuth2Token
from DIRAC.FrameworkSystem.Utilities.TokenManagementUtilities import getCachedKey, getCachedToken
from DIRAC.Resources.IdProvider.OAuth2IdProvider import OAuth2IdProvider


@pytest.mark.parametrize(
    "idProviderType, idProviderName, issuer, username, group, scope, audience, expectedValue",
    [
        # Only a client name: this is mandatory
        (OAuth2IdProvider, "IdPTest", "Issuer1", None, None, None, None, ("IdPTest", None, None, "IdPTest", "Issuer1")),
        (
            OAuth2IdProvider,
            "IdPTest2",
            "Issuer1",
            None,
            None,
            None,
            None,
            ("IdPTest2", None, None, "IdPTest2", "Issuer1"),
        ),
        (
            OAuth2IdProvider,
            "IdPTest2",
            "Issuer2",
            None,
            None,
            None,
            None,
            ("IdPTest2", None, None, "IdPTest2", "Issuer2"),
        ),
        # Client name and username
        (OAuth2IdProvider, "IdPTest", "Issuer1", "user", None, None, None, ("user", None, None, "IdPTest", "Issuer1")),
        # Client name and group (should not add any permission in scope)
        (
            OAuth2IdProvider,
            "IdPTest",
            "Issuer1",
            None,
            "group",
            None,
            None,
            ("IdPTest", None, None, "IdPTest", "Issuer1"),
        ),
        # Client name and scope
        (
            OAuth2IdProvider,
            "IdPTest",
            "Issuer1",
            None,
            None,
            ["permission:1", "permission:2"],
            None,
            ("IdPTest", "permission:1 permission:2", None, "IdPTest", "Issuer1"),
        ),
        (
            OAuth2IdProvider,
            "IdPTest",
            "Issuer1",
            None,
            None,
            ["permission:2", "permission:1"],
            None,
            ("IdPTest", "permission:1 permission:2", None, "IdPTest", "Issuer1"),
        ),
        # Client name and audience
        (
            OAuth2IdProvider,
            "IdPTest",
            "Issuer1",
            None,
            None,
            None,
            "CE1",
            ("IdPTest", None, "CE1", "IdPTest", "Issuer1"),
        ),
        # Client name, username, group
        (
            OAuth2IdProvider,
            "IdPTest",
            "Issuer1",
            "user",
            "group1",
            None,
            None,
            ("user", None, None, "IdPTest", "Issuer1"),
        ),
        # Client name, username, scope
        (
            OAuth2IdProvider,
            "IdPTest",
            "Issuer1",
            "user",
            None,
            ["permission:1", "permission:2"],
            None,
            ("user", "permission:1 permission:2", None, "IdPTest", "Issuer1"),
        ),
        # Client name, username, audience
        (
            OAuth2IdProvider,
            "IdPTest",
            "Issuer1",
            "user",
            None,
            None,
            "CE1",
            ("user", None, "CE1", "IdPTest", "Issuer1"),
        ),
        # Client name, username, group, scope
        (
            OAuth2IdProvider,
            "IdPTest",
            "Issuer1",
            "user",
            "group1",
            ["permission:1", "permission:2"],
            None,
            ("user", "permission:1 permission:2", None, "IdPTest", "Issuer1"),
        ),
        # Client name, username, group, audience
        (
            OAuth2IdProvider,
            "IdPTest",
            "Issuer1",
            "user",
            "group1",
            None,
            "CE1",
            ("user", None, "CE1", "IdPTest", "Issuer1"),
        ),
        # Client name, usergroup, scope, audience
        (
            OAuth2IdProvider,
            "IdPTest",
            "Issuer1",
            "user",
            "group1",
            ["permission:1", "permission:2"],
            "CE1",
            ("user", "permission:1 permission:2", "CE1", "IdPTest", "Issuer1"),
        ),
    ],
)
def test_getCachedKey(idProviderType, idProviderName, issuer, username, group, scope, audience, expectedValue):
    """Test getCachedKey"""
    # Prepare IdP
    idProviderClient = idProviderType()
    idProviderClient.name = idProviderName
    idProviderClient.issuer = issuer

    result = getCachedKey(idProviderClient, username, group, scope, audience)
    assert result == expectedValue


@pytest.mark.parametrize(
    "cachedKey, requiredTimeLeft, expectedValue",
    [
        # Normal case
        (("IdPTest", "permission:1 permission:2", "CE1", "IdPTest", "Issuer1"), 0, S_OK()),
        # Empty cachedKey
        ((), 0, S_ERROR("The key does not exist")),
        # Wrong cachedKey
        (("IdPTest", "permission:1", "CE1", "IdPTest", "Issuer1"), 0, S_ERROR("The key does not exist")),
        # Expired token (650 > 150)
        (
            ("IdPTest", "permission:1 permission:2", "CE1", "IdPTest", "Issuer1"),
            650,
            S_ERROR("Token found but expired"),
        ),
        # Expired cachedKey (1500 > 1200)
        (
            ("IdPTest", "permission:1 permission:2", "CE1", "IdPTest", "Issuer1"),
            1500,
            S_ERROR("The key does not exist"),
        ),
    ],
)
def test_getCachedToken(cachedKey, requiredTimeLeft, expectedValue):
    """Test getCachedToken"""
    # Prepare cachedToken dictionary
    cachedTokens = DictCache()
    currentTime = time.time()
    token = {
        "sub": "0001234",
        "aud": "CE1",
        "nbf": currentTime - 150,
        "scope": "permission:1 permission:2",
        "iss": "Issuer1",
        "exp": currentTime + 150,
        "iat": currentTime - 150,
        "jti": "000001234",
        "client_id": "0001234",
    }
    tokenKey = ("IdPTest", "permission:1 permission:2", token["aud"], "IdPTest", token["iss"])
    cachedTokens.add(tokenKey, 1200, OAuth2Token(token))

    # Try to get the token from the cache
    result = getCachedToken(cachedTokens, cachedKey, requiredTimeLeft)
    assert result["OK"] == expectedValue["OK"]
    if result["OK"]:
        resultToken = result["Value"]
        assert resultToken["sub"] == token["sub"]
        assert resultToken["scope"] == token["scope"]
    else:
        assert result["Message"] == expectedValue["Message"]
