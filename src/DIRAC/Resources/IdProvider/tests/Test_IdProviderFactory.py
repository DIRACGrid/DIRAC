""" Test IdProvider Factory"""
import pytest
import time

from authlib.jose import jwt

from DIRAC.Resources.IdProvider.IdProviderFactory import IdProviderFactory
from DIRAC.Resources.IdProvider.tests.IdProviderTestUtilities import setupConfig

config = """
DIRAC
{
  Security
  {
    Authorization
    {
      issuer = https://issuer.url/
      Clients
      {
        DIRACWeb
        {
          client_id = client_identificator
          client_secret = client_secret_key
          redirect_uri = https://redirect.url/
        }
      }
    }
  }
}
Resources
{
  IdProviders
  {
    SomeIdP1.1
    {
      ProviderType = OAuth2
      issuer = https://idp.url/
      client_id = IdP_client_id1
      client_secret = IdP_client_secret
      scope = openid+profile+offline_access+eduperson_entitlement
    }
    SomeIdP1.2
    {
      ProviderType = OAuth2
      issuer = https://idp.url/
      client_id = IdP_client_id2
      client_secret = IdP_client_secret
      scope = openid+profile+offline_access+eduperson_entitlement
    }
    SomeIdP2
    {
      ProviderType = OAuth2
      issuer = https://another-idp.url/
      client_id = IdP_client_id1
      client_secret = IdP_client_secret
      scope = openid+profile+offline_access+eduperson_entitlement
    }
    SomeIdP3
    {
      ProviderType = OAuth2
      issuer = https://and-another-idp.url/
      client_id = IdP_client_id3
      client_secret = IdP_client_secret
      scope = openid+profile+offline_access+eduperson_entitlement
    }
  }
}
"""
idps = IdProviderFactory()


@pytest.mark.parametrize(
    "clientName, expectedResult, expectedIssuer, expectedClientId, expectedClientSecret",
    [
        # Normal cases
        ("SomeIdP1.1", {"OK": True}, "https://idp.url/", "IdP_client_id1", "IdP_client_secret"),
        ("SomeIdP1.2", {"OK": True}, "https://idp.url/", "IdP_client_id2", "IdP_client_secret"),
        ("SomeIdP2", {"OK": True}, "https://another-idp.url/", "IdP_client_id1", "IdP_client_secret"),
        ("SomeIdP3", {"OK": True}, "https://and-another-idp.url/", "IdP_client_id3", "IdP_client_secret"),
        # Try to get an unknown DIRAC client
        ("DIRACUnknown", {"OK": False, "Message": "DIRACUnknown does not exist"}, None, None, None),
    ],
)
def test_getIdProvider(clientName, expectedResult, expectedIssuer, expectedClientId, expectedClientSecret):
    """Test getIdProvider"""
    setupConfig(config)
    result = idps.getIdProvider(clientName)
    assert result["OK"] == expectedResult["OK"]

    if not result["OK"]:
        assert expectedResult["Message"] in result["Message"]
    else:
        idProvider = result["Value"]
        assert expectedIssuer in idProvider.server_metadata_url

        if expectedClientId:
            assert idProvider.client_id == expectedClientId
        else:
            assert idProvider.client_secret is None

        if expectedClientSecret:
            assert idProvider.client_secret == expectedClientSecret
        else:
            assert idProvider.client_secret is None


@pytest.mark.parametrize(
    "payload, expectedResult, errorMessage, expectedIdentifier",
    [
        # Normal cases
        ({"sub": "user", "iss": "https://idp.url/", "client_id": "IdP_client_id1"}, True, "", "SomeIdP1.1"),
        ({"sub": "user", "iss": "https://idp.url/", "client_id": "IdP_client_id2"}, True, "", "SomeIdP1.2"),
        ({"sub": "user", "iss": "https://another-idp.url/", "client_id": "IdP_client_id1"}, True, "", "SomeIdP2"),
        ({"sub": "user", "iss": "https://and-another-idp.url/", "client_id": "IdP_client_id3"}, True, "", "SomeIdP3"),
        # JWT token without client_id
        ({"sub": "user", "iss": "https://idp.url/"}, False, "Cannot retrieve the IdProvider that emitted", None),
        # JWT token without issuer
        ({"sub": "user", "client_id": "IdP_client_id"}, False, "Cannot retrieve the IdProvider that emitted", None),
        # Invalid token
        ("invalidToken", False, "The provided token cannot be decoded", None),
    ],
)
def test_getIdProviderFromToken(payload, expectedResult, errorMessage, expectedIdentifier):
    """Test getIdProviderFromToken"""
    setupConfig(config)
    accessToken = payload
    if isinstance(payload, dict):
        # Create a JWT token from payload
        timeParameters = dict(
            iat=int(time.time()),
            exp=int(time.time()) + (12 * 3600),
        )
        payload.update(timeParameters)

        accessToken = jwt.encode(
            {"alg": "HS256"},
            payload,
            "secret",
        ).decode("utf-8")

    # Retrieve the IdProvider that issued the token
    result = idps.getIdProviderFromToken(accessToken)
    assert result["OK"] == expectedResult
    if result["OK"]:
        assert result["Value"].name == expectedIdentifier
    else:
        assert errorMessage in result["Message"]
