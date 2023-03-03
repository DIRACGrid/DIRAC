""" Test IdProvider Factory"""
import pytest

from DIRAC.Resources.IdProvider.Utilities import (
    getIdProviderIdentifiers,
    getIdProviderIdentifierFromIssuerAndClientID,
)
from DIRAC.Resources.IdProvider.tests.IdProviderTestUtilities import setupConfig


idProviderValidSection = """
Resources
{
  IdProviders
  {
    SomeIdP1
    {
      ProviderType = OAuth2
      issuer = https://idp1.url/
      client_id = IdP1_client_id
      client_secret = IdP1_client_secret
      scope = openid+profile+offline_access+eduperson_entitlement
    }
    SomeIdP2.1
    {
      ProviderType = IAM
      issuer = https://idp2.url/
      client_id = IdP2.1_client_id
      client_secret = IdP2.1_client_secret
      scope = openid+profile+offline_access+eduperson_entitlement
    }
    SomeIdP2.2
    {
      ProviderType = IAM
      issuer = https://idp2.url/
      client_id = IdP2.2_client_id
      client_secret = IdP2.2_client_secret
      scope = openid+profile+offline_access+eduperson_entitlement
    }
    SomeIdP3
    {
      ProviderType = CheckIn
      issuer = https://idp3.url/
      client_id = IdP1_client_id
      client_secret = IdP3_client_secret
      scope = openid+profile+offline_access+eduperson_entitlement
    }
  }
}
"""


idProviderEmptySection = """
Resources
{
  IdProviders
  {
  }
}
"""


idProviderNotExistSection = """
Resources
{
}
"""


@pytest.mark.parametrize(
    "config, providerType, expectedValue",
    [
        # Normal cases
        (idProviderValidSection, None, {"OK": True, "Value": ["SomeIdP1", "SomeIdP2.1", "SomeIdP2.2", "SomeIdP3"]}),
        (idProviderValidSection, "IAM", {"OK": True, "Value": ["SomeIdP2.1", "SomeIdP2.2"]}),
        (idProviderValidSection, "CheckIn", {"OK": True, "Value": ["SomeIdP3"]}),
        (idProviderValidSection, "OAuth2", {"OK": True, "Value": ["SomeIdP1"]}),
        # Provider type does not exist
        (idProviderValidSection, "DoNotExist", {"OK": True, "Value": []}),
        # There is no IdProviders, but the section exists
        (idProviderEmptySection, "OAuth2", {"OK": True, "Value": []}),
        # There is no IdProviders section
        (
            idProviderNotExistSection,
            "OAuth2",
            {"OK": False, "Message": "Path /Resources/IdProviders does not exist or it's not a section"},
        ),
    ],
)
def test_getIdProviderIdentifiers(config, providerType, expectedValue):
    """Get IdProvider from the DIRAC configuration"""
    setupConfig(config)

    result = getIdProviderIdentifiers(providerType)
    assert result["OK"] == expectedValue["OK"]
    if result["OK"]:
        assert result["Value"] == expectedValue["Value"]
    else:
        assert result["Message"] == expectedValue["Message"]


@pytest.mark.parametrize(
    "config, issuer, clientID, expectedValue",
    [
        # Normal cases
        (idProviderValidSection, "https://idp1.url/", "IdP1_client_id", {"OK": True, "Value": "SomeIdP1"}),
        (idProviderValidSection, "https://idp2.url/", "IdP2.1_client_id", {"OK": True, "Value": "SomeIdP2.1"}),
        (idProviderValidSection, "https://idp2.url/", "IdP2.2_client_id", {"OK": True, "Value": "SomeIdP2.2"}),
        (idProviderValidSection, "https://idp3.url/", "IdP1_client_id", {"OK": True, "Value": "SomeIdP3"}),
        # Issuer does not exist
        (
            idProviderValidSection,
            "https://donotexist.url/",
            "IdP1_client_id",
            {"OK": False, "Message": "IdProvider not found"},
        ),
        # ClientID does not exist
        (
            idProviderValidSection,
            "https://idp2.url/",
            "IdPNotExist_client_id",
            {"OK": False, "Message": "IdProvider not found"},
        ),
        # Issuer and ClientID exist but are not bound to the same IdProvider identifier
        (
            idProviderValidSection,
            "https://idp2.url/",
            "IdP1_client_id",
            {"OK": False, "Message": "IdProvider not found"},
        ),
        # There is no IdProviders, but the section exists
        (idProviderEmptySection, "https://idp1.url/", None, {"OK": False, "Message": "IdProvider not found"}),
        # There is no IdProviders section
        (
            idProviderNotExistSection,
            "https://idp1.url/",
            None,
            {"OK": False, "Message": "Path /Resources/IdProviders does not exist or it's not a section"},
        ),
    ],
)
def test_getIdProviderIdentifierFromIssuerAndClientID(config, issuer, clientID, expectedValue):
    """Get IdProvider from the DIRAC configuration"""
    setupConfig(config)

    result = getIdProviderIdentifierFromIssuerAndClientID(issuer, clientID)
    assert result["OK"] == expectedValue["OK"]
    if result["OK"]:
        assert result["Value"] == expectedValue["Value"]
    else:
        assert result["Message"] == expectedValue["Message"]
