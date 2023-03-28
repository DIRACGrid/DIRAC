""" Tests for IAMIDProvider interface.

Prior to run the test, make sure that:

* an IAM container is up and running.
* 2 clients are registered
* a user is defined

It is worth noting the tests will interact with IAM tokens and change their states.
"""
import os
import pytest
import requests

from diraccfg import CFG

from DIRAC import gConfig
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.Resources.IdProvider.IAMIdProvider import IAMIdProvider


config = """
Registry
{
  Groups
  {
    dirac_admin
    {
      Users = admin
      Properties = NormalUser
      IdPRole = wlcg.groups:/dirac/admin
    }
    dirac_prod
    {
      Users = jane_doe
      Properties = NormalUser,ProductionManagement
      VO = dirac
    }
    dirac_user
    {
      Users = jane_doe, richard_roe
      Properties = NormalUser
      VO = otherdirac
      IdPRole = wlcg.groups:/dirac/user
    }
  }
}
"""

#############################################################################

# Issuer
issuer = f"http://{os.environ['IAM_HOST']}:{os.environ['IAM_PORT']}/"

# Default parameters of the IAM container with simple client credentials
baseParams = {
    "issuer": issuer,
    "client_id": None,
    "client_secret": None,
    "scope": "openid+profile+offline_access",
}

# Default parameters of the IAM container with admin client credentials (token exchange allowed)
adminParams = {
    "issuer": issuer,
    "client_id": None,
    "client_secret": None,
    "scope": "openid+profile+offline_access",
}

# Default parameters of the IAM container with old client credentials
expiredParams = {
    "issuer": issuer,
    "client_id": "5f70a267-636a-430a-81d5-f885cab1c208",
    "client_secret": "OuHPKoix1cZI-YylbTcc2tUJENlgn5nLINJ86RWOzqQykYf9zCCOqLBLogYdljCoITQ2AwNfGwfN3VjItk-UKg",
    "scope": "openid+profile+offline_access",
}


#############################################################################


# Valid user tokens
validUserToken = {
    "access_token": None,
    "token_type": "Bearer",
    "refresh_token": None,
    "expires_in": 3599,
    "scope": "address phone openid email profile offline_access",
    "id_token": "eyJraWQiOiJyc2ExIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI3M2YxNmQ5My0yNDQxLTRhNTAtODhmZi04NTM2MGQ3OGM2YjUiLCJraWQiOiJyc2ExIiwiaXNzIjoiaHR0cDpcL1wvbG9jYWxob3N0OjgwODBcLyIsImdyb3VwcyI6W10sInByZWZlcnJlZF91c2VybmFtZSI6ImFkbWluIiwib3JnYW5pc2F0aW9uX25hbWUiOiJpbmRpZ28tZGMiLCJhdWQiOiI4YWI1ZmFkMS0zMTQwLTQyZjAtOGNiNi1kYTgzMzE1OTYyYmMiLCJuYW1lIjoiQWRtaW4gVXNlciIsImV4cCI6MTY3NjQ2NTM5OSwiaWF0IjoxNjc2NDY0Nzk5LCJqdGkiOiI3YzhlM2RhNC05OWEyLTQ4NjQtOWUyMy02NGVhOGNlOWRiOWYiLCJlbWFpbCI6IjFfYWRtaW5AaWFtLnRlc3QifQ.13i_HH8wwhxerwVP0l593Rzy0MmnPA3TivhAsqreBa5L0O7pxSDavsC10vaJyVQFiiib-a2qPnciY0VeWOreLtmAbud0i4KxWmn1MKG000nk0cIgftB0dbrgS6WRj61FtrSRMCPZuCkECNZ0BGH-Xx7qxfJoDtZ5ns_jwnAsBZn6As2xDBVhKfbMgjZtick3DwFRJK6hvGAgwrVFvPw9xVkSEJOv2fbB28TSLU_Cz9jYQFpptMLIj15JEV84gxpc5HFNaIVpBdAMLNIsOMOFsV5tnNy3VsW2IiMgDKc-DRNAmY4IWxC3BJgfxAGkAeLXdj31XaAne2PGafvlJA1HLQ",
}


# Valid user tokens but the access token is expired
expiredValidUserToken = {
    "access_token": "eyJraWQiOiJyc2ExIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI0ZjQ3M2RmNy05OGI2LTRlZTAtYTY2Yy02MDYwYTcwOWJlYjUiLCJpc3MiOiJodHRwOlwvXC9pYW0tbG9naW4tc2VydmljZTo4MDgwIiwiZXhwIjoxNjc3ODQ1NDU1LCJpYXQiOjE2Nzc4NDE4NTUsImp0aSI6IjlhN2E1NTY0LTk3OTktNDcxYi05M2FmLTNkYzAyZmFjZWY5OCIsImNsaWVudF9pZCI6ImNiMzM2OWVjLTIxMTUtNDEwZi05OGYxLWExODQ4MTAwMzE5MCJ9.kw_g76eYh7Ay5a5EhR_OVOQR3yjgXxkqF0oaIv-udZaIzA2MHdhXodB9BDUZaZOW3-FgnG9ZOfSNp5Dl6VWNGpO77M6DkImNBWkCpvDeHfl1giq8xUQ_bDO9isZQkpoEVZgxyBK1J2lmr-Z1Ef0ZR_kTb_UsRE0Eze_y_W20jS_TlHVULFd7j4UPspgOzFGAlVFfAfKhWhJVI76XxqCugeMhEYs43qQa4-cIBgLOB6KBddk1tq7-Rqw6EfgtYDb5_zChizX_4dfE10WKyDI50CU-OtMT88S7IHRPWGjoZ7Z3RaNdp6gHXDY67VHzKzouYqKAiIGyIqkPionmZaguow",
    "token_type": "Bearer",
    "refresh_token": None,
    "expires_in": 3599,
    "scope": "address phone openid email profile offline_access",
    "id_token": "eyJraWQiOiJyc2ExIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI2OTI3ODJiYS01Y2ZlLTQxMzktYTU5Zi1iYWNiMDY3YTBmZWYiLCJraWQiOiJyc2ExIiwiaXNzIjoiaHR0cDpcL1wvbG9jYWxob3N0OjgwODBcLyIsImdyb3VwcyI6WyJkaXJhY1wvdXNlciIsImRpcmFjXC9wcm9kIiwiZGlyYWMiXSwicHJlZmVycmVkX3VzZXJuYW1lIjoiamFuZV9kb2UiLCJvcmdhbmlzYXRpb25fbmFtZSI6ImluZGlnby1kYyIsImF1ZCI6ImM0MWE0Yjg3LTAxNzAtNGQ5MC1iZWU2LTNlM2E2OGQyZTY0NiIsIm5hbWUiOiJKYW5lIERvZSIsImV4cCI6MTY3NzA4MTk4NywiaWF0IjoxNjc3MDgxMzg3LCJqdGkiOiIzZjQ2ZmVhMS0yMTI5LTRkZGUtODgwNS1kMTZhNTA1MGIwNDYiLCJlbWFpbCI6ImphbmUuZG9lQGRvbm90ZXhpc3QuZW1haWwifQ.rSWiaUDp7oUP1lSOleJ7ANbKfQBVVr3KHXFR3-HKLmwakDUdKDz_6My0tyzs88RU4de2v9RMGHzCl5rgi2MFY_0DDiB_SOngafwAkXIik_vG48cg7wfGSX1IttgN4IriYKyCbLFwIo89_zPNdMfjYP-xHFYefKyzVyGDsRlGwy50bUBgIBdXF1oN49xw2B_LWlg0YZAszvaCaul2dTfQmBoYoBCI88t7uKbZAYV-A4TGOnG_PutfMbFDiYljyq37ouHM5c6NIvp7n-_mqevM7vX_K2aoOXgFAs3yZ5RK-nVVOntMTU9sZfCSxg3ywCRa0_vloXhgbRjFp3TizRwNJQ",
}


# Invalid user tokens: wrong access and refresh token
wrongUserToken = {
    "access_token": "eyJr.I84pZbzEyQ4zSG9MFXbXwBsz7e0_-sTBykYxs1vnLPFkxOtUriELsxRFNcR4icAXco3ZYyGo3wIBC3Q",
    "token_type": "Bearer",
    "refresh_token": "eyJhbGciOiJub25lIn0.eyJqdGkiOiI2NzUwYjiZDYxLTQ3ZmYtOTViYi00Y2NjNjNhOTg1MjMifQ.",
    "expires_in": 3599,
    "scope": "address phone openid email profile offline_access",
    "id_token": "eyJraWQiOiJyc2ExIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI3M2YxNmQ5My0yNDQxLTRhNTAtODhmZi04NTM2MGQ3OGM2YjUiLCJraWQiOiJyc2ExIiwiaXNzIjoiaHR0cDpcL1wvbG9jYWxob3N0OjgwODBcLyIsImdyb3VwcyI6W10sInByZWZlcnJlZF91c2VybmFtZSI6ImFkbWluIiwib3JnYW5pc2F0aW9uX25hbWUiOiJpbmRpZ28tZGMiLCJhdWQiOiI4YWI1ZmFkMS0zMTQwLTQyZjAtOGNiNi1kYTgzMzE1OTYyYmMiLCJuYW1lIjoiQWRtaW4gVXNlciIsImV4cCI6MTY3NjQ2NTM5OSwiaWF0IjoxNjc2NDY0Nzk5LCJqdGkiOiI3YzhlM2RhNC05OWEyLTQ4NjQtOWUyMy02NGVhOGNlOWRiOWYiLCJlbWFpbCI6IjFfYWRtaW5AaWFtLnRlc3QifQ.13i_HH8wwhxerwVP0l593Rzy0MmnPA3TivhAsqreBa5L0O7pxSDavsC10vaJyVQFiiib-a2qPnciY0VeWOreLtmAbud0i4KxWmn1MKG000nk0cIgftB0dbrgS6WRj61FtrSRMCPZuCkECNZ0BGH-Xx7qxfJoDtZ5ns_jwnAsBZn6As2xDBVhKfbMgjZtick3DwFRJK6hvGAgwrVFvPw9xVkSEJOv2fbB28TSLU_Cz9jYQFpptMLIj15JEV84gxpc5HFNaIVpBdAMLNIsOMOFsV5tnNy3VsW2IiMgDKc-DRNAmY4IWxC3BJgfxAGkAeLXdj31XaAne2PGafvlJA1HLQ",
}

# Valid client tokens
validClientToken = {
    "access_token": None,
    "token_type": "Bearer",
    "expires_in": 3599,
    "scope": "address phone openid email profile offline_access",
}


# Valid client tokens but the access token is expired
expiredValidClientToken = {
    "access_token": "eyJraWQiOiJyc2ExIiwiYWxnIjoiUlMyNTYifQ.eyJpc3MiOiJodHRwOlwvXC9pYW0tbG9naW4tc2VydmljZTo4MDgwIiwic3ViIjoiNWY3MGEyNjctNjM2YS00MzBhLTgxZDUtZjg4NWNhYjFjMjA4IiwiaWF0IjoxNjc3ODQ5OTIxLCJqdGkiOiIxNzhkZmZiMy1jYzYzLTRhM2EtOWVhZC0wMWJhNjg0MTEwNDgiLCJjbGllbnRfaWQiOiI1ZjcwYTI2Ny02MzZhLTQzMGEtODFkNS1mODg1Y2FiMWMyMDgifQ.udyuzv4HB3rg7_0X2lQT8rR0EcAcg0GYMm1azQ6WwfFbvdRLQfQWIKVFFJ9bVbLtpc7w282pFeqc7bKdN2vxXSpUbx0mkg404-oN5MgXik_oKTMtOXpaRUZGeL0xTF8XliD42wrx_tcmGu1dSyTI26ZnweDzteOhQ7faPJjycB-Q-FjTJ-R_3zqGaFEUYiPcbzMsXqo33gPaUtMeenVAWzwp9ATXEmH5gwBXcmTMaZnpLkMr-hRh3ZOYXOid_flKCRZFN633kZV3uHqLVj_4By-vclX06ajHRfYHXY3TJ9LBN4JLxssbF3O1adjzeYQ_aQ2E732WTOztNHokaMnHNg",
    "token_type": "Bearer",
    "expires_in": 3599,
    "scope": "address phone openid email profile offline_access",
}


# Invalid clien tokens: wrong access token
wrongClientToken = {
    "access_token": "eyJraWbF_c1QszqZzYgLH4uxoiej9qzsTPjRhPg",
    "token_type": "Bearer",
    "expires_in": 3599,
    "scope": "address phone openid email profile offline_access",
}


#############################################################################


@pytest.fixture(scope="module")
def iam_connection():
    """Prepare IAM tokens to run the tests"""
    # Get an admin token
    query = os.path.join(issuer, "token")
    params = {
        "grant_type": "password",
        "username": os.environ["IAM_ADMIN_USER"],
        "password": os.environ["IAM_ADMIN_PASSWORD"],
    }
    response = requests.post(
        query, params=params, auth=(os.environ["IAM_INIT_CLIENT_ID"], os.environ["IAM_INIT_CLIENT_SECRET"]), timeout=5
    )
    tokens = response.json()

    # Retrieve the simple client ID
    query = os.path.join(issuer, "iam/api/clients")
    headers = {
        "Authorization": f"Bearer {tokens.get('access_token')}",
        "Content-Type": "application/json",
    }
    response = requests.get(query, headers=headers, timeout=5)
    clients = response.json()

    for client in clients["Resources"]:
        if client["client_name"] == os.environ["IAM_SIMPLE_CLIENT_NAME"]:
            baseParams["client_id"] = client["client_id"]
            baseParams["client_secret"] = client["client_secret"]
            break

    for client in clients["Resources"]:
        if client["client_name"] == os.environ["IAM_ADMIN_CLIENT_NAME"]:
            adminParams["client_id"] = client["client_id"]
            adminParams["client_secret"] = client["client_secret"]
            break

    # Get a client token
    query = os.path.join(issuer, "token")
    params = {"grant_type": "client_credentials"}
    response = requests.post(
        query, params=params, auth=(baseParams["client_id"], baseParams["client_secret"]), timeout=5
    )
    tokens = response.json()
    validClientToken["access_token"] = tokens.get("access_token")

    # Get a user token
    params = {
        "grant_type": "password",
        "username": os.environ["IAM_SIMPLE_USER"],
        "password": os.environ["IAM_SIMPLE_PASSWORD"],
    }
    response = requests.post(
        query, params=params, auth=(baseParams["client_id"], baseParams["client_secret"]), timeout=5
    )
    tokens = response.json()
    validUserToken["access_token"] = tokens.get("access_token")
    validUserToken["refresh_token"] = tokens.get("refresh_token")
    expiredValidUserToken["refresh_token"] = tokens.get("refresh_token")


@pytest.mark.parametrize(
    "csAttribute, value, oauthAttribute, expectedValue, refreshed",
    [
        # Normal cases
        ("issuer", issuer, "issuer", issuer, True),
        ("issuer", issuer, "token_endpoint", f"{issuer}token", True),
        # The URL does not exist
        ("issuer", "http://donotexist:1234/", "issuer", "http://donotexist:1234/", False),
        ("issuer", "http://donotexist:1234/", "token_endpoint", None, False),
        # The requested metadata value does not exist
        ("issuer", issuer, "donotexist", None, True),
        ("issuer", "http://donotexist:1234/", "donotexist", None, False),
        # The CS parameter has no equivalent in IAM, Issuer is not defined
        ("DoNotExist", issuer, "issuer", None, False),
    ],
)
def test_getMetadata(iam_connection, csAttribute, oauthAttribute, value, expectedValue, refreshed):
    """Test getMetadata

    Executed twice:
    - the first time: the interface should fetch metadata from the server
    - the second time: the interface should get the metadata from its attributes
    """
    idProvider = IAMIdProvider(**{csAttribute: value})

    # First time: fetch metadata from server
    metadataFetchLast = idProvider.metadata_fetch_last
    result = idProvider.get_metadata(oauthAttribute)
    assert result == expectedValue
    # If it successfully fetched metadata, metadataFetchLast should be updated
    if refreshed:
        assert metadataFetchLast != idProvider.metadata_fetch_last
    else:
        assert metadataFetchLast == idProvider.metadata_fetch_last

    # Second time: should use cached metadata
    metadataFetchLast = idProvider.metadata_fetch_last
    result = idProvider.get_metadata(oauthAttribute)
    assert metadataFetchLast == idProvider.metadata_fetch_last


@pytest.mark.parametrize(
    "issuer, expectedValue",
    [
        # Normal cases
        (issuer, {"OK": True, "Value": None}),
        # URL does not exist
        ("http://donotexist:1234/", {"OK": False, "Message": "Connection failed"}),
    ],
)
def test_fetchMetadata(iam_connection, issuer, expectedValue):
    """Test fetchMetadata"""
    idProvider = IAMIdProvider(**{"issuer": issuer})
    result = idProvider.fetch_metadata()
    assert result["OK"] == expectedValue["OK"]
    if result["OK"]:
        assert result["Value"] == expectedValue["Value"]
    else:
        assert expectedValue["Message"] in result["Message"]


@pytest.mark.parametrize(
    "issuer, expectedValue",
    [
        # Normal cases
        (issuer, {"keys": [{"e": "AQAB", "kid": "rsa1", "kty": "RSA"}]}),
        # URL does not exist
        ("http://donotexist:1234/", None),
    ],
)
def test_getJWKs(iam_connection, issuer, expectedValue):
    """Test getJWKs

    Executed twice:
    - the first time: the interface should fetch JWKs from the server
    - the second time: the interface should get the JWKs from its attributes
    """
    idProvider = IAMIdProvider(**{"issuer": issuer})

    # First time: fetch jwks from server
    jwksFetchLast = idProvider.jwks_fetch_last
    result = idProvider.getJWKs()
    if not result:
        assert result == expectedValue
        # jwksFetchLast should have been updated
        assert jwksFetchLast == idProvider.jwks_fetch_last
    else:
        resultKey = result["keys"][0]
        expectedKey = expectedValue["keys"][0]
        assert resultKey["e"] == expectedKey["e"]
        assert resultKey["kid"] == expectedKey["kid"]
        assert resultKey["kty"] == expectedKey["kty"]

    # Second time: should use cached jwks
    jwksFetchLast = idProvider.jwks_fetch_last
    result = idProvider.getJWKs()
    assert jwksFetchLast == idProvider.jwks_fetch_last


@pytest.mark.parametrize(
    "issuer, expectedValue",
    [
        # Normal cases
        (issuer, {"OK": True, "Value": None}),
        # URL does not exists
        ("http://donotexist:1234/", {"OK": False, "Message": "Request exception"}),
    ],
)
def test_fetchJWKs(iam_connection, issuer, expectedValue):
    """Test fetchJWKs"""
    idProvider = IAMIdProvider(**{"issuer": issuer})
    result = idProvider.fetchJWKs()

    assert result["OK"] == expectedValue["OK"]
    if result["OK"]:
        assert result["Value"] == expectedValue["Value"]
    else:
        assert expectedValue["Message"] in result["Message"]


#############################################################################


@pytest.mark.parametrize(
    "token, idProviderParams, expectedValue",
    [
        # Normal cases
        (validUserToken, baseParams, {"OK": True, "Value": {"family_name": "Doe"}}),
        # The user access token is expired
        (expiredValidUserToken, expiredParams, {"OK": False, "Message": "Invalid access token"}),
        # The user access token is false
        (wrongUserToken, baseParams, {"OK": False, "Message": "Invalid access token"}),
        # A valid client access token is passed, but is not bound to a specific user
        # TOUPDATE: Message should actually be "Invalid access token"
        # The response contains HTML, an issue is opened here to fix this:
        # https://github.com/indigo-iam/iam/issues/569
        (validClientToken, baseParams, {"OK": False, "Message": "Result cannot be JSON-parsed"}),
        # The client access token is expired
        (expiredValidClientToken, expiredParams, {"OK": False, "Message": "Invalid access token"}),
        # The client access token is false
        (wrongClientToken, baseParams, {"OK": False, "Message": "Invalid access token"}),
        # No token at all
        ({}, baseParams, {"OK": False, "Message": "Invalid access token"}),
    ],
)
def test_getUserProfile(iam_connection, token, idProviderParams, expectedValue):
    """Test getUserProfile"""
    idProvider = IAMIdProvider(**idProviderParams)
    result = idProvider.getUserProfile(token.get("access_token"))

    assert result["OK"] == expectedValue["OK"]
    if result["OK"]:
        assert result["Value"]["family_name"] == expectedValue["Value"]["family_name"]
    else:
        assert expectedValue["Message"] in result["Message"]


@pytest.mark.parametrize(
    "token, expectedValue",
    [
        # Normal cases, group are not returned because scope=wlcg.groups and the wlcg.groups claim
        # is not considered by IAMIdProvider. Is it on purpose?
        (validUserToken, {"OK": True}),
        (expiredValidUserToken, {"OK": True}),
        (validClientToken, {"OK": True}),
        (expiredValidClientToken, {"OK": True}),
        # The access token is invalid
        (wrongUserToken, {"OK": False, "Message": "The provided token cannot be decoded"}),
        (wrongClientToken, {"OK": False, "Message": "The provided token cannot be decoded"}),
        # The access token is empty
        ({}, {"OK": False, "Message": "Access token is empty"}),
    ],
)
def test_getUserGroups(iam_connection, token, expectedValue):
    """Test getUserGroups"""
    gConfigurationData.localCFG = CFG()
    cfg = CFG()
    cfg.loadFromBuffer(config)
    gConfig.loadCFG(cfg)

    idProvider = IAMIdProvider(**baseParams)
    result = idProvider.getUserGroups(token.get("access_token"))
    assert result["OK"] == expectedValue["OK"]
    if not result["OK"]:
        assert expectedValue["Message"] in result["Message"]


#############################################################################


@pytest.mark.parametrize(
    "token, idProviderParams, claimant, expectedValue",
    [
        # Normal cases
        (validUserToken, baseParams, "user", {"OK": True}),
        (validClientToken, baseParams, "client", {"OK": True}),
        (expiredValidClientToken, expiredParams, "client", {"OK": True}),
        # The access token is invalid
        (wrongUserToken, baseParams, "user", {"OK": False, "Message": "The provided token cannot be decoded"}),
        (wrongClientToken, baseParams, "client", {"OK": False, "Message": "The provided token cannot be decoded"}),
        # The access token is empty
        ({}, baseParams, None, {"OK": False, "Message": "Access token is empty"}),
    ],
)
def test_verifyToken(iam_connection, token, idProviderParams, claimant, expectedValue):
    """Test verifyToken"""
    idProvider = IAMIdProvider(**idProviderParams)
    result = idProvider.verifyToken(token.get("access_token"))

    assert result["OK"] == expectedValue["OK"]
    if result["OK"]:
        tokenInfo = result["Value"]
        assert tokenInfo["client_id"] == idProviderParams["client_id"]
        assert tokenInfo["iss"].strip("/") == idProviderParams["issuer"].strip("/")
        if claimant == "client":
            assert tokenInfo["sub"] == idProviderParams["client_id"]
    else:
        assert expectedValue["Message"] in result["Message"]


#############################################################################


@pytest.mark.parametrize(
    "token, update, expectedValue",
    [
        # Normal cases
        # Here the order is very important: expiredValidUserToken should be used before validUserToken
        # Else, validUserToken will not be valid anymore for the next tests
        # Refreshing an access token triggers the generation of a new token, the revokation of the old one
        (expiredValidUserToken, False, {"OK": True}),
        (validUserToken, True, {"OK": True}),
        # The refresh token is not valid
        (wrongUserToken, False, {"OK": False, "Message": "Invalid refresh token"}),
        # The refresh token is empty
        ({}, False, {"OK": False, "Message": "Refresh token is empty"}),
    ],
)
def test_refreshToken(iam_connection, token, update, expectedValue):
    """Test refreshToken"""
    idProvider = IAMIdProvider(**baseParams)

    result = idProvider.refreshToken(token.get("refresh_token"))
    assert result["OK"] == expectedValue["OK"]
    if result["OK"]:
        resultToken = result["Value"]
        assert resultToken["scope"].split(" ").sort() == baseParams["scope"].split("+").sort()
        # Update the valid access token for next tests
        if update:
            token["access_token"] = resultToken["access_token"]
    else:
        assert expectedValue["Message"] in result["Message"]


@pytest.mark.parametrize(
    "token, expectedValue",
    [
        (validUserToken, {"OK": True}),
        (expiredValidUserToken, {"OK": False, "Message": "Invalid access token"}),
        (wrongUserToken, {"OK": False, "Message": "Invalid access token"}),
        ({}, {"OK": False, "Message": "Access token is empty"}),
    ],
)
def test_exchangeToken(iam_connection, token, expectedValue):
    """Test exchangeToken"""
    idProvider = IAMIdProvider(**adminParams)
    result = idProvider.exchangeToken(token.get("access_token"))

    assert result["OK"] == expectedValue["OK"]
    if result["OK"]:
        resultToken = result["Value"]
        resultPayload = idProvider.verifyToken(resultToken["access_token"])["Value"]
        payload = idProvider.verifyToken(token["access_token"])["Value"]
        assert resultPayload["sub"] == payload["sub"]
        assert resultPayload["client_id"] != payload["client_id"]
    else:
        assert expectedValue["Message"] in result["Message"]


@pytest.mark.parametrize(
    "token, tokenType, hint, expectedValue",
    [
        # A hint is provided
        (validUserToken, "access_token", True, {"OK": True}),
        (validUserToken, "refresh_token", True, {"OK": True}),
        (wrongUserToken, "access_token", True, {"OK": True}),
        (wrongUserToken, "refresh_token", True, {"OK": True}),
        (expiredValidUserToken, "access_token", True, {"OK": True}),
        (validClientToken, "access_token", True, {"OK": True}),
        (expiredValidClientToken, "access_token", True, {"OK": True}),
        (wrongClientToken, "access_token", True, {"OK": True}),
        # No hint
        (validUserToken, "access_token", False, {"OK": True}),
        (validUserToken, "refresh_token", False, {"OK": True}),
        (wrongUserToken, "access_token", False, {"OK": True}),
        (wrongUserToken, "refresh_token", False, {"OK": True}),
        (expiredValidUserToken, "access_token", False, {"OK": True}),
        (expiredValidUserToken, "refresh_token", False, {"OK": True}),
        (validClientToken, "access_token", False, {"OK": True}),
        (expiredValidClientToken, "access_token", False, {"OK": True}),
        (wrongClientToken, "access_token", False, {"OK": True}),
        # Special cases
        ({"access_token": ""}, "access_token", True, {"OK": False, "Message": "Token is empty"}),
        ({"refresh_token": ""}, "refresh_token", True, {"OK": False, "Message": "Token is empty"}),
        ({"notexist_token": ""}, "notexist_token", True, {"OK": False, "Message": "Token is empty"}),
    ],
)
def test_revokeToken(iam_connection, token, tokenType, hint, expectedValue):
    """Test revokeToken"""
    idProvider = IAMIdProvider(**baseParams)
    if hint:
        result = idProvider.revokeToken(token[tokenType], tokenTypeHint=tokenType)
    else:
        result = idProvider.revokeToken(token[tokenType])
    assert result["OK"] == expectedValue["OK"]
    if not result["OK"]:
        assert result["Message"] == expectedValue["Message"]
