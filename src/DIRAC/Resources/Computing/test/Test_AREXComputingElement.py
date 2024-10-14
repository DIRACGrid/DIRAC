import os
import requests
from unittest.mock import MagicMock, patch

from DIRAC import S_ERROR, S_OK


def test__checkSession(mocker):
    """Test checkSession"""
    from DIRAC.Resources.Computing.AREXComputingElement import AREXComputingElement

    arex = AREXComputingElement("test")

    # 1. The session has not been initialized: it shoud return an error
    result = arex._checkSession()

    assert not result["OK"], result
    assert not arex.session
    assert "Authorization" not in arex.headers, arex.headers

    # 2. This time the session is initialized but there is no proxy nor token
    # It should return an error
    arex.session = requests.Session()
    result = arex._checkSession()

    assert not result["OK"], result
    assert arex.session
    assert not arex.session.cert
    assert "Authorization" not in arex.headers, arex.headers

    # 3. We set a malformed proxy, but not a token: it should return an error
    arex.proxy = "fake proxy"
    mocker.patch(
        "DIRAC.Resources.Computing.AREXComputingElement.AREXComputingElement._prepareProxy", return_value=S_ERROR()
    )

    result = arex._checkSession()
    assert not result["OK"], result
    assert arex.session
    assert not arex.session.cert
    assert "Authorization" not in arex.headers, arex.headers

    # 4. We set a proxy, but not a token: the session should include the proxy, but not the token
    arex.proxy = "fake proxy"

    def side_effect():
        os.environ["X509_USER_PROXY"] = arex.proxy
        return S_OK()

    mocker.patch(
        "DIRAC.Resources.Computing.AREXComputingElement.AREXComputingElement._prepareProxy", side_effect=side_effect
    )

    result = arex._checkSession()
    assert result["OK"], result
    assert arex.session
    assert arex.session.cert
    assert "Authorization" not in arex.headers, arex.headers

    # 5. We set a proxy and a token: the session should just include
    # the token because the proxy is not mandatory
    arex.proxy = "fake proxy"
    arex.token = {"access_token": "fake token"}

    result = arex._checkSession()
    assert result["OK"], result
    assert arex.session
    assert not arex.session.cert
    assert "Authorization" in arex.headers, arex.headers

    # 7. Now we just include the token:
    # the session should only include the token
    arex.proxy = None
    result = arex._checkSession()
    assert result["OK"], result
    assert arex.session
    assert not arex.session.cert
    assert "Authorization" in arex.headers, arex.headers
