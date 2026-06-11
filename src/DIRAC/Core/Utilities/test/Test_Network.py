""" Test case for DIRAC.Core.Utilities.Network module
"""
import pytest

from DIRAC.Core.Utilities.Network import getFQDN, getIPsForHostName, checkHostsMatch


def test_getFQDN():
    assert isinstance(getFQDN(), str)


@pytest.mark.parametrize(
    "host1, host2, isValid, expected",
    [
        ("localhost", "localhost", True, True),
        ("localhost", "example.com", True, False),
        ("localhost", "example.invalid", False, False),
        ("example.com", "localhost", True, False),
        ("example.invalid", "localhost", False, False),
    ],
)
def test_checkHostsMatch(host1, host2, isValid, expected):
    result = checkHostsMatch(host1, host2)
    if isValid:
        assert result["OK"]
        assert result["Value"] is expected
    else:
        assert not result["OK"]


@pytest.mark.parametrize("hostname", ["localhost", "example.com"])
def test_getIPsForHostName(hostname):
    result = getIPsForHostName(hostname)
    assert result["OK"]
    assert len(result["Value"]) >= 1
