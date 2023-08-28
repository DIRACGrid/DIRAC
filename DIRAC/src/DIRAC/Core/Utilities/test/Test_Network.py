""" Test case for DIRAC.Core.Utilities.Network module
"""
import pytest

from DIRAC.Core.Utilities.Network import discoverInterfaces, getFQDN, getIPsForHostName, checkHostsMatch


def test_discoverInterfaces():
    interfaces = discoverInterfaces()
    assert len(interfaces) >= 1
    assert "lo" in interfaces or "lo0" in interfaces
    assert interfaces.get("lo", interfaces.get("lo0"))["ip"].startswith("127")
    for interfaceInfo in interfaces.values():
        assert "ip" in interfaceInfo
        assert "mac" in interfaceInfo


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
