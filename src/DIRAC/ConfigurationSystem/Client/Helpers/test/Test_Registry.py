"""Unit tests for the Registry helpers"""

import pytest
from diraccfg import CFG

from DIRAC import S_OK, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers import Registry

REGISTRY = """
Registry
{
  Hosts
  {
    host1.example.org
    {
      DN = /DC=org/CN=host1.example.org
    }
    host2.example.org
    {
      DN = /DC=org/CN=host2.example.org, /DC=org/CN=host2-alias.example.org
    }
  }
}
"""


@pytest.fixture
def registry(mocker):
    cfg = CFG()
    cfg.loadFromBuffer(REGISTRY)
    mocker.patch.object(gConfig, "getSections", side_effect=lambda path: S_OK(_section(cfg, path).listSections()))
    mocker.patch.object(gConfig, "getValue", side_effect=lambda path, default=None: _value(cfg, path, default))
    Registry.reset_all_caches()
    yield
    Registry.reset_all_caches()


def _section(cfg, path):
    for part in path.strip("/").split("/"):
        cfg = cfg[part]
    return cfg


def _value(cfg, path, default):
    section, option = path.rsplit("/", 1)
    return _section(cfg, section).getOption(option, default)


@pytest.mark.parametrize(
    "dn, expected",
    [
        ("/DC=org/CN=host1.example.org", "host1.example.org"),
        (" /DC=org/CN=host2-alias.example.org ", "host2.example.org"),
    ],
)
def test_getHostnameForDN(registry, dn, expected):
    assert Registry.getHostnameForDN(dn) == {"OK": True, "Value": expected}


def test_getHostnameForDN_unknown(registry):
    assert not Registry.getHostnameForDN("/DC=org/CN=unknown")["OK"]


def test_getHostnameForDN_isCached(registry):
    """Repeated lookups of a DN do not search the CS again"""
    assert Registry.getHostnameForDN("/DC=org/CN=host2.example.org")["Value"] == "host2.example.org"
    calls = gConfig.getValue.call_count
    for _ in range(10):
        assert Registry.getHostnameForDN("/DC=org/CN=host2.example.org")["Value"] == "host2.example.org"
    assert gConfig.getValue.call_count == calls
    # reset_all_caches (called when the CS is reloaded) drops it
    Registry.reset_all_caches()
    assert Registry.getHostnameForDN("/DC=org/CN=host2.example.org")["Value"] == "host2.example.org"
    assert gConfig.getValue.call_count > calls
