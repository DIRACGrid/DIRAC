"""Unit tests for clearing the caches of the Registry helpers"""

import pytest

from DIRAC.ConfigurationSystem.Client.Helpers import Registry


@pytest.mark.parametrize(
    "function",
    [Registry.getUsernameForDN, Registry.getDNForUsername, Registry.getVOForGroup, Registry.getProxyProvidersForDN],
)
def test_resetAllCaches(function):
    """reset_all_caches clears the cache of every cached Registry function"""
    function.cache["key"] = "stale value"
    Registry.reset_all_caches()
    assert not function.cache
