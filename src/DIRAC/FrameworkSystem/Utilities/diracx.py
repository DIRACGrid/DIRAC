import random
import requests

from cachetools import TTLCache, LRUCache, cached
from cachetools.keys import hashkey
from pathlib import Path
from requests.adapters import HTTPAdapter
from tempfile import NamedTemporaryFile
from typing import Any
from collections.abc import Generator
from urllib3 import PoolManager
from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool

from DIRAC import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from contextlib import contextmanager

from diracx.core.preferences import DiracxPreferences

from diracx.core.utils import write_credentials

from diracx.core.models import TokenResponse

try:
    from diracx.client.sync import SyncDiracClient
except ImportError:
    # TODO: Remove this once diracx is tagged
    from diracx.client import DiracClient as SyncDiracClient

# How long tokens are kept
DEFAULT_TOKEN_CACHE_TTL = 5 * 60
DEFAULT_TOKEN_CACHE_SIZE = 1024

# Number of pools to use for a given host.
# It should be in the order of host behind the alias
SESSION_NUM_POOLS = 20
# Number of connection per Pool
SESSION_CONNECTION_POOL_MAX_SIZE = 10


class RandomizedPoolManager(PoolManager):
    """
    A PoolManager subclass that creates multiple connection pools per host.
    Each connection request randomly picks one of the available pools.
    """

    def __init__(self, num_pools=3, **kwargs):
        self.num_pools = num_pools
        super().__init__(**kwargs)

    def connection_from_host(self, host, port=None, scheme="http", pool_kwargs=None):
        # Pick a random index to diversify the pool key.

        rand_index = random.randint(0, self.num_pools - 1)
        pool_key = (f"{host}-{rand_index}", port, scheme)
        if pool_key in self.pools:
            return self.pools[pool_key]

        # Create a new pool if none exists for this key.
        if scheme == "http":
            self.pools[pool_key] = HTTPConnectionPool(host, port, **self.connection_pool_kw)
        elif scheme == "https":
            self.pools[pool_key] = HTTPSConnectionPool(host, port, **self.connection_pool_kw)
        else:
            raise ValueError(f"Unsupported scheme: {scheme}")

        return self.pools[pool_key]


class RandomizedHTTPAdapter(HTTPAdapter):
    """
    An HTTPAdapter that uses the RandomizedPoolManager.
    """

    def __init__(self, num_pools=3, maxsize=10, **kwargs):
        self.num_pools = num_pools
        self.custom_maxsize = maxsize
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        """
        Initialize the pool manager with our custom RandomizedPoolManager.
        """
        # This ends up being passed to the HTTP(s)ConnectionPool constructors
        pool_kwargs.update(
            {
                "maxsize": self.custom_maxsize,
                "block": block,
            }
        )
        self.poolmanager = RandomizedPoolManager(**pool_kwargs)


# Create a requests session.
diracx_session = requests.Session()
# Create an instance of the custom adapter.
diracx_pool_adapter = RandomizedHTTPAdapter(num_pools=SESSION_NUM_POOLS, maxsize=SESSION_CONNECTION_POOL_MAX_SIZE)

# Mount the adapter to handle both HTTP and HTTPS.
diracx_session.mount("http://", diracx_pool_adapter)
diracx_session.mount("https://", diracx_pool_adapter)


def get_token(
    username: str, group: str, dirac_properties: set[str], *, expires_minutes: int | None = None, source: str = ""
):
    """Do a legacy exchange to get a DiracX access_token+refresh_token

    The source parameter only purpose is to appear in the URL on diracx logs"""
    diracxUrl = gConfig.getValue("/DiracX/URL")
    if not diracxUrl:
        raise ValueError("Missing mandatory /DiracX/URL configuration")
    apiKey = gConfig.getValue("/DiracX/LegacyExchangeApiKey")
    if not apiKey:
        raise ValueError("Missing mandatory /DiracX/LegacyExchangeApiKey configuration")

    vo = Registry.getVOForGroup(group)
    scopes = [f"vo:{vo}", f"group:{group}"] + [f"property:{prop}" for prop in dirac_properties]

    r = diracx_session.get(
        f"{diracxUrl}/api/auth/legacy-exchange",
        params={
            "preferred_username": username,
            "scope": " ".join(scopes),
            "expires_minutes": expires_minutes,
            "source": source,
        },
        headers={"Authorization": f"Bearer {apiKey}"},
        timeout=10,
    )
    if not r.ok:
        raise RuntimeError(f"Error getting token from DiracX: {r.status_code} {r.text}")

    return r.json()


@cached(
    TTLCache(maxsize=DEFAULT_TOKEN_CACHE_SIZE, ttl=DEFAULT_TOKEN_CACHE_TTL),
    key=lambda a, b, c, **_: hashkey(a, b, *sorted(c)),
)
def _get_token_file(username: str, group: str, dirac_properties: set[str], *, source: str = "") -> Path:
    """Write token to a temporary file and return the path to that file"""
    data = get_token(username, group, dirac_properties, source=source)
    token_location = Path(NamedTemporaryFile().name)
    write_credentials(TokenResponse(**data), location=token_location)
    return token_location


diracx_client_cache = LRUCache(maxsize=64)


@contextmanager
def TheImpersonator(credDict: dict[str, Any], *, source: str = "") -> Generator[SyncDiracClient, None, None]:
    """
    Client to be used by DIRAC server needing to impersonate
    a user for diracx.
    It queries a token, places it in a file, and returns the `SyncDiracClient`
    class

    Use as a context manager
    """
    diracxUrl = gConfig.getValue("/DiracX/URL")
    if not diracxUrl:
        raise ValueError("Missing mandatory /DiracX/URL configuration")

    token_location = _get_token_file(
        credDict["username"],
        credDict["group"],
        set(credDict.get("groupProperties", []) + credDict.get("properties", [])),
        source=source,
    )
    client = diracx_client_cache.get(token_location)
    if client is None:
        pref = DiracxPreferences(url=diracxUrl, credentials_path=token_location)
        client = SyncDiracClient(diracx_preferences=pref)
        client.__enter__()
        diracx_client_cache[token_location] = client
    yield client
