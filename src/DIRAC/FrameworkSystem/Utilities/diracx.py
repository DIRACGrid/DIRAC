import requests

from cachetools import TTLCache, LRUCache, cached
from cachetools.keys import hashkey
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from collections.abc import Generator
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

legacy_exchange_session = requests.Session()


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

    r = legacy_exchange_session.get(
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
