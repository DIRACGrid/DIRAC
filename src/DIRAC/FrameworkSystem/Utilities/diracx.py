import requests

from cachetools import TTLCache, cached
from cachetools.keys import hashkey
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from DIRAC import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers import Registry


from diracx.core.preferences import DiracxPreferences

from diracx.core.utils import write_credentials

from diracx.core.models import TokenResponse
from diracx.client import DiracClient

# How long tokens are kept
DEFAULT_TOKEN_CACHE_TTL = 5 * 60
DEFAULT_TOKEN_CACHE_SIZE = 1024


def get_token(username: str, group: str, dirac_properties: set[str], *, expires_minutes: int | None = None):
    """Do a legacy exchange to get a DiracX access_token+refresh_token"""
    diracxUrl = gConfig.getValue("/DiracX/URL")
    if not diracxUrl:
        raise ValueError("Missing mandatory /DiracX/URL configuration")
    apiKey = gConfig.getValue("/DiracX/LegacyExchangeApiKey")
    if not apiKey:
        raise ValueError("Missing mandatory /DiracX/LegacyExchangeApiKey configuration")

    vo = Registry.getVOForGroup(group)
    scopes = [f"vo:{vo}", f"group:{group}"] + [f"property:{prop}" for prop in dirac_properties]

    r = requests.get(
        f"{diracxUrl}/api/auth/legacy-exchange",
        params={
            "preferred_username": username,
            "scope": " ".join(scopes),
            "expires_minutes": expires_minutes,
        },
        headers={"Authorization": f"Bearer {apiKey}"},
        timeout=10,
    )
    if not r.ok:
        raise RuntimeError(f"Error getting token from DiracX: {r.status_code} {r.text}")

    return r.json()


@cached(
    TTLCache(maxsize=DEFAULT_TOKEN_CACHE_SIZE, ttl=DEFAULT_TOKEN_CACHE_TTL),
    key=lambda a, b, c: hashkey(a, b, *sorted(c)),
)
def _get_token_file(username: str, group: str, dirac_properties: set[str]) -> Path:
    """Write token to a temporary file and return the path to that file"""
    data = get_token(username, group, dirac_properties)
    token_location = Path(NamedTemporaryFile().name)
    write_credentials(TokenResponse(**data), location=token_location)
    return token_location


def TheImpersonator(credDict: dict[str, Any]) -> DiracClient:
    """
    Client to be used by DIRAC server needing to impersonate
    a user for diracx.
    It queries a token, places it in a file, and returns the `DiracClient`
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
    )
    pref = DiracxPreferences(url=diracxUrl, credentials_path=token_location)

    return DiracClient(diracx_preferences=pref)
