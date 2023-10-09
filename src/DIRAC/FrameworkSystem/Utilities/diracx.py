# pylint: disable=import-error

import requests

from cachetools import TTLCache, cached
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

# Add a cache not to query the token all the time
_token_cache = TTLCache(maxsize=100, ttl=DEFAULT_TOKEN_CACHE_TTL)


@cached(_token_cache, key=lambda x, y: repr(x))
def _get_token(credDict, diracxUrl, /) -> Path:
    """
    Write token to a temporary file and return the path to that file

    """

    apiKey = gConfig.getValue("/DiracX/LegacyExchangeApiKey")
    if not apiKey:
        raise ValueError("Missing mandatory /DiracX/LegacyExchangeApiKey configuration")

    vo = Registry.getVOForGroup(credDict["group"])
    dirac_properties = list(set(credDict.get("groupProperties", [])) | set(credDict.get("properties", [])))
    group = credDict["group"]

    scopes = [f"vo:{vo}", f"group:{group}"] + [f"property:{prop}" for prop in dirac_properties]

    r = requests.get(
        f"{diracxUrl}/api/auth/legacy-exchange",
        params={
            "preferred_username": credDict["username"],
            "scope": " ".join(scopes),
        },
        headers={"Authorization": f"Bearer {apiKey}"},
        timeout=10,
    )

    r.raise_for_status()

    token_location = Path(NamedTemporaryFile().name)

    write_credentials(TokenResponse(**r.json()), location=token_location)

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
    token_location = _get_token(credDict, diracxUrl)
    pref = DiracxPreferences(url=diracxUrl, credentials_path=token_location)

    return DiracClient(diracx_preferences=pref)
