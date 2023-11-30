from __future__ import annotations

__all__ = (
    "DiracXClient",
    "diracxTokenFromPEM",
)

import base64
import json
import re
import textwrap
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from diracx.client import DiracClient as _DiracClient
from diracx.core.models import TokenResponse
from diracx.core.preferences import DiracxPreferences
from diracx.core.utils import serialize_credentials

from DIRAC import gConfig, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Security.Locations import getDefaultProxyLocation
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue, returnValueOrRaise


PEM_BEGIN = "-----BEGIN DIRACX-----"
PEM_END = "-----END DIRACX-----"
RE_DIRACX_PEM = re.compile(rf"{PEM_BEGIN}\n(.*)\n{PEM_END}", re.MULTILINE | re.DOTALL)


@convertToReturnValue
def addTokenToPEM(pemPath, group):
    from DIRAC.Core.Base.Client import Client

    vo = Registry.getVOMSVOForGroup(group)
    disabledVOs = gConfig.getValue("/DiracX/DisabledVOs", [])
    if vo and vo not in disabledVOs:
        token_content = returnValueOrRaise(
            Client(url="Framework/ProxyManager", proxyLocation=pemPath).exchangeProxyForToken()
        )

        token = TokenResponse(
            access_token=token_content["access_token"],
            expires_in=token_content["expires_in"],
            token_type=token_content.get("token_type"),
            refresh_token=token_content.get("refresh_token"),
        )

        token_pem = f"{PEM_BEGIN}\n"
        data = base64.b64encode(serialize_credentials(token).encode("utf-8")).decode()
        token_pem += textwrap.fill(data, width=64)
        token_pem += f"\n{PEM_END}\n"

        with open(pemPath, "a") as f:
            f.write(token_pem)


def diracxTokenFromPEM(pemPath) -> dict[str, Any] | None:
    """Extract the DiracX token from the proxy PEM file"""
    pem = Path(pemPath).read_text()
    if match := RE_DIRACX_PEM.search(pem):
        match = match.group(1)
        return json.loads(base64.b64decode(match).decode("utf-8"))


@contextmanager
def DiracXClient() -> _DiracClient:
    """Get a DiracX client instance with the current user's credentials"""
    diracxUrl = gConfig.getValue("/DiracX/URL")
    if not diracxUrl:
        raise ValueError("Missing mandatory /DiracX/URL configuration")

    proxyLocation = getDefaultProxyLocation()
    diracxToken = diracxTokenFromPEM(proxyLocation)
    if not diracxToken:
        raise ValueError(f"No dirax token in the proxy file {proxyLocation}")

    with NamedTemporaryFile(mode="wt") as token_file:
        token_file.write(json.dumps(diracxToken))
        token_file.flush()
        token_file.seek(0)

        pref = DiracxPreferences(url=diracxUrl, credentials_path=token_file.name)
        with _DiracClient(diracx_preferences=pref) as api:
            yield api
