from __future__ import annotations

__all__ = (
    "addRPCStub",
    "DiracXClient",
    "diracxTokenFromPEM",
    "executeRPCStub",
    "FutureClient",
)

import base64
import functools
import hashlib
import importlib
import json
import re
import textwrap
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile, gettempdir
from typing import Any

try:
    from diracx.client.sync import SyncDiracClient
except ImportError:
    # TODO: Remove this once diracx is tagged
    from diracx.client import DiracClient as SyncDiracClient
from diracx.core.models import TokenResponse
from diracx.core.preferences import DiracxPreferences
from diracx.core.utils import serialize_credentials

from DIRAC import gConfig, gLogger
from DIRAC.Core.Utilities.File import secureOpenForWrite

from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Security.Locations import getDefaultProxyLocation
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue, returnValueOrRaise, isReturnStructure


PEM_BEGIN = "-----BEGIN DIRACX-----"
PEM_END = "-----END DIRACX-----"
RE_DIRACX_PEM = re.compile(rf"{PEM_BEGIN}\n(.*?)\n{PEM_END}", re.DOTALL)


@convertToReturnValue
def addTokenToPEM(pemPath, group):
    from DIRAC.Core.Base.Client import Client

    vo = Registry.getVOForGroup(group)
    if not vo:
        gLogger.error(f"ERROR: Could not find VO for group {group}, DiracX will not work!")
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

        pem = Path(pemPath).read_text()
        # Remove any existing DiracX token there would be
        new_pem = re.sub(RE_DIRACX_PEM, "", pem)
        new_pem += token_pem

        Path(pemPath).write_text(new_pem)


def diracxTokenFromPEM(pemPath) -> dict[str, Any] | None:
    """Extract the DiracX token from the proxy PEM file"""
    pem = Path(pemPath).read_text()
    if match := RE_DIRACX_PEM.findall(pem):
        if len(match) > 1:
            raise ValueError("Found multiple DiracX tokens, this should never happen")
        match = match[0]
        return json.loads(base64.b64decode(match).decode("utf-8"))


class FutureClient:
    """This is just a empty class to make sure that all the FutureClients
    inherit from a common class.
    """

    ...


@contextmanager
def DiracXClient() -> Iterator[SyncDiracClient]:
    """Get a DiracX client instance with the current user's credentials"""
    diracxUrl = gConfig.getValue("/DiracX/URL")
    if not diracxUrl:
        raise ValueError("Missing mandatory /DiracX/URL configuration")

    proxyLocation = getDefaultProxyLocation()
    diracxToken = diracxTokenFromPEM(proxyLocation)
    if not diracxToken:
        raise ValueError(f"No diracx token in the proxy file {proxyLocation}")

    hash = hashlib.sha256(diracxToken["refresh_token"].split(".")[1].encode())
    token_file = Path(gettempdir()) / f"dx_{hash.hexdigest()}"
    if not token_file.exists():
        token_file.parent.mkdir(parents=True, exist_ok=True)
        with secureOpenForWrite(token_file) as (fd, _):
            fd.write(json.dumps(diracxToken))

    pref = DiracxPreferences(url=diracxUrl, credentials_path=token_file)
    with SyncDiracClient(diracx_preferences=pref) as api:
        yield api


def addRPCStub(meth):
    """Decorator to add an rpc like stub to DiracX adapter method
    to be called by the ForwardDISET operation

    """

    @functools.wraps(meth)
    def inner(self, *args, **kwargs):
        dCls = self.__class__.__name__
        dMod = self.__module__
        res = meth(self, *args, **kwargs)
        if isReturnStructure(res):
            res["rpcStub"] = {
                "dCls": dCls,
                "dMod": dMod,
                "dMeth": meth.__name__,
                "args": args,
                "kwargs": kwargs,
            }
        return res

    return inner


def executeRPCStub(stub: dict):
    className = stub.get("dCls")
    modName = stub.get("dMod")
    methName = stub.get("dMeth")
    methArgs = stub.get("args")
    methKwArgs = stub.get("kwargs")
    # Load the module
    mod = importlib.import_module(modName)
    # import the class
    cl = getattr(mod, className)

    # Check that cl is a subclass of JSerializable,
    # and that we are not putting ourselves in trouble...
    if not (isinstance(cl, type) and issubclass(cl, FutureClient)):
        raise TypeError("Only subclasses of FutureClient can be decoded")

    # Instantiate the object
    obj = cl()
    meth = getattr(obj, methName)
    return meth(*methArgs, **methKwArgs)
