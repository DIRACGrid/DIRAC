"""
Single interface to the FTS REST client libraries. This is the only module allowed to import them.

The FTS servers are defined in the CS, in one section per flavour::

  Resources
  {
    FTSEndpoints
    {
      FTS3
      {
        CERN-FTS3 = https://fts3-pilot.cern.ch:8446
      }
      FTS4
      {
        CERN-FTS4 = https://fts4.cern.ch:8446
      }
    }
  }

The flavour of a server determines the library used to talk to it:
``fts3`` (fts-rest-flask) for FTS3 servers, ``fts4`` for FTS4 servers.
Both have the same ``easy`` API, except for some job parameters (see :py:func:`buildJob`).
The ``fts4`` library is optional: it is only needed if FTS4 servers are defined.

The contexts (connections to a server with a given identity) are cached by :py:class:`FTSContextCache`.
"""

from __future__ import annotations

import errno
import inspect
import threading
from datetime import timedelta
from types import ModuleType
from typing import TYPE_CHECKING

import fts3.rest.client.easy as easy_fts3

try:
    import fts4.rest.client.easy as easy_fts4
except ImportError:
    easy_fts4 = None

from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getFTSServersByFlavour
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.FrameworkSystem.Client.TokenManagerClient import gTokenManager

if TYPE_CHECKING:
    from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job

#: ``easy`` module of the client library of each flavour (None if not installed)
EASY_FTS = {"FTS3": easy_fts3, "FTS4": easy_fts4}

#: Flavour assumed for servers not (or not anymore) in the CS
DEFAULT_FLAVOUR = "FTS3"

#: Number of retries FTS does for each transfer
FTS_RETRY = 3

#: Minimum lifetime (in seconds) of the FTS tokens
TOKEN_REQUIRED_TIME_LEFT = 3600


##########################################################################
# Libraries and servers


def _easyFts(flavour: str) -> ModuleType:
    """``easy`` module of the library of a flavour

    :raises ValueError: unknown flavour, or library not installed
    """
    easyFts = EASY_FTS.get(flavour)
    if easyFts is None:
        raise ValueError(f"No FTS client library for flavour {flavour}")
    return easyFts


def _flavourOf(context) -> str:
    """Flavour of the library a context belongs to"""
    for flavour, easyFts in EASY_FTS.items():
        if easyFts is not None and isinstance(context, easyFts.Context):
            return flavour
    raise ValueError(f"Unknown FTS context type {type(context)}")


def _normalizeURL(url: str) -> str:
    return url.rstrip("/")


def getFlavour(serverURL: str) -> str:
    """Flavour of a server, from the CS. Servers not in the CS are assumed to be FTS3"""
    normalized = _normalizeURL(serverURL)
    for flavour, servers in getFTSServersByFlavour()["Value"].items():
        if any(_normalizeURL(url) == normalized for url in servers.values()):
            return flavour
    return DEFAULT_FLAVOUR


##########################################################################
# Operations


def _toError(context, e: Exception, what: str):
    """Convert a library exception into an S_ERROR (errno.ESRCH if not found)"""
    if isinstance(e, _easyFts(_flavourOf(context)).exceptions.NotFound):
        return S_ERROR(errno.ESRCH, f"{what}: not found on {context.endpoint}")
    return S_ERROR(f"{what}: {e}")


def newContext(serverURL: str, ucert: str | None = None, accessToken: str | None = None):
    """Create a context to a server, with the library of its flavour.
    It costs a query to the server.

    :param serverURL: URL of the server
    :param ucert: proxy file, for an X509 context
    :param accessToken: token, for a token based context

    :returns: context
    :raises: in case the context cannot be created
    """
    easyFts = _easyFts(getFlavour(serverURL))
    if accessToken:
        return easyFts.Context(serverURL, fts_access_token=accessToken, verify=False)
    return easyFts.Context(serverURL, ucert=ucert, verify=False)


def usesTokens(context) -> bool:
    return context.access_method == "oauth2"


def delegate(context, lifetime: int):
    """Check the delegation of an X509 context (2 queries), and renew it if it is missing,
    or if less than a third of ``lifetime`` is left. Nothing is done for token based contexts.

    :param lifetime: lifetime (in seconds) of the new delegation

    :raises: in case the delegation fails
    """
    lifetime = timedelta(seconds=lifetime)
    _easyFts(_flavourOf(context)).delegate(context, lifetime=lifetime, delegate_when_lifetime_lt=lifetime // 3)


def _newJob(easyFts: ModuleType, **params) -> dict:
    """``new_job``, refusing the parameters the library does not know.
    The libraries accept and silently ignore them.

    :raises TypeError: unknown parameter
    """
    known = inspect.signature(easyFts.new_job).parameters
    unknown = [name for name in params if name not in known or known[name].kind == inspect.Parameter.VAR_KEYWORD]
    if unknown:
        raise TypeError(f"Job parameters not supported by {easyFts.__name__}: {unknown}")
    return easyFts.new_job(**params)


def buildJob(flavour: str, ftsJob: FTS3Job) -> dict:
    """Convert what :py:meth:`~DIRAC.DataManagementSystem.Client.FTS3Job.FTS3Job.prepareSubmission`
    prepared into a job dictionary of the library of the given flavour
    """
    easyFts = _easyFts(flavour)
    transfers = [
        easyFts.new_transfer(
            trans["source"],
            trans["destination"],
            checksum=trans["checksum"],
            filesize=trans["filesize"],
            metadata=trans["metadata"],
            activity=trans["activity"],
            scitag=trans.get("scitag"),
            source_token=trans.get("source_token"),
            destination_token=trans.get("destination_token"),
        )
        for trans in ftsJob.transfers
    ]

    params = {}
    # Space tokens and pin lifetime (SRM) do not exist anymore in FTS4
    if flavour == "FTS3":
        params = {
            "source_spacetoken": ftsJob.sourceSpaceToken,
            "destination_spacetoken": ftsJob.targetSpaceToken,
            "copy_pin_lifetime": ftsJob.copyPinLifetime,
        }

    return _newJob(
        easyFts,
        transfers=transfers,
        overwrite=True,
        disable_cleanup=True,
        bring_online=ftsJob.bringOnline,
        retry=FTS_RETRY,
        verify_checksum="target",  # Only check target vs specified, since we verify the source earlier
        multihop=ftsJob.multihop,
        metadata=ftsJob.jobMetadata,
        priority=ftsJob.priority,
        unmanaged_tokens=True,
        archive_timeout=ftsJob.archiveTimeout,
        **params,
    )


def submit(context, ftsJob: FTS3Job):
    """Submit a job prepared by FTS3Job.prepareSubmission.

    The delegation is managed when the context is created (see :py:class:`FTSContextCache`).
    The library checks it again before submitting (2 queries for X509 contexts, none for tokens),
    with its own default lifetimes: it only re-delegates if the delegation is missing or expired,
    since ours always has much more left than the library threshold.

    :returns: S_OK(job ID)
    """
    flavour = _flavourOf(context)
    easyFts = _easyFts(flavour)
    try:
        job = buildJob(flavour, ftsJob)
        return S_OK(easyFts.submit(context, job))
    except easyFts.exceptions.FTS3ClientException as e:
        return _toError(context, e, "Error at submission")


def getJobStatus(context, ftsGUID: str):
    """:returns: S_OK(job status dictionary, with the files), in the FTS REST format"""
    easyFts = _easyFts(_flavourOf(context))
    try:
        return S_OK(easyFts.get_job_status(context, ftsGUID, list_files=True))
    except easyFts.exceptions.FTS3ClientException as e:
        return _toError(context, e, f"Error getting the status of {ftsGUID}")


def cancel(context, ftsGUID: str):
    """:returns: S_OK(dictionary with at least job_state)"""
    easyFts = _easyFts(_flavourOf(context))
    try:
        return S_OK(easyFts.cancel(context, ftsGUID))
    except easyFts.exceptions.FTS3ClientException as e:
        return _toError(context, e, f"Error canceling {ftsGUID}")


##########################################################################
# Contexts


class FTSContextCache:
    """Contexts to the FTS servers, shared by all the threads of the FTS3Agent during a cycle.

    * One context per server and identity: (user, group) for X509, group for tokens.
    * The credentials are cached by the ProxyManager client (a new proxy gives a new file)
      and by the TokenManager client. A context is re-created when the credential it was made with changes.
    * The delegation of X509 contexts is checked, and renewed if needed, when they are created
      (see :py:func:`delegate`).
      If a server loses it during the cycle, :py:func:`submit` delegates again.

    :py:meth:`clear` has to be called between the agent cycles, when no thread uses the cache.
    """

    def __init__(self, proxyLifetime: int):
        """
        :param proxyLifetime: lifetime (in seconds) of the proxies, and of their delegation to the servers
        """
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.proxyLifetime = proxyLifetime
        self._lock = threading.Lock()
        # (server, identity) -> (credential, context)
        self._contexts: dict[tuple, tuple] = {}

    def _getProxyFile(self, username: str, group: str):
        """:returns: S_OK(path of the proxy file), cached by the ProxyManager client"""
        res = getDNForUsername(username)
        if not res["OK"]:
            return res
        # We take the first DN returned
        userDN = res["Value"][0]
        # The FTS servers renew the delegation when a third of its lifetime is left,
        # so we make our cache a bit less than 2/3rd of the lifetime.
        cacheTime = int(2 * self.proxyLifetime / 3) - 600
        return gProxyManager.downloadVOMSProxyToFile(
            userDN, group, requiredTimeLeft=self.proxyLifetime, cacheTime=cacheTime
        )

    def getContext(self, serverURL: str, username: str, group: str, useTokens: bool = False):
        """Get a context to an FTS server

        :param serverURL: URL of the server
        :param username: user owning the job (for X509)
        :param group: group of the user
        :param useTokens: if True, get a token based context, otherwise an X509 one

        :returns: S_OK(context of the library corresponding to the server flavour)
        """
        if useTokens:
            key = (serverURL, "token", group)
            res = gTokenManager.getToken(userGroup=group, requiredTimeLeft=TOKEN_REQUIRED_TIME_LEFT, scope=["fts"])
            if res["OK"]:
                res = S_OK(res["Value"]["access_token"])
        else:
            key = (serverURL, "x509", username, group)
            res = self._getProxyFile(username, group)
        if not res["OK"]:
            return res
        credential = res["Value"]

        with self._lock:
            cached = self._contexts.get(key)
        if cached and cached[0] == credential:
            return S_OK(cached[1])

        # Created outside of the lock, since it queries the server.
        # Concurrent threads may create the same context: the last one is kept, the others are just dropped
        try:
            if useTokens:
                context = newContext(serverURL, accessToken=credential)
            else:
                context = newContext(serverURL, ucert=credential)
                delegate(context, self.proxyLifetime)
        except Exception as e:
            self.log.error("Could not get a context", f"{key}: {e!r}")
            return S_ERROR(f"Could not get a context to {serverURL}: {e!r}")

        with self._lock:
            self._contexts[key] = (credential, context)
        return S_OK(context)

    def clear(self):
        """Forget all the contexts. Must be called when no other thread uses the cache."""
        with self._lock:
            self.log.info("Forgetting the FTS contexts", f"{len(self._contexts)}")
            self._contexts = {}
