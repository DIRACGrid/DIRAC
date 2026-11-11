"""Tests of the FTSRestLibs module, with fake client libraries"""

import errno
import threading
import time
from datetime import timedelta
from types import SimpleNamespace

import pytest

from DIRAC import S_OK
from DIRAC.Core.Utilities.DErrno import cmpError
from DIRAC.DataManagementSystem.private import FTSRestLibs
from DIRAC.DataManagementSystem.private.FTSRestLibs import FTSContextCache

# pylint: disable=redefined-outer-name,protected-access

SERVERS = {
    "FTS3": {"CERN-FTS3": "https://fts3.cern.ch:8446", "RAL-FTS3": "https://fts3.ral.uk:8446/"},
    "FTS4": {"CERN-FTS4": "https://fts4.cern.ch:8446"},
}
FTS3_URL = SERVERS["FTS3"]["CERN-FTS3"]
FTS4_URL = SERVERS["FTS4"]["CERN-FTS4"]
PROXY_LIFETIME = 36 * 3600
LIFETIME = timedelta(seconds=PROXY_LIFETIME)


class FakeExceptions:
    class FTS3ClientException(Exception):
        pass

    class NotFound(FTS3ClientException):
        pass


def makeFakeEasyFts(flavour):
    """A fake ``easy`` module, recording the contexts created, the delegations and the submissions"""
    easyFts = SimpleNamespace(
        __name__=f"fake_{flavour}",
        opened=[],
        delegations=[],
        submissions=[],
        failOpen=False,
        failDelegation=False,
        exceptions=FakeExceptions,
    )
    lock = threading.Lock()

    class Context:
        def __init__(self, endpoint, ucert=None, fts_access_token=None, verify=True):
            if easyFts.failOpen:
                raise FakeExceptions.FTS3ClientException("boom")
            # Make the race window bigger for the threading test
            time.sleep(0.01)
            self.endpoint = endpoint
            self.ucert = ucert
            self.access_method = "oauth2" if fts_access_token else "X509"
            with lock:
                easyFts.opened.append((endpoint, ucert, fts_access_token))

    def delegate(context, lifetime, delegate_when_lifetime_lt):
        if easyFts.failDelegation:
            raise FakeExceptions.FTS3ClientException("cannot delegate")
        with lock:
            easyFts.delegations.append((context.endpoint, context.ucert, lifetime, delegate_when_lifetime_lt))

    def submit(context, job):
        easyFts.submissions.append((context, job))
        return "myGUID"

    def get_job_status(context, ftsGUID, list_files):
        raise FakeExceptions.NotFound("not there")

    def cancel(context, ftsGUID):
        raise FakeExceptions.FTS3ClientException("cannot cancel")

    easyFts.Context = Context
    easyFts.delegate = delegate
    easyFts.submit = submit
    easyFts.get_job_status = get_job_status
    easyFts.cancel = cancel
    return easyFts


class FakeProxyManager:
    """Mimics the cache of the ProxyManager client: same file until there is a new proxy"""

    def __init__(self):
        self.version = 0

    def downloadVOMSProxyToFile(self, userDN, group, requiredTimeLeft, cacheTime):
        return S_OK(f"/proxies/{userDN.split('=')[1]}_{group}_{self.version}.pem")


class FakeTokenManager:
    def __init__(self):
        self.token = "token1"

    def getToken(self, userGroup, requiredTimeLeft, scope):
        return S_OK({"access_token": f"{self.token}_{userGroup}"})


@pytest.fixture
def env(monkeypatch):
    libs = {flavour: makeFakeEasyFts(flavour) for flavour in ("FTS3", "FTS4")}
    proxyManager = FakeProxyManager()
    tokenManager = FakeTokenManager()
    monkeypatch.setattr(FTSRestLibs, "EASY_FTS", libs)
    monkeypatch.setattr(FTSRestLibs, "getFTSServersByFlavour", lambda: S_OK(SERVERS))
    monkeypatch.setattr(FTSRestLibs, "gProxyManager", proxyManager)
    monkeypatch.setattr(FTSRestLibs, "gTokenManager", tokenManager)
    monkeypatch.setattr(FTSRestLibs, "getDNForUsername", lambda username: S_OK([f"/DN={username}"]))
    return SimpleNamespace(
        cache=FTSContextCache(proxyLifetime=PROXY_LIFETIME),
        libs=libs,
        proxyManager=proxyManager,
        tokenManager=tokenManager,
    )


def test_getFlavour(env):
    assert FTSRestLibs.getFlavour(FTS4_URL) == "FTS4"
    # Trailing slashes are ignored
    assert FTSRestLibs.getFlavour(FTS4_URL + "/") == "FTS4"
    assert FTSRestLibs.getFlavour("https://fts3.ral.uk:8446") == "FTS3"
    # Unknown servers are assumed to be FTS3
    assert FTSRestLibs.getFlavour("https://old.server:8446") == "FTS3"


def test_unknownFlavour(env):
    with pytest.raises(ValueError):
        FTSRestLibs._easyFts("FTS5")
    # Library not installed
    env.libs["FTS4"] = None
    with pytest.raises(ValueError):
        FTSRestLibs._easyFts("FTS4")


def test_libraryPerFlavour(env):
    fts3Context = env.cache.getContext(FTS3_URL, "user", "group")["Value"]
    fts4Context = env.cache.getContext(FTS4_URL, "user", "group")["Value"]
    assert isinstance(fts3Context, env.libs["FTS3"].Context)
    assert isinstance(fts4Context, env.libs["FTS4"].Context)
    assert FTSRestLibs._flavourOf(fts4Context) == "FTS4"
    with pytest.raises(ValueError):
        FTSRestLibs._flavourOf(object())


def test_x509ContextReused(env):
    lib = env.libs["FTS3"]
    res = env.cache.getContext(FTS3_URL, "user", "group")
    assert res["OK"], res
    context = res["Value"]

    # The delegation is checked when the context is created, with the proxy lifetime,
    # to be renewed when a third is left
    assert lib.delegations == [(FTS3_URL, context.ucert, LIFETIME, LIFETIME / 3)]

    assert env.cache.getContext(FTS3_URL, "user", "group")["Value"] is context
    assert len(lib.opened) == 1
    assert len(lib.delegations) == 1

    # Another server shares the proxy file, but has its own context and delegation
    other = env.cache.getContext(SERVERS["FTS3"]["RAL-FTS3"], "user", "group")["Value"]
    assert other is not context
    assert other.ucert == context.ucert
    assert len(lib.delegations) == 2

    # Another group gets its own proxy
    assert env.cache.getContext(FTS3_URL, "user", "group2")["Value"].ucert != context.ucert


def test_newProxy(env):
    first = env.cache.getContext(FTS3_URL, "user", "group")["Value"]
    # The ProxyManager gives a new file: new context
    env.proxyManager.version += 1
    second = env.cache.getContext(FTS3_URL, "user", "group")["Value"]
    assert second is not first
    assert second.ucert != first.ucert
    assert env.libs["FTS3"].delegations[-1][1] == second.ucert
    # And it replaces the old one
    assert env.cache.getContext(FTS3_URL, "user", "group")["Value"] is second


def test_tokenContexts(env):
    lib = env.libs["FTS4"]
    first = env.cache.getContext(FTS4_URL, "user", "group", useTokens=True)["Value"]
    assert FTSRestLibs.usesTokens(first)
    # Shared between users of the same group, and no delegation
    assert env.cache.getContext(FTS4_URL, "user2", "group", useTokens=True)["Value"] is first
    assert lib.delegations == []

    # New token, new context
    env.tokenManager.token = "token2"
    second = env.cache.getContext(FTS4_URL, "user", "group", useTokens=True)["Value"]
    assert second is not first
    assert lib.opened[-1] == (FTS4_URL, None, "token2_group")

    # X509 and token contexts are distinct
    assert not FTSRestLibs.usesTokens(env.cache.getContext(FTS4_URL, "user", "group")["Value"])


def test_concurrentAccess(env):
    """Concurrent threads all get a valid context, and only one is kept"""
    contexts = []

    def worker():
        contexts.append(env.cache.getContext(FTS3_URL, "user", "group")["Value"])

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(contexts) == 10
    assert all(c.endpoint == FTS3_URL for c in contexts)
    assert len(env.cache._contexts) == 1
    kept = env.cache.getContext(FTS3_URL, "user", "group")["Value"]
    assert kept in contexts


def test_clear(env):
    first = env.cache.getContext(FTS3_URL, "user", "group")["Value"]
    env.cache.clear()
    assert env.cache._contexts == {}
    assert env.cache.getContext(FTS3_URL, "user", "group")["Value"] is not first


def test_contextError(env):
    env.libs["FTS3"].failOpen = True
    assert not env.cache.getContext(FTS3_URL, "user", "group")["OK"]


def test_delegationError(env):
    """A context which cannot be delegated is not returned, nor cached"""
    env.libs["FTS3"].failDelegation = True
    assert not env.cache.getContext(FTS3_URL, "user", "group")["OK"]
    assert env.cache._contexts == {}


def test_submit(env, monkeypatch):
    """The job built for the flavour of the context is submitted"""
    job = {"files": ["f1"], "delete": None, "params": {"priority": 3}}
    monkeypatch.setattr(FTSRestLibs, "buildJob", lambda flavour, ftsJob: job if flavour == "FTS4" else None)
    lib = env.libs["FTS4"]
    context = env.cache.getContext(FTS4_URL, "user", "group")["Value"]
    res = FTSRestLibs.submit(context, None)
    assert res["OK"], res
    assert res["Value"] == "myGUID"
    assert lib.submissions == [(context, job)]


def test_errors(env):
    """The exceptions of the library are converted to S_ERROR"""
    context = env.cache.getContext(FTS3_URL, "user", "group")["Value"]
    assert cmpError(FTSRestLibs.getJobStatus(context, "guid"), errno.ESRCH)
    res = FTSRestLibs.cancel(context, "guid")
    assert not res["OK"]
    assert "cannot cancel" in res["Message"]
