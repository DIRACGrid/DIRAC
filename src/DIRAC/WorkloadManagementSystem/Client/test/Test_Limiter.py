"""Tests for the Limiter: MatchingDelay token buckets and running limits"""

import threading
from unittest import mock

import pytest

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.DictCache import DictCache, TwoLevelCache
from DIRAC.WorkloadManagementSystem.Client import Limiter as limiterModule
from DIRAC.WorkloadManagementSystem.Client.Limiter import Limiter

SITE = "DIRAC.HLTFarm.lhcb"


class FakeOperations:
    """Operations helper serving a nested dict as the JobScheduling section"""

    def __init__(self, tree, flags=None):
        self.tree = {"JobScheduling": tree}
        self.flags = flags or {}

    def _node(self, path):
        node = self.tree
        for part in path.split("/"):
            if not isinstance(node, dict) or part not in node:
                return None
            node = node[part]
        return node

    def getValue(self, path, default=None):
        return self.flags.get(path, default)

    def getSections(self, path):
        node = self._node(path)
        if node is None:
            return S_ERROR(DErrno.ESECTION, f"{path} does not exist")
        return S_OK([k for k, v in node.items() if isinstance(v, dict)])

    def getOptionsDict(self, path):
        node = self._node(path)
        if node is None:
            return S_ERROR(DErrno.ESECTION, f"{path} does not exist")
        return S_OK({k: str(v) for k, v in node.items() if not isinstance(v, dict)})


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.fixture(autouse=True)
def resetLimiterState(monkeypatch):
    """The Limiter keeps its state in class attributes: start each test from scratch"""
    monkeypatch.setattr(Limiter, "csDictCache", DictCache())
    monkeypatch.setattr(Limiter, "condCache", DictCache())
    monkeypatch.setattr(Limiter, "newCache", TwoLevelCache(10, 300))
    monkeypatch.setattr(Limiter, "delayBuckets", {})
    monkeypatch.setattr(
        Limiter, "recentMatches", limiterModule.collections.defaultdict(limiterModule.collections.deque)
    )


@pytest.fixture
def clock(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(limiterModule.time, "monotonic", clock)
    return clock


def makeLimiter(tree, running=None, jobAttributes=None, flags=None):
    jobDB = mock.MagicMock()
    jobDB.jobAttributeNames = ["JobType", "Site", "JobGroup", "Status"]
    counts = running or {}
    jobDB.getCounters.return_value = S_OK([({"JobType": k}, v) for k, v in counts.items()])
    jobDB.getJobAttributes.return_value = S_OK(jobAttributes or {})
    return Limiter(jobDB=jobDB, opsHelper=FakeOperations(tree, flags))


def delayTree(delays, bursts=None):
    site = {"JobType": delays}
    if bursts:
        site["Burst"] = {"JobType": bursts}
    return {"MatchingDelay": {SITE: site}}


def test_delay_without_burst_allows_one_match_per_interval(clock):
    limiter = makeLimiter(delayTree({"MCReconstruction": 10}))

    first = limiter.reserveMatchingSlots(SITE)
    assert first.negativeCond == {}
    first.commit({"JobType": "MCReconstruction"})

    assert limiter.reserveMatchingSlots(SITE).negativeCond == {"JobType": ["MCReconstruction"]}
    clock.now += 9.9
    assert limiter.reserveMatchingSlots(SITE).negativeCond == {"JobType": ["MCReconstruction"]}
    clock.now += 0.1
    assert limiter.reserveMatchingSlots(SITE).negativeCond == {}


def test_burst_allows_several_matches_then_refills_at_the_configured_rate(clock):
    limiter = makeLimiter(delayTree({"MCReconstruction": 0.5}, {"MCReconstruction": 3}))

    for _ in range(3):
        reservation = limiter.reserveMatchingSlots(SITE)
        assert reservation.negativeCond == {}
        reservation.commit({"JobType": "MCReconstruction"})
    assert limiter.reserveMatchingSlots(SITE).negativeCond == {"JobType": ["MCReconstruction"]}

    # One token every 0.5 s, never more than the burst
    clock.now += 0.5
    reservation = limiter.reserveMatchingSlots(SITE)
    assert reservation.negativeCond == {}
    reservation.commit({"JobType": "MCReconstruction"})
    assert limiter.reserveMatchingSlots(SITE).negativeCond == {"JobType": ["MCReconstruction"]}
    clock.now += 100
    assert Limiter.delayBuckets[SITE][("JobType", "MCReconstruction")].tokens < 1
    limiter.reserveMatchingSlots(SITE).release()
    assert Limiter.delayBuckets[SITE][("JobType", "MCReconstruction")].tokens == 3


def test_unused_reservations_are_given_back(clock):
    limiter = makeLimiter(delayTree({"MCReconstruction": 10, "MCSimulation": 10}))

    # No match at all
    limiter.reserveMatchingSlots(SITE).release()
    # A job of another type
    limiter.reserveMatchingSlots(SITE).commit({"JobType": "MCSimulation"})

    assert limiter.reserveMatchingSlots(SITE).negativeCond == {"JobType": ["MCSimulation"]}


def test_concurrent_requests_cannot_share_a_token(clock):
    limiter = makeLimiter(delayTree({"MCReconstruction": 10}))
    barrier = threading.Barrier(50)
    allowed = []

    def request():
        barrier.wait()
        reservation = limiter.reserveMatchingSlots(SITE)
        allowed.append(not reservation.negativeCond)

    threads = [threading.Thread(target=request) for _ in range(50)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert allowed.count(True) == 1


def test_matching_delay_can_be_disabled(clock):
    limiter = makeLimiter(delayTree({"MCReconstruction": 10}), flags={"JobScheduling/CheckMatchingDelay": False})

    for _ in range(3):
        assert limiter.reserveMatchingSlots(SITE).negativeCond == {}


def test_burst_section_is_not_a_job_attribute(clock):
    limiter = makeLimiter(delayTree({"MCReconstruction": 10}, {"MCReconstruction": 2}))
    limiter.log = mock.MagicMock()

    limiter.reserveMatchingSlots(SITE).release()

    assert set(Limiter.delayBuckets[SITE]) == {("JobType", "MCReconstruction")}
    limiter.log.error.assert_not_called()


def test_configuration_changes_are_applied_to_existing_buckets(clock):
    limiter = makeLimiter(delayTree({"MCReconstruction": 10}))
    limiter.reserveMatchingSlots(SITE).commit({"JobType": "MCReconstruction"})

    # The delay is shortened (and the CS cache expires): time already elapsed counts at the
    # old rate, later time at the new one
    limiter._Limiter__opsHelper = FakeOperations(delayTree({"MCReconstruction": 1}))
    Limiter.csDictCache = DictCache()
    assert limiter.reserveMatchingSlots(SITE).negativeCond == {"JobType": ["MCReconstruction"]}
    clock.now += 1

    assert limiter.reserveMatchingSlots(SITE).negativeCond == {}


def test_running_limit_counts_matches_made_since_the_cached_count():
    tree = {"RunningLimit": {SITE: {"JobType": {"MCReconstruction": 10}, "CEs": {}}}}
    limiter = makeLimiter(tree, running={"MCReconstruction": 9})

    assert limiter.getNegativeCondForSite(SITE) == {}

    # A match is made: the site is at its limit even though the cached count says 9
    assert limiter.recordMatch(SITE, 1, knownAtts={"JobType": "MCReconstruction"})["OK"]
    assert limiter.getNegativeCondForSite(SITE) == {"JobType": ["MCReconstruction"]}
    limiter.jobDB.getCounters.assert_called_once()


def test_running_limit_ignores_matches_older_than_the_cached_count(monkeypatch):
    tree = {"RunningLimit": {SITE: {"JobType": {"MCReconstruction": 10}}}}
    limiter = makeLimiter(tree, running={"MCReconstruction": 9})
    monkeypatch.setattr(limiterModule.time, "time", lambda: 100.0)
    limiter.recordMatch(SITE, 1, knownAtts={"JobType": "MCReconstruction"})

    # The count is taken after the match: it already includes it
    monkeypatch.setattr(limiterModule.time, "time", lambda: 200.0)
    assert limiter.getNegativeCondForSite(SITE) == {}


def test_record_match_fetches_missing_attributes(clock):
    tree = {"MatchingDelay": {SITE: {"JobGroup": {"00012345": 10}}}}
    limiter = makeLimiter(tree, jobAttributes={"JobGroup": "00012345"})

    reservation = limiter.reserveMatchingSlots(SITE)
    assert limiter.recordMatch(SITE, 42, reservation, knownAtts={"JobType": "MCReconstruction"})["OK"]

    limiter.jobDB.getJobAttributes.assert_called_once_with(42, ["JobGroup"])
    assert limiter.reserveMatchingSlots(SITE).negativeCond == {"JobGroup": ["00012345"]}


def test_matcher_gives_tokens_back_when_nothing_matches(clock):
    from DIRAC.WorkloadManagementSystem.Client.Matcher import Matcher

    limiter = makeLimiter(delayTree({"MCReconstruction": 10}))
    tqDB = mock.MagicMock()
    tqDB.matchAndGetJob.return_value = S_OK({"matchFound": False})
    matcher = Matcher(
        pilotAgentsDB=mock.MagicMock(),
        jobDB=limiter.jobDB,
        tqDB=tqDB,
        jlDB=mock.MagicMock(),
        opsHelper=mock.MagicMock(),
    )
    matcher.limiter = limiter
    matcher._getResourceDict = mock.Mock(return_value={"Site": SITE})

    assert matcher.selectJob({}, {}) == {}
    assert tqDB.matchAndGetJob.call_args.kwargs["negativeCond"] == {}
    assert limiter.reserveMatchingSlots(SITE).negativeCond == {}
