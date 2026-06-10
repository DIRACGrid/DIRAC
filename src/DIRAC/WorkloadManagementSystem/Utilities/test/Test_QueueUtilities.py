""" Test class for QueueUtilities
"""
import copy
from unittest.mock import MagicMock

import pytest
from DIRAC import S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.Utilities.QueueUtilities import *

siteDict1 = {
    "Site1": {
        "CE1": {"Queues": {"Queue1": {}, "Queue2": {}}, "CEType": "Type1"},
        "CE2": {"Queues": {"Queue1": {}}, "CEType": "Type2"},
    },
    "Site2": {"CE3": {"Queues": {"Queue1": {"NumberOfProcessors": 3}}, "CEType": "Type2"}},
}


expectedQueueDict1 = {
    "CE1_Queue1": {
        "CEName": "CE1",
        "CEType": "Type1",
        "QueueName": "Queue1",
        "Site": "Site1",
        "ParametersDict": {
            "GridCE": "CE1",
            "Queue": "Queue1",
            "RequiredTag": [],
            "Setup": None,
            "Site": "Site1",
            "Tag": [],
            "WorkingDirectory": "Queue1",
        },
    },
    "CE1_Queue2": {
        "CEName": "CE1",
        "CEType": "Type1",
        "QueueName": "Queue2",
        "Site": "Site1",
        "ParametersDict": {
            "GridCE": "CE1",
            "Queue": "Queue2",
            "RequiredTag": [],
            "Setup": None,
            "Site": "Site1",
            "Tag": [],
            "WorkingDirectory": "Queue2",
        },
    },
    "CE2_Queue1": {
        "CEName": "CE2",
        "CEType": "Type2",
        "QueueName": "Queue1",
        "Site": "Site1",
        "ParametersDict": {
            "GridCE": "CE2",
            "Queue": "Queue1",
            "RequiredTag": [],
            "Setup": None,
            "Site": "Site1",
            "Tag": [],
            "WorkingDirectory": "Queue1",
        },
    },
    "CE3_Queue1": {
        "CEName": "CE3",
        "CEType": "Type2",
        "QueueName": "Queue1",
        "Site": "Site2",
        "ParametersDict": {
            "GridCE": "CE3",
            "Queue": "Queue1",
            "RequiredTag": [],
            "Setup": None,
            "Site": "Site1",
            "Tag": ["MultiProcessor"],
            "NumberOfProcessors": 3,
            "WorkingDirectory": "Queue1",
        },
    },
}


@pytest.mark.parametrize(
    "queueDict, dictExpected",
    [
        ({}, {}),
        ({"notUsefulParam": ""}, {"notUsefulParam": ""}),
        ({"maxCPUTime": 45000}, {"maxCPUTime": 45000}),
        ({"SI00": 54}, {"SI00": 54}),
        ({"maxCPUTime": 45000, "SI00": 54}, {"maxCPUTime": 45000, "SI00": 54, "CPUTime": 583200}),
    ],
)
def test_computeQueueCPULimit(queueDict, dictExpected):
    """Test the computeCPULimit function"""
    computeQueueCPULimit(queueDict)
    assert queueDict == dictExpected


@pytest.mark.parametrize(
    "ceDict, queueDict, dictExpected",
    [
        ({}, {}, {"Tag": [], "RequiredTag": []}),
        ({"notUsefulParam": ""}, {"notUsefulParam": ""}, {"Tag": [], "RequiredTag": []}),
        ({"Tag": "Test"}, {}, {"Tag": ["Test"], "RequiredTag": []}),
        ({}, {"Tag": "Test"}, {"Tag": ["Test"], "RequiredTag": []}),
        ({"Tag": "Test"}, {"Tag": "Test"}, {"Tag": ["Test"], "RequiredTag": []}),
        ({"Tag": "Test1"}, {"Tag": "Test2"}, {"Tag": ["Test1", "Test2"], "RequiredTag": []}),
        ({"Tag": ["Test1", "Test2"]}, {"Tag": ["Test2", "Test2"]}, {"Tag": ["Test1", "Test2"], "RequiredTag": []}),
        ({"RequiredTag": "Test"}, {}, {"Tag": [], "RequiredTag": ["Test"]}),
        ({}, {"RequiredTag": "Test"}, {"Tag": [], "RequiredTag": ["Test"]}),
        ({"RequiredTag": "Test"}, {"RequiredTag": "Test"}, {"Tag": [], "RequiredTag": ["Test"]}),
        ({"RequiredTag": "Test1"}, {"RequiredTag": "Test2"}, {"Tag": [], "RequiredTag": ["Test1", "Test2"]}),
        (
            {"Tag": ["Test1", "Test2"], "RequiredTag": ["Test2"]},
            {"Tag": ["Test1", "Test3"], "RequiredTag": ["Test2", "Test3"]},
            {"Tag": ["Test1", "Test2", "Test3"], "RequiredTag": ["Test2", "Test3"]},
        ),
    ],
)
def test_resolveTags(ceDict, queueDict, dictExpected):
    """Test the resolveTags function"""
    resolveTags(ceDict, queueDict)
    assert queueDict["Tag"].sort() == dictExpected["Tag"].sort()
    assert queueDict["RequiredTag"].sort() == dictExpected["RequiredTag"].sort()


@pytest.mark.parametrize(
    "ceDict, queueDict, dictExpected",
    [
        ({}, {}, {}),
        ({"Platform": "x86_64"}, {}, {"Platform": "x86_64"}),
        ({"Platform": "notexist"}, {}, {"Platform": "notexist"}),
        ({}, {"Platform": "x86_64"}, {"Platform": "x86_64"}),
        ({}, {"Platform": "notexist"}, {"Platform": "notexist"}),
        ({"Platform": "x86_64", "OS": "centos"}, {}, {"Platform": "x86_64"}),
        ({"Platform": "notexist", "OS": "centos"}, {}, {"Platform": "notexist"}),
        ({}, {"Platform": "x86_64", "OS": "centos"}, {"Platform": "x86_64", "OS": "centos"}),
        ({}, {"Platform": "notexist", "OS": "centos"}, {"Platform": "notexist", "OS": "centos"}),
    ],
)
def test_setPlatform(ceDict, queueDict, dictExpected):
    """Test the setPlatform function"""
    setPlatform(ceDict, queueDict)
    assert queueDict == dictExpected


@pytest.mark.parametrize(
    "queueDict, queuesExpected",
    [
        (siteDict1, expectedQueueDict1),
    ],
)
def test_getQueuesResolved(mocker, queueDict, queuesExpected):
    """Test the getQueuesResolvedEnhanced function"""
    queueCECache = QueueCECache()
    queueDictLocal = copy.deepcopy(queueDict)

    ce = MagicMock()
    ce.isValid = MagicMock(return_value=S_OK())
    ceFactoryModule = "DIRAC.Resources.Computing.ComputingElementFactory"
    mocker.patch(f"{ceFactoryModule}.ComputingElementFactory.getCE", return_value=S_OK(ce))
    queueDictResolved = getQueuesResolved(queueDictLocal, queueCECache)

    assert queueDictResolved["OK"]
    for qName, qDictResolved in queueDictResolved["Value"].items():
        assert sorted(qDictResolved) == sorted(queuesExpected[qName])


# Target used to patch the CE factory used internally by QueueCECache.
# The factory is set to return a DIFFERENT CE per build (side_effect=[ce1, ce2, ...]),
# so that *which* CE we get back distinguishes "served from cache" (ce1 again) from
# "rebuilt" (ce2). That, plus the factory call_count, is what proves the cache logic --
# asserting we get back the value the mock returned would prove nothing.
GET_CE = "DIRAC.Resources.Computing.ComputingElementFactory.ComputingElementFactory.getCE"


def test_getQueuesResolved_acceptsLegacyDict(mocker):
    """Backward compatibility: a plain dict cache is accepted and adopted as the
    backing store, so legacy callers passing ``{}`` keep working (and keep reuse)."""
    ce = MagicMock()
    ce.isValid = MagicMock(return_value=S_OK())
    ce.ceParameters = {}
    mocker.patch(GET_CE, return_value=S_OK(ce))

    legacyCache = {}
    result = getQueuesResolved(copy.deepcopy(siteDict1), legacyCache, instantiateCEs=True)

    assert result["OK"]
    # The plain dict was adopted as the cache backing store: entries were written into it.
    assert legacyCache
    assert all("CE" in entry and "Hash" in entry for entry in legacyCache.values())


def test_QueueCECache_cacheHitDoesNotRebuild(mocker):
    """A second call with unchanged parameters reuses the cached CE instead of rebuilding."""
    ce1, ce2 = MagicMock(), MagicMock()
    getCEMock = mocker.patch(GET_CE, side_effect=[S_OK(ce1), S_OK(ce2)])

    cache = QueueCECache()
    params = {"CEType": "SSH", "Host": "host1"}

    first = cache.getCE("queue1", "SSH", "ce1", params)
    second = cache.getCE("queue1", "SSH", "ce1", params)

    # Factory invoked exactly once, with the forwarded arguments
    getCEMock.assert_called_once_with(ceType="SSH", ceName="ce1", ceParametersDict=params)
    # Were the cache broken, the 2nd call would rebuild and hand back ce2 instead of ce1
    assert first["Value"] is ce1
    assert second["Value"] is ce1


def test_QueueCECache_parameterChangeRebuilds(mocker):
    """Changed parameters rebuild the CE (new hash) and hand back the fresh one."""
    ce1, ce2 = MagicMock(), MagicMock()
    getCEMock = mocker.patch(GET_CE, side_effect=[S_OK(ce1), S_OK(ce2)])

    cache = QueueCECache()
    first = cache.getCE("queue1", "SSH", "ce1", {"Host": "host1"})
    second = cache.getCE("queue1", "SSH", "ce1", {"Host": "host2"})

    assert getCEMock.call_count == 2  # rebuilt because the parameter hash changed
    assert first["Value"] is ce1
    assert second["Value"] is ce2  # the new CE, not the stale cached one


def test_QueueCECache_dropForcesRebuild(mocker):
    """drop() evicts the cached CE, so the next call rebuilds a fresh one."""
    ce1, ce2 = MagicMock(), MagicMock()
    mocker.patch(GET_CE, side_effect=[S_OK(ce1), S_OK(ce2)])

    cache = QueueCECache()
    params = {"Host": "host1"}

    first = cache.getCE("queue1", "SSH", "ce1", params)
    assert first["Value"] is ce1
    cache.drop("queue1")

    rebuilt = cache.getCE("queue1", "SSH", "ce1", params)
    assert rebuilt["Value"] is ce2  # cache miss after drop -> a fresh CE was built


def test_QueueCECache_failedBuildIsNotCached(mocker):
    """A failed build leaves no cache entry, so a later call retries rather than re-returning the error."""
    ceOK = MagicMock()
    getCEMock = mocker.patch(GET_CE, side_effect=[S_ERROR("boom"), S_OK(ceOK)])

    cache = QueueCECache()
    params = {"Host": "host1"}

    failed = cache.getCE("queue1", "SSH", "ce1", params)
    assert not failed["OK"]

    retried = cache.getCE("queue1", "SSH", "ce1", params)
    assert retried["OK"]
    assert retried["Value"] is ceOK
    assert getCEMock.call_count == 2  # the failure cached nothing, so the 2nd call rebuilt


def test_QueueCECache_dropMissingKeyIsNoOp():
    """drop() on an unknown queue key does nothing and does not raise."""
    cache = QueueCECache()
    cache.drop("does-not-exist")  # must not raise


def test_QueueCECache_dropToleratesCEShutdownFailure(mocker):
    """Evicting a CE must never break the cache, even if tearing the CE down fails.

    The agent loop relies on drop()/rebuild always succeeding; a CE that errors
    while releasing its connection must not propagate and must still be evicted.
    """
    ce = MagicMock()
    ce.shutdown.side_effect = RuntimeError("boom")
    mocker.patch(GET_CE, return_value=S_OK(ce))

    cache = QueueCECache()
    cache.getCE("queue1", "SSH", "ce1", {"Host": "host1"})
    cache.drop("queue1")  # must not raise

    # The entry is gone, so the next call rebuilds rather than serving the dead CE
    assert cache.getCE("queue1", "SSH", "ce1", {"Host": "host1"})["OK"]
