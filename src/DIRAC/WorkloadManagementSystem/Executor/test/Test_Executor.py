"""pytest(s) for Executors"""

# pylint: disable=protected-access, missing-docstring

from unittest.mock import MagicMock
import pytest

from DIRAC import gLogger

from DIRAC.WorkloadManagementSystem.Client.JobState.CachedJobState import CachedJobState
from DIRAC.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest

# sut
from DIRAC.WorkloadManagementSystem.Executor.JobScheduling import JobScheduling
from DIRAC.WorkloadManagementSystem.Executor.InputData import InputData

mockNone = MagicMock()
mockNone.return_value = None


# JobScheduling


@pytest.mark.parametrize(
    "sites, banned, expected",
    [
        (["MY.Site1.org", "MY.Site2.org"], None, ["MY.Site1.org", "MY.Site2.org"]),
        (["MY.Site1.org", "MY.Site2.org"], ["MY.Site1.org", "MY.Site2.org"], []),
        (["MY.Site1.org", "MY.Site2.org"], ["MY.Site2.org"], ["MY.Site1.org"]),
        (["MY.Site1.org", "MY.Site2.org"], [], ["MY.Site1.org", "MY.Site2.org"]),
        ([], ["MY.Site1.org"], []),
        ([], [], []),
        (["MY.Site1.org", "MY.Site2.org"], ["MY.Site1.org"], ["MY.Site2.org"]),
        (["MY.Site1.org", "MY.Site2.org"], ["MY.Site1.org", "MY.Site3.org"], ["MY.Site2.org"]),
        ([], ["MY.Site1.org", "MY.Site3.org"], []),
        (["MY.Site1.org", "MY.Site2.org"], ["MY.Site4.org"], ["MY.Site1.org", "MY.Site2.org"]),
        (
            ["MY.Site1.org", "MY.Site2.org", "MY.Site3.org"],
            ["MY.Site4.org"],
            ["MY.Site1.org", "MY.Site2.org", "MY.Site3.org"],
        ),
        (["MY.Site1.org", "MY.Site2.org"], ["MY.Site4.org"], ["MY.Site1.org", "MY.Site2.org"]),
    ],
)
def test__applySiteFilter(sites, banned, expected):
    js = JobScheduling()
    filtered = js._applySiteFilter(sites, banned)
    assert set(filtered) == set(expected)


BACKOFF = {"MaxHoldTime": 6 * 3600}


@pytest.mark.parametrize(
    "storedCount, options, expectedDelay",
    [
        (None, BACKOFF, 300),
        (0, BACKOFF, 300),
        (1, BACKOFF, 600),
        (3, BACKOFF, 2400),
        (6, BACKOFF, 19200),
        (7, BACKOFF, 6 * 3600),
        (1000, BACKOFF, 6 * 3600),
        ("garbage", BACKOFF, 300),
        (2, {"HoldTime": 60, "MaxHoldTime": 200}, 200),
    ],
)
def test__nextHoldTime(storedCount, options, expectedDelay):
    js = JobScheduling()
    stored = {}
    if storedCount is None:
        js.retrieveOptimizerParam = MagicMock(return_value={"OK": False, "Message": "Parameter not found"})
    else:
        js.retrieveOptimizerParam = MagicMock(return_value={"OK": True, "Value": storedCount})
    js.storeOptimizerParam = MagicMock(side_effect=lambda name, value: stored.update({name: value}))
    js.ex_getOption = MagicMock(side_effect=lambda name, default=None: options.get(name, default))

    assert js._JobScheduling__nextHoldTime() == expectedDelay
    expectedCount = storedCount + 1 if isinstance(storedCount, int) else 1
    assert stored == {"HoldCount": expectedCount}


@pytest.mark.parametrize(
    "options, expectedDelay",
    [({}, 300), ({"HoldTime": 120}, 120), ({"HoldTime": 600, "MaxHoldTime": 100}, 600)],
)
def test__nextHoldTime_noBackoff(options, expectedDelay):
    """Without a MaxHoldTime above HoldTime, every hold lasts HoldTime and no counter is kept"""
    js = JobScheduling()
    js.retrieveOptimizerParam = MagicMock(return_value={"OK": True, "Value": 5})
    js.storeOptimizerParam = MagicMock()
    js.ex_getOption = MagicMock(side_effect=lambda name, default=None: options.get(name, default))

    assert js._JobScheduling__nextHoldTime() == expectedDelay
    js.retrieveOptimizerParam.assert_not_called()
    js.storeOptimizerParam.assert_not_called()


@pytest.mark.parametrize(
    "manifestOptions, expected",
    [
        ({}, []),
        ({"Tag": "bof"}, ["bof"]),
        ({"Tags": "bof, bif"}, ["bof", "bif"]),
        ({"WholeNode": "yes"}, ["WholeNode", "MultiProcessor"]),
        ({"NumberOfProcessors": 1}, []),
        ({"NumberOfProcessors": 4}, ["MultiProcessor", "4Processors"]),
        ({"NumberOfProcessors": 4, "MinNumberOfProcessors": 2}, ["MultiProcessor", "4Processors"]),
        ({"NumberOfProcessors": 4, "MaxNumberOfProcessors": 12}, ["MultiProcessor", "4Processors"]),
        ({"NumberOfProcessors": 4, "MaxNumberOfProcessors": 12}, ["MultiProcessor", "4Processors"]),
        ({"MinNumberOfProcessors": 4, "MaxNumberOfProcessors": 12}, ["MultiProcessor", "4Processors"]),
        ({"MinNumberOfProcessors": 4, "MaxNumberOfProcessors": 4}, ["MultiProcessor", "4Processors"]),
        ({"MinNumberOfProcessors": 4}, ["MultiProcessor", "4Processors"]),
    ],
)
def test__getTagsFromManifest(manifestOptions, expected):
    manifest = JobManifest()
    for varName, varValue in manifestOptions.items():
        manifest.setOption(varName, varValue)

    js = JobScheduling()
    tagList = js._getTagsFromManifest(manifest)
    assert set(tagList) == set(expected)


# InputData


@pytest.mark.parametrize(
    "manifestOptions, expected",
    [
        ({}, []),
        ({"InputSandbox": "SB:ProductionSandboxSE|/SandBox/l/lhcb_mc/4c7.bof"}, []),
        ({"InputSandbox": "LFN:/l/lhcb_mc/4c7.bof"}, ["/l/lhcb_mc/4c7.bof"]),
        (
            {"InputSandbox": ["SB:ProductionSandboxSE|/SandBox/l/lhcb_mc/4c7.bof", "LFN:/l/lhcb_mc/4c7.bof"]},
            ["/l/lhcb_mc/4c7.bof"],
        ),
        ({"InputSandbox": ["SB:ProductionSandboxSE|/SandBox/l/lhcb_mc/4c7.bof", "bif:/l/lhcb_mc/4c7.bof"]}, []),
        (
            {"InputSandbox": ["LFN:/l/lhcb_mc/4c8.bof", "LFN:/l/lhcb_mc/4c7.bof"]},
            ["/l/lhcb_mc/4c8.bof", "/l/lhcb_mc/4c7.bof"],
        ),
    ],
)
def test__getInputSandbox(mocker, manifestOptions, expected):
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.JobState.JobState.JobDB.__init__", side_effect=mockNone)
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.JobState.JobState.JobLoggingDB.__init__", side_effect=mockNone)
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.JobState.JobState.TaskQueueDB.__init__", side_effect=mockNone)

    inputData = InputData()
    js = CachedJobState(1)
    js.setAttribute("JobType", "User")

    manifest = JobManifest()
    for varName, varValue in manifestOptions.items():
        manifest.setOption(varName, varValue)

    js.setManifest(manifest)
    res = inputData._getInputSandbox(js)
    assert res["OK"] is True
    assert res["Value"] == expected


@pytest.mark.parametrize(
    "okReplicas, getSitesForSE_RV, storageGetStatus_RV, expectedRes, expectedValue",
    [
        ({}, None, None, False, None),
        (
            {"lfn_1": ["SE_1"]},
            {"OK": True, "Value": ["Site_1"]},
            {"OK": True, "Value": {"DiskSE": True, "TapeSE": False}},
            True,
            {"Site_1": {"disk": 1, "tape": 0}},
        ),
        (
            {"lfn_1": ["SE_2"]},
            {"OK": True, "Value": ["Site_1"]},
            {"OK": True, "Value": {"DiskSE": False, "TapeSE": True}},
            True,
            {"Site_1": {"disk": 0, "tape": 1}},
        ),
        (
            {"lfn_1": ["SE_1"]},
            {"OK": True, "Value": ["Site_1", "Site_2"]},
            {"OK": True, "Value": {"DiskSE": True, "TapeSE": False}},
            True,
            {"Site_1": {"disk": 1, "tape": 0}, "Site_2": {"disk": 1, "tape": 0}},
        ),
        (
            {"lfn_1": ["SE_1"], "lfn_2": ["SE_1"]},
            {"OK": True, "Value": ["Site_1", "Site_2"]},
            {"OK": True, "Value": {"DiskSE": True, "TapeSE": False}},
            True,
            {"Site_1": {"disk": 2, "tape": 0}, "Site_2": {"disk": 2, "tape": 0}},
        ),
        (
            {"lfn_1": ["SE_1"], "lfn_2": ["SE_2"]},
            {"OK": True, "Value": ["Site_1", "Site_2"]},
            {"OK": True, "Value": {"DiskSE": False, "TapeSE": True}},
            True,
            {"Site_1": {"disk": 1, "tape": 1}, "Site_2": {"disk": 1, "tape": 1}},
        ),
        (
            {"lfn_1": ["SE_1"], "lfn_2": ["SE_2", "SE_3"]},
            {"OK": True, "Value": ["Site_1", "Site_2"]},
            {"OK": True, "Value": {"DiskSE": True, "TapeSE": False}},
            True,
            {"Site_1": {"disk": 2, "tape": 1}, "Site_2": {"disk": 2, "tape": 1}},
        ),
        (
            {"lfn_1": ["SE_1"], "lfn_2": ["SE_1", "SE_2"]},
            {"OK": True, "Value": ["Site_1", "Site_2"]},
            {"OK": True, "Value": {"DiskSE": True, "TapeSE": False}},
            True,
            {"Site_1": {"disk": 2, "tape": 1}, "Site_2": {"disk": 2, "tape": 1}},
        ),
    ],
)
def test__getSiteCandidates(mocker, okReplicas, getSitesForSE_RV, storageGetStatus_RV, expectedRes, expectedValue):
    mockSE = MagicMock()
    mockSE.getStatus.return_value = storageGetStatus_RV

    mocker.patch("DIRAC.WorkloadManagementSystem.Client.JobState.JobState.JobDB.__init__", side_effect=mockNone)
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.JobState.JobState.JobLoggingDB.__init__", side_effect=mockNone)
    mocker.patch("DIRAC.WorkloadManagementSystem.Client.JobState.JobState.TaskQueueDB.__init__", side_effect=mockNone)
    mocker.patch(
        "DIRAC.DataManagementSystem.Utilities.DMSHelpers.DMSHelpers.getSitesForSE", return_value=getSitesForSE_RV
    )
    mocker.patch("DIRAC.Resources.Storage.StorageElement.StorageElementItem", return_value=mockSE)

    inputData = InputData()
    inputData.log = gLogger
    # inputData.jobLog = gLogger
    res = inputData._getSiteCandidates(okReplicas, "vo")
    assert res["OK"] is expectedRes
    if res["OK"]:
        assert res["Value"] == expectedValue
