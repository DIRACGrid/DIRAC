""" pytest(s) for Executors
"""
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


@pytest.mark.parametrize(
    "manifestOptions, expected",
    [
        ({}, []),
        ({"Tag": "bof"}, ["bof"]),
        ({"Tags": "bof, bif"}, ["bof", "bif"]),
        ({"MaxRAM": 2}, ["2GB"]),
        ({"Tags": "bof, bif", "MaxRAM": 2}, ["bof", "bif", "2GB"]),
        ({"WholeNode": "yes", "MaxRAM": 2}, ["WholeNode", "MultiProcessor", "2GB"]),
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
