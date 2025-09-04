from itertools import zip_longest
from diraccfg import CFG

import pytest
from unittest.mock import MagicMock

from DIRAC import gConfig
from DIRAC.ConfigurationSystem.Client import ConfigurationData
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import (
    getDIRACPlatform,
    getCompatiblePlatforms,
    getQueue,
)


mockGCReply = MagicMock()


@pytest.mark.parametrize(
    "mockGCReplyInput, requested, expectedRes, expectedValue",
    [
        ({"OK": False, "Message": "error"}, "plat", False, None),
        ({"OK": True, "Value": ""}, "plat", False, None),
        (
            {"OK": True, "Value": {"plat1": "OS1, OS2,  OS3", "plat2": "OS4, OS5", "plat3": "OS1, OS4"}},
            "plat",
            False,
            None,
        ),
        (
            {"OK": True, "Value": {"plat1": "OS1, OS2,  OS3", "plat2": "OS4, OS5", "plat3": "OS1, OS4"}},
            "OS1",
            True,
            ["plat1", "plat3"],
        ),
        (
            {"OK": True, "Value": {"plat1": "OS1, OS2,  OS3", "plat2": "OS4, OS5", "plat3": "OS1, OS4"}},
            "OS2",
            True,
            ["plat1"],
        ),
        (
            {"OK": True, "Value": {"plat1": "OS1, OS2,  OS3", "plat2": "OS4, OS5", "plat3": "OS1, OS4"}},
            "OS3",
            True,
            ["plat1"],
        ),
        (
            {"OK": True, "Value": {"plat1": "OS1, OS2,  OS3", "plat2": "OS4, OS5", "plat3": "OS1, OS4"}},
            "OS4",
            True,
            ["plat2", "plat3"],
        ),
        (
            {"OK": True, "Value": {"plat1": "OS1, OS2,  OS3", "plat2": "OS4, OS5", "plat3": "OS1, OS4"}},
            "OS5",
            True,
            ["plat2"],
        ),
        (
            {"OK": True, "Value": {"plat1": "OS1, OS2,  OS3", "plat2": "OS4, OS5", "plat3": "OS1, OS4"}},
            "plat1",
            True,
            ["plat1"],
        ),
    ],
)
def test_getDIRACPlatform(mocker, mockGCReplyInput, requested, expectedRes, expectedValue):
    mockGCReply.return_value = mockGCReplyInput

    mocker.patch("DIRAC.Interfaces.API.Dirac.gConfig.getOptionsDict", side_effect=mockGCReply)

    res = getDIRACPlatform(requested)
    assert res["OK"] is expectedRes, res
    if expectedRes:
        assert set(res["Value"]) == set(expectedValue), res["Value"]


@pytest.mark.parametrize(
    "mockGCReplyInput, requested, expectedRes, expectedValue",
    [
        ({"OK": False, "Message": "error"}, "plat", False, None),
        ({"OK": True, "Value": ""}, "plat", False, None),
        (
            {
                "OK": True,
                "Value": {"plat1": "xOS1, xOS2,  xOS3", "plat2": "sys2, xOS4, xOS5", "plat3": "sys1, xOS1, xOS4"},
            },
            "plat",
            True,
            ["plat"],
        ),
        (
            {
                "OK": True,
                "Value": {"plat1": "xOS1, xOS2,  xOS3", "plat2": "sys2, xOS4, xOS5", "plat3": "sys1, xOS1, xOS4"},
            },
            "plat1",
            True,
            ["plat1", "xOS1", "xOS2", "xOS3"],
        ),
    ],
)
def test_getCompatiblePlatforms(mocker, mockGCReplyInput, requested, expectedRes, expectedValue):
    mockGCReply.return_value = mockGCReplyInput

    mocker.patch("DIRAC.Interfaces.API.Dirac.gConfig.getOptionsDict", side_effect=mockGCReply)

    res = getCompatiblePlatforms(requested)
    assert res["OK"] is expectedRes, res
    if expectedRes:
        assert set(res["Value"]) == set(expectedValue), res["Value"]


config = """
Resources
{
    Sites
    {
        LHCb
        {
            LHCb.CERN.cern
            {
                CEs
                {
                    ce1.cern.ch
                    {
                        CEType = AREX
                        architecture = x86_64
                        OS = linux_CentOS_7.9.2009
                        Tag = Token
                        Queues
                        {
                            nordugrid-SLURM-grid
                            {
                                SI00 = 2775
                                MaxRAM = 128534
                                CPUTime = 3836159
                                maxCPUTime = 5760
                                Tag = MultiProcessor
                                MaxWaitingJobs = 10
                                MaxTotalJobs = 200
                                LocalCEType = Pool/Singularity
                                OS = linux_AlmaLinux_9.4.2104
                            }
                        }
                    }
                }
            }
        }
    }
}
"""


def test_getQueue():
    """Test getQueue function."""

    # Set up the configuration file
    ConfigurationData.localCFG = CFG()
    cfg = CFG()
    cfg.loadFromBuffer(config)
    gConfig.loadCFG(cfg)

    # Test getQueue
    site = "LHCb.CERN.cern"
    ce = "ce1.cern.ch"
    queue = "nordugrid-SLURM-grid"

    result = getQueue(site, ce, queue)
    assert result["OK"]

    expectedDict = {
        "CEType": "AREX",
        "Queue": "nordugrid-SLURM-grid",
        "architecture": "x86_64",
        "SI00": "2775",
        "MaxRAM": "128534",
        "CPUTime": "3836159",
        "maxCPUTime": "5760",
        "Tag": ["MultiProcessor", "Token"],
        "MaxWaitingJobs": "10",
        "MaxTotalJobs": "200",
        "LocalCEType": "Pool/Singularity",
        "OS": "linux_AlmaLinux_9.4.2104",
    }
    assert sorted(result["Value"].pop("Tag")) == sorted(expectedDict.pop("Tag"))
    assert result["Value"] == expectedDict
