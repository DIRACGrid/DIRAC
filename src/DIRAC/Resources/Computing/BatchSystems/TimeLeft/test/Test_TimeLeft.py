""" Test TimeLeft utility
"""
from diraccfg import CFG
import pytest
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client import ConfigurationData
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft import TimeLeft


CONFIG_UNKNOWN = """
LocalSite {
    BatchSystemInfo
    {
        Type = Unknown
        JobID = 12345
        Parameters {
            BinaryPath = Unknown
            Host = Unknown
            InfoPath = Unknown
            Queue = Unknown
        }
    }
}
"""

CONFIG_SLURM = """
LocalSite {
    BatchSystemInfo
    {
        Type = SLURM
        JobID = 12345
        Parameters {
            BinaryPath = Unknown
            Host = Unknown
            InfoPath = Unknown
            Queue = Unknown
        }
    }
}
"""


@pytest.fixture
def configUnknown():
    """Load a fake configuration"""
    ConfigurationData.localCFG = CFG()
    cfg = CFG()
    cfg.loadFromBuffer(CONFIG_UNKNOWN)
    gConfig.loadCFG(cfg)


@pytest.fixture
def configSlurm():
    """Load a fake configuration"""
    ConfigurationData.localCFG = CFG()
    cfg = CFG()
    cfg.loadFromBuffer(CONFIG_SLURM)
    gConfig.loadCFG(cfg)


def test_cpuPowerNotDefined(configSlurm):
    """Test cpuPower not defined"""
    tl = TimeLeft()
    res = tl.getTimeLeft()
    assert not res["OK"]
    assert "/LocalSite/CPUNormalizationFactor not defined" in res["Message"]


def test_batchSystemInfoNotDefined(mocker):
    """Test batch system info not defined"""
    mocker.patch(
        "DIRAC.gConfig.getOptionsDictRecursively", return_value=S_ERROR("Path does not exist or it's not a section")
    )

    tl = TimeLeft()
    tl.cpuPower = 10
    res = tl.getTimeLeft()
    assert not res["OK"]
    assert "Path does not exist or it's not a section" in res["Message"]


def test_batchSystemTypeNotDefined(mocker, configUnknown):
    """Test batch system type not defined"""
    tl = TimeLeft()
    tl.cpuPower = 10
    res = tl.getTimeLeft()
    assert not res["OK"]
    assert "Current batch system is not supported" in res["Message"]


def test_getScaledCPU(mocker, configSlurm):
    """Test getScaledCPU()"""
    mocker.patch(
        "DIRAC.Resources.Computing.BatchSystems.TimeLeft.SLURMResourceUsage.runCommand",
        return_value=S_OK("19283,9000,10,900,30:00"),
    )

    tl = TimeLeft()

    # Test 1: no normalization
    res = tl.getScaledCPU()
    assert res == 0

    # Test 2: normalization
    tl.cpuPower = 5.0
    res = tl.getScaledCPU()
    assert res == 45000


def test_getTimeLeft(mocker, configSlurm):
    """Test getTimeLeft()"""
    mocker.patch(
        "DIRAC.Resources.Computing.BatchSystems.TimeLeft.SLURMResourceUsage.runCommand",
        return_value=S_OK("19283,9000,10,900,30:00"),
    )

    tl = TimeLeft()

    # Test 1: CPU power = 10
    tl.cpuPower = 10.0
    res = tl.getTimeLeft()
    assert res["OK"]
    assert res["Value"] == 9000

    # Test 2: CPU power = 15
    tl.cpuPower = 15.0
    res = tl.getTimeLeft()
    assert res["OK"]
    assert res["Value"] == 13500
