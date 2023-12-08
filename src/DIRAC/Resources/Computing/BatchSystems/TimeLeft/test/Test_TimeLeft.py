""" Test TimeLeft utility
"""
from DIRAC import S_OK
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.TimeLeft import TimeLeft


def test_cpuPowerNotDefined(mocker):
    """Test cpuPower not defined"""
    mocker.patch("DIRAC.gConfig.getSections", return_value={"Type": "SLURM", "JobID": "12345", "Parameters": {}})

    tl = TimeLeft()
    res = tl.getTimeLeft()
    assert not res["OK"]
    assert "/LocalSite/CPUNormalizationFactor not defined" in res["Message"]


def test_batchSystemNotDefined(mocker):
    """Test batch system not defined"""
    mocker.patch("DIRAC.gConfig.getSections", return_value={})

    tl = TimeLeft()
    tl.cpuPower = 10
    res = tl.getTimeLeft()
    assert not res["OK"]
    assert "Current batch system is not supported" in res["Message"]


def test_batchSystemNotDefinedInConfigButInEnvironmentVariables(mocker, monkeypatch):
    """Test batch system not defined but present in environment variables (should fail from v9.0)"""
    mocker.patch(
        "DIRAC.Resources.Computing.BatchSystems.TimeLeft.HTCondorResourceUsage.runCommand",
        return_value=S_OK("9000 800"),
    )
    mocker.patch("DIRAC.gConfig.getSections", return_value={})
    monkeypatch.setenv("HTCONDOR_JOBID", "12345.0")
    monkeypatch.setenv("_CONDOR_JOB_AD", "/path/to/config")

    tl = TimeLeft()
    tl.cpuPower = 10
    res = tl.getTimeLeft()
    assert res["OK"]
    assert res["Value"] == 82000


def test_getScaledCPU(mocker):
    """Test getScaledCPU()"""
    mocker.patch(
        "DIRAC.Resources.Computing.BatchSystems.TimeLeft.SLURMResourceUsage.runCommand",
        return_value=S_OK("19283,9000,10,900,30:00"),
    )
    mocker.patch("DIRAC.gConfig.getSections", return_value={"Type": "SLURM", "JobID": "12345", "Parameters": {}})

    tl = TimeLeft()

    # Test 1: no normalization
    res = tl.getScaledCPU()
    assert res == 0

    # Test 2: normalization
    tl.cpuPower = 5.0
    res = tl.getScaledCPU()
    assert res == 45000


def test_getTimeLeft(mocker):
    """Test getTimeLeft()"""
    mocker.patch(
        "DIRAC.Resources.Computing.BatchSystems.TimeLeft.SLURMResourceUsage.runCommand",
        return_value=S_OK("19283,9000,10,900,30:00"),
    )
    mocker.patch("DIRAC.gConfig.getSections", return_value={"Type": "SLURM", "JobID": "12345", "Parameters": {}})

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
