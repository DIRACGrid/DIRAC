""" Test class for SLURMResourceUsage utility
"""
import pytest

from DIRAC import S_OK
from DIRAC.Resources.Computing.BatchSystems.TimeLeft.SLURMResourceUsage import SLURMResourceUsage


SLURM_OUT_SUCCESS_0 = "12345,86400,24,3600,03:00:00"
SLURM_OUT_SUCCESS_1 = "56789,86400,24,3600,4-03:00:00"
SLURM_OUT_SUCCESS_2 = "19283,9000,10,900,30:00"
SLURM_OUT_ERROR = ""


def test_getResourceUsageSuccess(mocker):
    """Here we want to make sure that wallclock limit is correctly interpreted"""
    mocker.patch(
        "DIRAC.Resources.Computing.BatchSystems.TimeLeft.SLURMResourceUsage.runCommand",
        side_effect=[
            S_OK(SLURM_OUT_SUCCESS_0),
            S_OK(SLURM_OUT_SUCCESS_1),
            S_OK(SLURM_OUT_SUCCESS_2),
            S_OK(SLURM_OUT_ERROR),
        ],
    )
    # Get resource usage of job 12345: number of processors (24), wallclocktime limit expressed in hours
    slurmResourceUsage = SLURMResourceUsage("12345", {"Queue": "Test"})
    res = slurmResourceUsage.getResourceUsage()
    assert res["OK"], res["Message"]
    assert res["Value"]["CPU"] == 86400
    assert res["Value"]["CPULimit"] == 259200
    assert res["Value"]["WallClock"] == 3600
    assert res["Value"]["WallClockLimit"] == 10800

    # Get resource usage of job 56789: same number of processors (24), wallclocktime limit expressed in days
    slurmResourceUsage = SLURMResourceUsage("56789", {"Queue": "Test"})
    res = slurmResourceUsage.getResourceUsage()
    assert res["OK"], res["Message"]
    assert res["Value"]["CPU"] == 86400
    assert res["Value"]["CPULimit"] == 8553600
    assert res["Value"]["WallClock"] == 3600
    assert res["Value"]["WallClockLimit"] == 356400

    # Get resource usage of job 19283: different number of processors (10), wallclocktime limit expressed in minutes
    slurmResourceUsage = SLURMResourceUsage("19283", {"Queue": "Test"})
    res = slurmResourceUsage.getResourceUsage()
    assert res["OK"], res["Message"]
    assert res["Value"]["CPU"] == 9000
    assert res["Value"]["CPULimit"] == 18000
    assert res["Value"]["WallClock"] == 900
    assert res["Value"]["WallClockLimit"] == 1800

    # Get resource usage of job 00000: job does not exist
    slurmResourceUsage = SLURMResourceUsage("0000", {"Queue": "Test"})
    res = slurmResourceUsage.getResourceUsage()
    assert not res["OK"], res["Value"]
