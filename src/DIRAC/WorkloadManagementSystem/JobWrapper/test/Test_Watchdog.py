""" unit test for Watchdog.py
"""
import math
import os
from unittest.mock import MagicMock

# sut
from DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog import Watchdog

mock_exeThread = MagicMock()
mock_spObject = MagicMock()


def test_calibrate():
    pid = os.getpid()
    wd = Watchdog(pid, mock_exeThread, mock_spObject, 5000)
    res = wd.calibrate()
    assert res["OK"] is True


def test__performChecks():
    pid = os.getpid()
    wd = Watchdog(pid, mock_exeThread, mock_spObject, 5000)

    res = wd.calibrate()
    assert res["OK"] is True
    res = wd._performChecks()
    assert res["OK"] is True


def test__performChecksFull():
    pid = os.getpid()
    wd = Watchdog(pid, mock_exeThread, mock_spObject, 5000)
    wd.testCPULimit = 1
    wd.testMemoryLimit = 1

    res = wd.calibrate()
    assert res["OK"] is True
    res = wd._performChecks()
    assert res["OK"] is True


def test__getUsageSummaryNoSamples(monkeypatch):
    """A job ending before the first Watchdog cycle must not report non-finite parameters."""
    monkeypatch.delenv("JOBID", raising=False)
    pid = os.getpid()
    wd = Watchdog(pid, mock_exeThread, mock_spObject, 5000)
    res = wd.calibrate()
    assert res["OK"] is True

    # No check cycle has run yet, so all the sampling lists are still empty
    wd._Watchdog__getUsageSummary()

    for name in ("LastUpdateCPU(s)", "DiskSpace(MB)", "MemoryUsed(MB)", "LoadAverage"):
        assert name not in wd.currentStats
    assert all(math.isfinite(value) for value in wd.currentStats.values()), wd.currentStats
