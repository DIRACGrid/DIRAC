"""unit test for Watchdog.py"""
import math
import os
import time
from unittest.mock import MagicMock, patch

import pytest

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


class TestCheckTimeLeft:
    """Tests for the simplified wall-clock countdown time-left logic."""

    def _make_watchdog(self, initialWallClockLeft=0, cpuPower=10.0):
        """A Watchdog holding a budget the JobAgent has already taken StopMargin off."""
        pid = os.getpid()
        wd = Watchdog(pid, mock_exeThread, mock_spObject, 5000)
        wd.initialWallClockLeft = initialWallClockLeft
        wd.cpuPower = cpuPower
        wd.initialValues = {"StartTime": time.time()}
        wd.testTimeLeft = 1
        return wd

    def test_time_left_not_available(self):
        """When CPUTimeLeft was not set, the check should pass gracefully."""
        wd = self._make_watchdog(initialWallClockLeft=0)
        result = wd._Watchdog__checkTimeLeft()
        assert result["OK"] is True

    def test_plenty_of_time_left(self):
        """When there's plenty of time, the check should pass."""
        wd = self._make_watchdog(initialWallClockLeft=3600)
        result = wd._Watchdog__checkTimeLeft()
        assert result["OK"] is True
        assert wd.wallClockLeft > 3000

    def test_budget_not_exhausted(self):
        """The payload keeps the whole published budget: it is already net of the margin.

        Under the old behaviour the Watchdog took another StopMargin off here and stopped
        the job with 100 s still on its clock.
        """
        wd = self._make_watchdog(initialWallClockLeft=3600)
        wd.initialValues["StartTime"] = time.time() - 3500
        result = wd._Watchdog__checkTimeLeft()
        assert result["OK"] is True

    def test_budget_exhausted(self):
        """Once the published budget runs out, what is left of the slot is the reserve."""
        wd = self._make_watchdog(initialWallClockLeft=3600)
        wd.initialValues["StartTime"] = time.time() - 3700
        result = wd._Watchdog__checkTimeLeft()
        assert not result["OK"]

    def test_time_left_updates_heartbeat_value(self):
        """self.wallClockLeft should be updated for heartbeat display."""
        wd = self._make_watchdog(initialWallClockLeft=3600, cpuPower=10.0)
        wd._Watchdog__checkTimeLeft()
        # wallClockLeft should be approximately 3600s
        assert wd.wallClockLeft > 3500

    def test_boundary(self):
        """A second still on the clock is a second the payload may use."""
        wd = self._make_watchdog(initialWallClockLeft=1000)
        wd.initialValues["StartTime"] = time.time() - 998
        result = wd._Watchdog__checkTimeLeft()
        assert result["OK"] is True

    @patch("DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog.gConfig")
    def test_initialize_reads_config(self, mock_gConfig):
        """initialize() should read CPUTimeLeft and convert to wall-clock seconds."""
        config_values = {
            "/LocalSite/CPUTimeLeft": 36000,  # 36000 HS06*s
            "/LocalSite/CPUNormalizationFactor": 10.0,  # 10 HS06
        }
        mock_gConfig.getValue.side_effect = lambda key, default=None: config_values.get(key, default)

        pid = os.getpid()
        wd = Watchdog(pid, mock_exeThread, mock_spObject, 5000)
        wd.calibrate()
        wd.initialize()

        # 36000 / 10.0 = 3600 wall-clock seconds
        assert wd.initialWallClockLeft == 3600.0


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


def test__getUsageSummaryWithSamples(monkeypatch):
    """Once samples have been collected, the summary must actually report them."""
    monkeypatch.delenv("JOBID", raising=False)
    pid = os.getpid()
    wd = Watchdog(pid, mock_exeThread, mock_spObject, 5000)
    assert wd.calibrate()["OK"] is True
    assert wd._performChecks()["OK"] is True

    wd._Watchdog__getUsageSummary()

    # LoadAverage is sampled unconditionally, the others depend on the probes
    assert "LoadAverage" in wd.currentStats
    assert {"WallClockTime(s)", "ScaledCPUTime(s)"} <= set(wd.currentStats)
    assert all(math.isfinite(value) for value in wd.currentStats.values()), wd.currentStats


def test__getUsageSummaryWithoutCalibrationBaseline(monkeypatch):
    """Samples without a baseline must be skipped, not raise.

    calibrate() only records initialValues[DiskSpace]/[MemoryUsed] when the probe
    succeeded, while a later check cycle collects samples regardless.
    """
    monkeypatch.delenv("JOBID", raising=False)
    pid = os.getpid()
    wd = Watchdog(pid, mock_exeThread, mock_spObject, 5000)
    assert wd.calibrate()["OK"] is True

    # as if both probes had failed at calibration time
    wd.initialValues.pop("DiskSpace", None)
    wd.initialValues.pop("MemoryUsed", None)

    assert wd._performChecks()["OK"] is True
    wd._Watchdog__getUsageSummary()

    for name in ("DiskSpace(MB)", "MemoryUsed(MB)"):
        assert name not in wd.currentStats
    assert all(math.isfinite(value) for value in wd.currentStats.values()), wd.currentStats


class TestConcurrentJobsInOneSlot:
    """A PoolComputingElement runs several jobs side by side in the same batch slot."""

    SLOT_SECONDS = 3600
    MARGIN = 300

    @pytest.mark.parametrize(
        "secondsIntoSlot, stillRunning",
        [(SLOT_SECONDS - MARGIN - 60, True), (SLOT_SECONDS - MARGIN + 60, False)],
    )
    def test_jobs_matched_at_different_times_stop_together(self, secondsIntoSlot, stillRunning):
        """Both end at the slot's end, not at their own start plus a whole slot.

        The JobAgent publishes what is left *now*, so the job matched ten minutes later is
        handed a budget shorter by exactly those ten minutes.
        """
        now = time.time()
        for startedAt in (0, 600):  # two matches, ten minutes apart
            wd = Watchdog(os.getpid(), MagicMock(), MagicMock(), 5000)
            wd.initialWallClockLeft = self.SLOT_SECONDS - startedAt - self.MARGIN
            wd.initialValues = {"StartTime": now - (secondsIntoSlot - startedAt)}
            wd.testTimeLeft = 1
            assert wd._Watchdog__checkTimeLeft()["OK"] is stillRunning, f"job matched at {startedAt}s"
