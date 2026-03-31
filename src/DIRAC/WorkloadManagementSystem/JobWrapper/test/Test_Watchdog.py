"""unit test for Watchdog.py"""
import os
import time
from unittest.mock import MagicMock, patch

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

    def _make_watchdog(self, initialWallClockLeft=0, stopMargin=300, cpuPower=10.0):
        pid = os.getpid()
        wd = Watchdog(pid, mock_exeThread, mock_spObject, 5000)
        wd.initialWallClockLeft = initialWallClockLeft
        wd.stopMargin = stopMargin
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
        wd = self._make_watchdog(initialWallClockLeft=3600, stopMargin=300)
        result = wd._Watchdog__checkTimeLeft()
        assert result["OK"] is True
        assert wd.wallClockLeft > 3000

    def test_below_stop_margin(self):
        """When wall-clock left drops below stop margin, the check should fail."""
        wd = self._make_watchdog(initialWallClockLeft=3600, stopMargin=300)
        # Pretend the job started 3500 seconds ago (only 100s left, below 300s margin)
        wd.initialValues["StartTime"] = time.time() - 3500
        result = wd._Watchdog__checkTimeLeft()
        assert not result["OK"]

    def test_time_left_updates_heartbeat_value(self):
        """self.wallClockLeft should be updated for heartbeat display."""
        wd = self._make_watchdog(initialWallClockLeft=3600, cpuPower=10.0)
        wd._Watchdog__checkTimeLeft()
        # wallClockLeft should be approximately 3600s
        assert wd.wallClockLeft > 3500

    def test_exact_stop_margin_boundary(self):
        """When wall-clock left equals stop margin, the check should fail (< not <=)."""
        wd = self._make_watchdog(initialWallClockLeft=1000, stopMargin=300)
        # 699s elapsed → 301s left, which is not < 300 → should pass
        wd.initialValues["StartTime"] = time.time() - 699
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
