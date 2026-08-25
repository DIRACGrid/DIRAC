"""unit test for Watchdog.py"""
import os
import signal
import subprocess
import sys
import time
from unittest.mock import MagicMock, patch

import psutil
import pytest

# sut
from DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog import Watchdog

SECTION = "/Systems/WorkloadManagement/JobWrapper"

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


#: A job that knows how to wind down: signal it 10 min out, once it has run 5 min.
#: 16740 normalized units is 600 s of wall clock on a node benchmarked at 27.9.
GRACEFUL = {"StopSigRegex": "gauss", "StopSigFinishWork": 16740}


@patch.object(Watchdog, "_signalPayload")
class TestGracefulStop:
    """Signalling the payload before its budget runs out. Opt-in through StopSigRegex."""

    def _make_watchdog(self, jobArgs=None, initialWallClockLeft=3600, elapsed=0):
        wd = Watchdog(os.getpid(), MagicMock(), MagicMock(), 5000, jobArgs=jobArgs or {})
        wd.initialWallClockLeft = initialWallClockLeft
        wd.initialValues = {"StartTime": time.time() - elapsed}
        wd.testTimeLeft = 1
        wd.exeThread.is_alive.return_value = True
        wd.cpuPower = 27.9
        return wd

    @pytest.mark.parametrize(
        "cpuPower, elapsed, expectSignalled",
        [
            # 16740 units buys 600 s here, so 700 s of budget left is still comfortable...
            (27.9, 2900, False),
            (27.9, 3100, True),
            # ...but on a node half as fast the same work needs 1200 s, and 700 s is not
            # enough. A figure in seconds could not tell those two nodes apart.
            (13.95, 2900, True),
        ],
    )
    def test_the_wind_down_is_budgeted_in_cpu_work(self, mock_signal, cpuPower, elapsed, expectSignalled):
        """Finishing the unit of work in progress costs more wall clock on a slower node."""
        wd = self._make_watchdog(jobArgs=GRACEFUL, initialWallClockLeft=3600, elapsed=elapsed)
        wd.cpuPower = cpuPower
        wd._Watchdog__checkTimeLeft()
        assert wd.stopSigSent is expectSignalled

    def test_signals_the_payload_with_budget_still_on_the_clock(self, mock_signal):
        """3600 s of budget, 3000 gone: the payload is asked to stop, and told with 600 left.

        Those 600 s are the point: an application asked to stop needs time to stop, so the
        ask cannot wait for the budget to run out.
        """
        wd = self._make_watchdog(jobArgs=GRACEFUL, elapsed=3000)
        wd.execute()
        assert wd.stopSigSent is True
        assert wd.wallClockLeft == pytest.approx(GRACEFUL["StopSigFinishWork"] / 27.9, abs=2)
        mock_signal.assert_called_once_with()

    @pytest.mark.parametrize(
        "jobArgs, initialWallClockLeft, elapsed, why",
        [
            ({"StopSigFinishWork": 16740}, 3600, 3000, "no StopSigRegex, so opted out"),
            ({**GRACEFUL, "StopSigRegex": ""}, 3600, 3000, "an empty regex opts out, it does not match everything"),
            (GRACEFUL, 3600, 600, "plenty of budget left"),
            (GRACEFUL, 300, 60, "barely started: less work done than stopping it would cost"),
            (GRACEFUL, 0, 3000, "no budget published, so no deadline to act on"),
        ],
    )
    def test_stays_silent(self, mock_signal, jobArgs, initialWallClockLeft, elapsed, why):
        wd = self._make_watchdog(jobArgs=jobArgs, initialWallClockLeft=initialWallClockLeft, elapsed=elapsed)
        wd._Watchdog__checkTimeLeft()
        assert wd.stopSigSent is False, why
        mock_signal.assert_not_called()

    @pytest.mark.parametrize(
        "elapsed, expectSignalled",
        [
            # 16740 units is 600 s here, so the payload must have run 600 s to have done as
            # much work as stopping it will cost. 500 s in, it has not, however near the end.
            (500, False),
            (700, True),
        ],
    )
    def test_the_start_guard_defaults_to_the_cost_of_stopping(self, mock_signal, elapsed, expectSignalled):
        """Unset, StopSigStartWork is what winding down costs: below that there is nothing to save.

        Without it a payload matched into a slot shorter than its own wind-down would be
        signalled immediately and report success having produced nothing.
        """
        wd = self._make_watchdog(jobArgs=GRACEFUL, initialWallClockLeft=1000, elapsed=elapsed)
        assert wd._Watchdog__checkTimeLeft()["OK"] is True
        assert wd.stopSigSent is expectSignalled

    def test_signals_once_then_leaves_the_hard_stop_to_finish(self, mock_signal):
        """Later polls must not keep signalling, and a payload that ignores it is still killed."""
        wd = self._make_watchdog(jobArgs=GRACEFUL, elapsed=3000)
        for _ in range(5):
            assert wd._Watchdog__checkTimeLeft()["OK"] is True
        assert mock_signal.call_count == 1

        wd.initialValues["StartTime"] = time.time() - 3700
        assert not wd._Watchdog__checkTimeLeft()["OK"]

    @pytest.mark.parametrize(
        "cs, jobArgs, expected",
        [
            ({}, {}, (None, 2, 0, 0)),  # off, on the built-in defaults
            (  # the CS alone turns it on: no JDL change, so productions already submitted are covered
                {
                    f"{SECTION}/StopSigRegex": "Gauss",
                    f"{SECTION}/StopSigNumber": 10,
                    f"{SECTION}/StopSigStartWork": 8370,
                    f"{SECTION}/StopSigFinishWork": 5580,
                },
                {},
                ("Gauss", 10, 8370, 5580),
            ),
            # a job that knows better than the site-wide setting still gets its way
            (
                {f"{SECTION}/StopSigRegex": "Gauss"},
                {"StopSigRegex": "myApp", "StopSigNumber": "1"},
                ("myApp", 1, 0, 0),
            ),
            # and can opt out of it
            ({f"{SECTION}/StopSigRegex": "Gauss"}, {"StopSigRegex": ""}, ("", 2, 0, 0)),
        ],
    )
    def test_settings_come_from_the_jdl_then_the_cs(self, mock_signal, cs, jobArgs, expected):
        with patch("DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog.gConfig") as watchdogCfg:
            watchdogCfg.getValue.side_effect = lambda key, default=None: cs.get(key, default)
            wd = Watchdog(os.getpid(), MagicMock(), MagicMock(), 5000, jobArgs=jobArgs)
            wd.calibrate()
            wd.initialize()

        assert (
            wd.stopSigRegex,
            wd.stopSigNumber,
            wd.stopSigStartWork,
            wd.stopSigFinishWork,
        ) == expected


class TestSignalPayload:
    """Which processes the signal reaches, and which it must not."""

    @staticmethod
    def _standIn(*payloads):
        """A stand-in JobWrapper that spawns the given payloads and outlives them."""
        spawns = "; ".join(f"subprocess.Popen({list(p)!r})" for p in payloads)
        wrapper = subprocess.Popen([sys.executable, "-c", f"import subprocess, time; {spawns}; time.sleep(60)"])
        deadline = time.time() + 30
        children = []
        while len(children) < len(payloads) and time.time() < deadline:
            children = psutil.Process(wrapper.pid).children(recursive=True)
            time.sleep(0.05)
        assert len(children) == len(payloads), "the stand-in never spawned its payloads"
        return wrapper, {" ".join(c.cmdline()): c for c in children}

    @staticmethod
    def _watchdog(pid, nameRegex):
        wd = Watchdog(pid, MagicMock(), MagicMock(), 5000, jobArgs={"StopSigRegex": nameRegex})
        wd.stopSigNumber = int(signal.SIGTERM)
        return wd

    @staticmethod
    def _stopped(process):
        deadline = time.time() + 30
        while process.is_running() and process.status() != psutil.STATUS_ZOMBIE and time.time() < deadline:
            time.sleep(0.05)
        return not process.is_running() or process.status() == psutil.STATUS_ZOMBIE

    def test_signals_the_named_application_only(self):
        """Not the parent, which still has the outputs to upload, nor its siblings."""
        wrapper, payloads = self._standIn(["sleep", "61"], ["sleep", "62"])
        try:
            assert self._watchdog(wrapper.pid, "sleep 61")._signalPayload() == 1
            assert self._stopped(payloads["sleep 61"]), "the named payload ignored the signal"
            assert payloads["sleep 62"].is_running(), "a process the regex did not name was signalled"
            assert wrapper.poll() is None, "the wrapper was signalled too"
        finally:
            wrapper.kill()
            wrapper.wait(timeout=30)

    def test_leaves_other_jobs_on_the_node_alone(self):
        """A node runs many jobs whose command lines all match. Only our own tree is ours.

        The search starts from this JobWrapper's pid, so another job's payload is out of
        reach however well it matches.
        """
        ours, ourPayloads = self._standIn(["sleep", "61"])
        theirs, theirPayloads = self._standIn(["sleep", "61"])
        try:
            assert self._watchdog(ours.pid, "sleep 61")._signalPayload() == 1
            assert self._stopped(ourPayloads["sleep 61"])
            assert theirPayloads["sleep 61"].is_running(), "signalled another job's payload"
        finally:
            for wrapper in (ours, theirs):
                wrapper.kill()
                wrapper.wait(timeout=30)

    @pytest.mark.parametrize("nameRegex", ["no-such-payload", "[unclosed"])
    def test_signals_nothing_it_cannot_name(self, nameRegex):
        """A regex matching nothing, or not compiling, must not fall back to signalling all."""
        wrapper, payloads = self._standIn(["sleep", "61"])
        try:
            assert self._watchdog(wrapper.pid, nameRegex)._signalPayload() == 0
            assert payloads["sleep 61"].is_running()
        finally:
            wrapper.kill()
            wrapper.wait(timeout=30)

    def test_an_exited_payload_is_not_an_error(self):
        assert self._watchdog(999999999, "anything")._signalPayload() == 0
