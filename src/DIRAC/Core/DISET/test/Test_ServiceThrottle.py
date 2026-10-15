"""Unit tests for the throttle state tracking of DISET services"""
from unittest.mock import MagicMock

import pytest

from DIRAC.Core.DISET.private import Service as ServiceModule
from DIRAC.Core.DISET.private.Service import Service

MAX_QUEUE = 10


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def time(self):
        return self.now


@pytest.fixture
def clock(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(ServiceModule.time, "time", clock.time)
    return clock


@pytest.fixture
def service(clock):
    """A Service with only the attributes used by the throttle logic"""
    svc = Service.__new__(Service)
    svc._name = "Test/Service"
    svc._cfg = MagicMock()
    svc._cfg.getMaxWaitingPetitions.return_value = MAX_QUEUE
    svc._cfg.getMaxThreads.return_value = 5
    svc._threadPool = MagicMock()
    svc._threadPool._threads = set()
    svc._throttleStartedAt = None
    svc._lastThrottleLog = 0
    svc._maxThrottleDuration = 0
    return svc


def setQueue(svc, size):
    svc._threadPool._work_queue.qsize.return_value = size


def test_no_throttle_below_limit(service):
    setQueue(service, MAX_QUEUE)
    assert service.updateThrottleState() is False
    assert service.throttleDuration == 0


def test_throttle_starts_above_limit(service, clock):
    setQueue(service, MAX_QUEUE + 1)
    assert service.updateThrottleState() is True
    clock.now += 42
    assert service.throttleDuration == pytest.approx(42)


def test_throttle_hysteresis(service):
    setQueue(service, MAX_QUEUE + 1)
    assert service.updateThrottleState() is True
    # Back under the limit but above the clear threshold: keep throttling
    setQueue(service, MAX_QUEUE * ServiceModule.THROTTLE_CLEAR_FRACTION + 1)
    assert service.updateThrottleState() is True
    # Drained to the clear threshold: stop throttling
    setQueue(service, MAX_QUEUE * ServiceModule.THROTTLE_CLEAR_FRACTION)
    assert service.updateThrottleState() is False
    assert service.throttleDuration == 0


def test_throttle_duration_persists_across_checks(service, clock):
    setQueue(service, MAX_QUEUE + 1)
    service.updateThrottleState()
    start = service._throttleStartedAt
    clock.now += ServiceModule.THROTTLE_LOG_INTERVAL_SECONDS + 1
    assert service.updateThrottleState() is True
    assert service._throttleStartedAt == start
    assert service._lastThrottleLog == clock.now
