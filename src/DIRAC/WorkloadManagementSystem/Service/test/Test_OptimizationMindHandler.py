"""Unit tests for the job loading of the OptimizationMindHandler"""

# pylint: disable=protected-access

from unittest.mock import MagicMock

import pytest

from DIRAC import S_OK, gLogger
from DIRAC.WorkloadManagementSystem.Service import OptimizationMindHandler as module
from DIRAC.WorkloadManagementSystem.Service.OptimizationMindHandler import OptimizationMindHandler as Handler

JOBPATH = "WorkloadManagement/JobPath"


@pytest.fixture
def mind(mocker):
    """The handler class with the JobDB, the dispatcher and the CS mocked"""
    options = {}
    tasks = set()
    jobDB = MagicMock()

    mocker.patch.object(Handler, "srv_getCSOption", side_effect=lambda name, default=None: options.get(name, default))
    mocker.patch.object(Handler, "getExecutorsConnected", return_value={JOBPATH: 1})
    mocker.patch.object(Handler, "getTaskIds", side_effect=lambda: list(tasks))
    mocker.patch.object(Handler, "executeTask", side_effect=lambda jid, _: tasks.add(jid))
    mocker.patch.object(module, "CachedJobState")
    setTaskPeriod = mocker.patch.object(module.ThreadScheduler.gThreadScheduler, "setTaskPeriod")
    mocker.patch.object(Handler, "log", gLogger, create=True)
    mocker.patch.object(Handler, "_OptimizationMindHandler__jobDB", jobDB, create=True)
    mocker.patch.object(Handler, "_OptimizationMindHandler__loadTaskId", "loadTask")

    m = MagicMock()
    m.options, m.tasks, m.jobDB, m.setTaskPeriod = options, tasks, jobDB, setTaskPeriod
    m.load = Handler._OptimizationMindHandler__loadJobs
    return m


def test_loadAddsUnknownJobsOnly(mind):
    mind.tasks.update({1, 2})
    mind.jobDB.selectJobs.return_value = S_OK([1, 2, 3, 4])
    assert mind.load()["OK"]
    assert mind.tasks == {1, 2, 3, 4}
    assert Handler.executeTask.call_count == 2


@pytest.mark.parametrize("options, expectedPeriod", [({}, 60), ({"LoadJobPeriod": 300}, 300)])
def test_loadPeriod(mind, options, expectedPeriod):
    mind.options.update(options)
    mind.jobDB.selectJobs.return_value = S_OK([])
    assert mind.load()["OK"]
    mind.setTaskPeriod.assert_called_once_with("loadTask", expectedPeriod)


def test_concurrentLoadIsSkipped(mind):
    """A load requested while another one is running does nothing"""
    lock = Handler._OptimizationMindHandler__loadLock
    assert lock.acquire(blocking=False)
    try:
        assert mind.load()["OK"]
    finally:
        lock.release()
    mind.jobDB.selectJobs.assert_not_called()
    mind.setTaskPeriod.assert_not_called()
    # The lock is released again after a load
    mind.jobDB.selectJobs.return_value = S_OK([])
    assert mind.load()["OK"]
    assert not lock.locked()
