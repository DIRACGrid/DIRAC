"""Unit tests for ExecutorMindHandler"""

# pylint: disable=protected-access

from unittest.mock import MagicMock

import pytest

from DIRAC import S_OK
from DIRAC.Core.Base.ExecutorMindHandler import ExecutorMindHandler
from DIRAC.Core.Utilities.ExecutorDispatcher import ExecutorDispatcher, ExecutorDispatcherCallbacks


class RecordingCallbacks(ExecutorDispatcherCallbacks):
    """Dispatch every task to "type1" and record what is sent to which executor"""

    def __init__(self):
        self.sent = []

    def cbDispatch(self, taskId, taskObj, pathExecuted):
        return S_OK("type1")

    def cbSendTask(self, taskId, taskObj, eId, eType):
        self.sent.append((eId, taskId))
        return S_OK()


@pytest.fixture
def handler():
    """Stands in for the handler instance: only what conn_connected uses"""
    callbacks = RecordingCallbacks()
    eDispatch = ExecutorDispatcher()
    eDispatch.setCallbacks(callbacks)
    handler = MagicMock()
    handler._ExecutorMindHandler__allowedClients = []
    handler._ExecutorMindHandler__eDispatch = eDispatch
    handler.exec_executorConnected.return_value = S_OK()
    for i in range(10):
        eDispatch.addTask(f"task{i}", None)
    return handler, callbacks


@pytest.mark.parametrize(
    "kwargs, expectedInFlight",
    [
        ({"maxTasks": 1}, 1),
        ({"maxTasks": 4}, 4),
        ({"maxTasks": 20}, 10),
        ({"maxTasks": 0}, 1),
        ({"maxTasks": "garbage"}, 1),
        ({}, 1),
    ],
)
def test_conn_connected_maxTasks(handler, kwargs, expectedInFlight):
    """An executor is sent as many tasks at once as the maxTasks it connected with"""
    handler, callbacks = handler
    result = ExecutorMindHandler.conn_connected(handler, "trid1", None, {"executorTypes": ["type1"], **kwargs})
    assert result["OK"]
    assert callbacks.sent == [("trid1", f"task{i}") for i in range(expectedInFlight)]
    handler.exec_executorConnected.assert_called_once_with("trid1", ["type1"])
