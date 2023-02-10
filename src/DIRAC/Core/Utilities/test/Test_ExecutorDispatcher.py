""" py.test test of ExecutorDispatcher
"""
# pylint: disable=protected-access
from DIRAC.Core.Utilities.ExecutorDispatcher import (
    ExecutorState,
    ExecutorQueues,
)


execState = ExecutorState()
execState.addExecutor(1, "type1", 2)

eQ = ExecutorQueues()


def test_ExecutorState():
    """test of ExecutorState"""
    assert execState.freeSlots(1) == 2
    assert execState.addTask(1, "t1") == 1
    assert execState.addTask(1, "t1") == 1
    assert execState.addTask(1, "t2") == 2
    assert execState.freeSlots(1) == 0
    assert execState.full(1)
    assert execState.removeTask("t1") == 1
    assert execState.freeSlots(1) == 1
    assert execState.getFreeExecutors("type1") == {1: 1}
    assert execState.getTasksForExecutor(1) == {"t2"}
    assert execState.removeExecutor(1)
    assert execState._internals()


def test_execQueues():
    """test of ExecutorQueues"""
    for y in range(2):
        for i in range(3):
            assert eQ.pushTask(f"type{y}", f"t{y}{i}") == i + 1
    assert "DONE IN"
    res_internals = eQ._internals()
    assert res_internals["queues"] == {"type0": ["t00", "t01", "t02"], "type1": ["t10", "t11", "t12"]}
    assert set(res_internals["lastUse"].keys()) == {"type0", "type1"}
    assert res_internals["taskInQueue"] == {
        "t00": "type0",
        "t01": "type0",
        "t02": "type0",
        "t10": "type1",
        "t11": "type1",
        "t12": "type1",
    }
    assert eQ.pushTask("type0", "t01") == 3
    assert eQ.getState()
    assert eQ.popTask("type0")[0] == "t00"
    assert eQ.pushTask("type0", "t00", ahead=True) == 3
    assert eQ.popTask("type0")[0] == "t00"
    assert eQ.deleteTask("t01")
    res_internals = eQ._internals()
    assert res_internals["queues"] == {"type0": ["t02"], "type1": ["t10", "t11", "t12"]}
    assert set(res_internals["lastUse"].keys()) == {"type0", "type1"}
    assert res_internals["taskInQueue"] == {
        "t02": "type0",
        "t10": "type1",
        "t11": "type1",
        "t12": "type1",
    }
    assert eQ.getState()
    assert eQ.deleteTask("t02")
    res_internals = eQ._internals()
    assert res_internals["queues"] == {"type0": [], "type1": ["t10", "t11", "t12"]}
    assert set(res_internals["lastUse"].keys()) == {"type0", "type1"}
    assert res_internals["taskInQueue"] == {
        "t10": "type1",
        "t11": "type1",
        "t12": "type1",
    }
    assert eQ.getState()
    for i in range(3):
        assert eQ.popTask("type1")[0] == f"t1{i}"
    res_internals = eQ._internals()
    assert res_internals["queues"] == {"type0": [], "type1": []}
    assert set(res_internals["lastUse"].keys()) == {"type0", "type1"}
    assert res_internals["taskInQueue"] == {}

    assert eQ.pushTask("type0", "t00") == 1
    assert eQ.popTask("type0") == ("t00", "type0")
    res_internals = eQ._internals()
    assert res_internals["queues"] == {"type0": [], "type1": []}
    assert set(res_internals["lastUse"].keys()) == {"type0", "type1"}
    assert res_internals["taskInQueue"] == {}

    assert eQ.pushTask("type0", "t00") == 1
    assert eQ.deleteTask("t00")
    res_internals = eQ._internals()
    assert res_internals["queues"] == {"type0": [], "type1": []}
    assert set(res_internals["lastUse"].keys()) == {"type0", "type1"}
    assert res_internals["taskInQueue"] == {}

    assert not eQ.deleteTask("t00")
