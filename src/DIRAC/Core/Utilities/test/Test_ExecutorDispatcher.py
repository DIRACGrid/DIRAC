""" py.test test of ExecutorDispatcher
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=protected-access

__RCSID__ = "$Id$"


from DIRAC.Core.Utilities.ExecutorDispatcher import ExecutorState, ExecutorQueues


execState = ExecutorState()
execState.addExecutor(1, "type1", 2)

eQ = ExecutorQueues()


def test_ExecutorState():
  """ test of ExecutorState
  """
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
  """ test of ExecutorQueues
  """
  for y in range(2):
    for i in range(3):
      assert eQ.pushTask("type%s" % y, "t%s%s" % (y, i)) == i + 1
  assert "DONE IN"
  assert eQ.pushTask("type0", "t01") == 3
  assert eQ.getState()
  assert eQ.popTask("type0")[0] == "t00"
  assert eQ.pushTask("type0", "t00", ahead=True) == 3
  assert eQ.popTask("type0")[0] == "t00"
  assert eQ.deleteTask("t01")
  assert eQ.getState()
  assert eQ.deleteTask("t02")
  assert eQ.getState()
  for i in range(3):
    assert eQ.popTask("type1")[0] == "t1%s" % i
  assert eQ._internals()
