""" unit test for Watchdog.py
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# imports
import os
from mock import MagicMock

# sut
from DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog import Watchdog

mock_exeThread = MagicMock()
mock_spObject = MagicMock()


def test_calibrate():
  pid = os.getpid()
  wd = Watchdog(pid, mock_exeThread, mock_spObject, 5000)
  res = wd.calibrate()
  assert res['OK'] is True


def test__performChecks():
  pid = os.getpid()
  wd = Watchdog(pid, mock_exeThread, mock_spObject, 5000)

  res = wd.calibrate()
  assert res['OK'] is True
  res = wd._performChecks()
  assert res['OK'] is True


def test__performChecksFull():
  pid = os.getpid()
  wd = Watchdog(pid, mock_exeThread, mock_spObject, 5000)
  wd.testCPULimit = 1
  wd.testMemoryLimit = 1

  res = wd.calibrate()
  assert res['OK'] is True
  res = wd._performChecks()
  assert res['OK'] is True
