"""
Test SubLogger
"""

__RCSID__ = "$Id$"

import pytest

from DIRAC.FrameworkSystem.private.standardLogging.test.TestLogUtilities import gLogger, gLoggerReset


def test_getSubLoggerLogRecord():
  """
  Create some subloggers and create a log record
  """
  capturedBackend, log, sublog = gLoggerReset()

  # Send a first log with a simple sublogger
  log.always('message')
  assert " Framework/log " in capturedBackend.getvalue()

  # Reinitialize the buffer and send a log with a child of the sublogger
  capturedBackend.truncate(0)
  capturedBackend.seek(0)

  sublog.always('message')
  assert " Framework/log/sublog " in capturedBackend.getvalue()

  # Generate a new sublogger from sublog
  capturedBackend.truncate(0)
  capturedBackend.seek(0)

  subsublog = sublog.getSubLogger('subsublog')
  subsublog.always('message')
  assert " Framework/log/sublog/subsublog " in capturedBackend.getvalue()


def test_getSubLoggerObject():
  """
  Create a sublogger, set its level, get a sublogger with the same name and check that is it the same object
  """
  _, _, _ = gLoggerReset()

  log = gLogger.getSubLogger('log')
  log.setLevel('notice')
  anotherLog = gLogger.getSubLogger('log')

  assert log.getLevel() == anotherLog.getLevel()
  assert log == anotherLog
