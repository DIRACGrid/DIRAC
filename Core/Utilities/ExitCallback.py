# $HeadURL$


from __future__ import absolute_import

import signal
import os
import sys

__RCSID__ = "$Id$"


gCallbackList = []

def registerSignals():
  """
  Registers signal handlers
  """
  for sigNum in (signal.SIGINT, signal.SIGTERM):
    try:
      signal.signal(sigNum, execute)
    except Exception:
      pass


def execute(exitCode, frame):
  """
  Executes the callback list
  """
  # TODO: <Adri> Disable ExitCallback until I can debug it
  sys.stdout.flush()
  sys.stderr.flush()
  os._exit(exitCode)
  for callback in gCallbackList:
    try:
      callback(exitCode)
    except Exception:
      from DIRAC.FrameworkSystem.Client.Logger import gLogger
      gLogger.exception("Exception while calling callback")
  os._exit(exitCode)


def registerExitCallback(function):
  """
  Adds a new callback to the list
  """
  if function not in gCallbackList:
    gCallbackList.append(function)
