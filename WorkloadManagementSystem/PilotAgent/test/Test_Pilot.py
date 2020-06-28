""" Test class for agents
"""

# imports
import json
import os
import sys

if "--no-cov" in sys.argv:
  del sys.argv[sys.argv.index('--no-cov')]

from DIRAC.WorkloadManagementSystem.PilotAgent.pilotTools import PilotParams, CommandBase
from DIRAC.WorkloadManagementSystem.PilotAgent.pilotCommands import GetPilotVersion


def test_GetPilotVersion():
  pp = PilotParams()
  # Now defining a local file for test, and all the necessary parameters
  fp = open('pilot.json', 'w')
  json.dump({'TestSetup': {'Version': ['v1r1', 'v2r2']}}, fp)
  fp.close()
  pp.setup = 'TestSetup'
  pp.pilotCFGFileLocation = 'file://%s' % os.getcwd()
  gpv = GetPilotVersion(pp)
  result = gpv.execute()
  assert result is None
  assert gpv.pp.releaseVersion == 'v1r1'


def test_commandBase():
  pp = PilotParams()
  cb = CommandBase(pp)
  returnCode, _outputData = cb.executeAndGetOutput("ls")
  assert returnCode == 0
