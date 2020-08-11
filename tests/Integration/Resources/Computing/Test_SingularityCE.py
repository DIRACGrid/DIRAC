#!/bin/env python

""" This integration test is for "Inner" Computing Element SingularityComputingElement
    This test is here and not in the unit tests because it requires singularity to be installed.
"""

import os
import shutil

from DIRAC import gLogger
from DIRAC.tests.Utilities.utils import find_all

from DIRAC.Resources.Computing.test.Test_PoolComputingElement import jobScript, _stopJob

# sut
from DIRAC.Resources.Computing.SingularityComputingElement import SingularityComputingElement


gLogger.setLevel('DEBUG')
fj = find_all('pilot.json', '../', 'tests/Integration/Resources/Computing')[0]


def test_submitJob():
  shutil.copy(fj, os.curdir)
  with open('testJob.py', 'w') as execFile:
    execFile.write(jobScript % '1')
  os.chmod('testJob.py', 0o755)

  ce = SingularityComputingElement('SingularityComputingElement')
  res = ce.submitJob('testJob.py', None)
  assert res['OK'] is False
  assert res['ReschedulePayload'] is True
  res = ce.getCEStatus()
  assert res['OK'] is True
  assert res['SubmittedJobs'] == 1
  _stopJob(1)
  for ff in ['testJob.py', 'pilot.json']:
    if os.path.isfile(ff):
      os.remove(ff)


# def test_submit():
#   jobDesc = {"jobID": 123,
#              "jobParams": {},
#              "resourceParams": {},
#              "optimizerParams": {}}

#   ce = SingularityComputingElement('InProcess')

#   res = ce.submitJob(find_all(executableFile, 'tests')[0],
#                      jobDesc=jobDesc,
#                      log=gLogger.getSubLogger('job_log'),
#                      logLevel='DEBUG')
#   assert res['OK'] is True
