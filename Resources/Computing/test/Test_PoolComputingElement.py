#!/bin/env python
"""
tests for PoolComputingElement module
"""

import os
import time

from DIRAC.Resources.Computing.PoolComputingElement import PoolComputingElement

jobScript = """#!/usr/bin/env python

import time
import os

jobNumber = %s
stopFile = 'stop_job_' + str( jobNumber )
start = time.time()

print "Start job", jobNumber
while True:
  time.sleep( 0.1 )
  if os.path.isfile( stopFile ):
    os.unlink( stopFile )
    break
  if (time.time() - start) > 10:
    break
print "End job", jobNumber
"""


def _stopJob(nJob):
  with open('stop_job_%s' % nJob, 'w') as stopFile:
    stopFile.write('Stop')
  time.sleep(0.2)
  if os.path.isfile('stop_job_%s' % nJob):
    os.unlink('stop_job_%s' % nJob)


def test_executeJob():

  ceParameters = {'WholeNode': True,
                  'NumberOfProcessors': 4}
  ce = PoolComputingElement('TestPoolCE')
  ce.setParameters(ceParameters)

  for i in range(4):
    with open('testPoolCEJob_%s.py' % i, 'w') as execFile:
      execFile.write(jobScript % i)
    os.chmod('testPoolCEJob_%s.py' % i, 0o755)

  # Test that max 4 processors can be used at a time
  result = ce.submitJob('testPoolCEJob_0.py', None)
  assert result['OK'] is True
  result = ce.getCEStatus()
  assert 1 == result['UsedProcessors']

  jobParams = {'numberOfProcessors': 2}
  result = ce.submitJob('testPoolCEJob_1.py', None, **jobParams)
  assert result['OK'] is True
  result = ce.getCEStatus()
  assert 3 == result['UsedProcessors']

  jobParams = {'numberOfProcessors': 2}
  result = ce.submitJob('testPoolCEJob_2.py', None, **jobParams)
  assert result['OK'] is False
  assert "Not enough slots" in result['Message']

  _stopJob(0)
  jobParams = {'numberOfProcessors': 2}
  ce = PoolComputingElement('TestPoolCE')
  ceParameters = {'WholeNode': False,
                  'NumberOfProcessors': 4}
  ce.setParameters(ceParameters)
  result = ce.submitJob('testPoolCEJob_2.py', None, **jobParams)
  assert result['OK'] is True
  result = ce.getCEStatus()
  assert 2 == result['UsedProcessors']

  for i in range(4):
    _stopJob(i)
  time.sleep(1)
  result = ce.getCEStatus()
  assert 0 == result['UsedProcessors']

  # Whole node jobs
  result = ce.submitJob('testPoolCEJob_0.py', None)
  assert result['OK'] is True
  result = ce.getCEStatus()
  assert 1 == result['UsedProcessors']

  jobParams = {'wholeNode': True}
  result = ce.submitJob('testPoolCEJob_1.py', None, **jobParams)
  assert result['OK'] is False
  assert "Can not take WholeNode job" in result['Message']

  _stopJob(0)
  time.sleep(1)

  jobParams = {'wholeNode': True}
  result = ce.submitJob('testPoolCEJob_1.py', None, **jobParams)
  assert result['OK'] is True

  # Stop all the jobs if any, cleanup tmp files
  for i in range(4):
    _stopJob(i)
    for ff in ['testPoolCEJob_%s.py' % i, 'stop_job_%s' % i]:
      if os.path.isfile(ff):
        os.unlink(ff)
