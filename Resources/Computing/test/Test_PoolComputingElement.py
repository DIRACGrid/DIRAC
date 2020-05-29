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

print "Start job", jobNumber, start
while True:
  time.sleep( 0.1 )
  if os.path.isfile( stopFile ):
    os.unlink( stopFile )
    break
  if (time.time() - start) > 30:
    break
print "End job", jobNumber, time.time()
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

  for i in range(6):
    with open('testPoolCEJob_%s.py' % i, 'w') as execFile:
      execFile.write(jobScript % i)
    os.chmod('testPoolCEJob_%s.py' % i, 0o755)

  # Test that max 4 processors can be used at a time
  result = ce.submitJob('testPoolCEJob_0.py', None)
  assert result['OK'] is True
  result = ce.getCEStatus()
  assert result['UsedProcessors'] == 1

  jobParams = {'mpTag': True, 'numberOfProcessors': 2}
  result = ce.submitJob('testPoolCEJob_1.py', None, **jobParams)
  assert result['OK'] is True

  jobParams = {'mpTag': True, 'numberOfProcessors': 2}
  result = ce.submitJob('testPoolCEJob_1.py', None, **jobParams)
  assert result['OK'] is False

  ceParameters = {'WholeNode': True,
                  'NumberOfProcessors': 8}
  ce = PoolComputingElement('TestPoolCE')
  ce.setParameters(ceParameters)

  jobParams = {'mpTag': True, 'numberOfProcessors': 2, 'maxNumberOfProcessors': 2}
  result = ce.submitJob('testPoolCEJob_2.py', None, **jobParams)
  assert result['OK'] is True
  result = ce.getCEStatus()
  assert result['UsedProcessors'] == 2

  jobParams = {'mpTag': True, 'numberOfProcessors': 1, 'maxNumberOfProcessors': 3}
  result = ce.submitJob('testPoolCEJob_3.py', None, **jobParams)
  assert result['OK'] is True
  result = ce.getCEStatus()
  assert result['UsedProcessors'] == 5

  jobParams = {'numberOfProcessors': 2}  # This is same as asking for SP
  result = ce.submitJob('testPoolCEJob_4.py', None, **jobParams)
  assert result['OK'] is True
  result = ce.getCEStatus()
  assert result['UsedProcessors'] == 6

  jobParams = {'mpTag': True, 'numberOfProcessors': 3}
  result = ce.submitJob('testPoolCEJob_5.py', None, **jobParams)
  assert result['OK'] is False
  assert "Not enough processors" in result['Message']

  ce = PoolComputingElement('TestPoolCE')
  ceParameters = {'WholeNode': False,
                  'NumberOfProcessors': 4}
  ce.setParameters(ceParameters)

  jobParams = {'mpTag': True, 'numberOfProcessors': 2}
  result = ce.submitJob('testPoolCEJob_5.py', None, **jobParams)
  assert result['OK'] is True
  result = ce.getCEStatus()
  assert result['UsedProcessors'] == 2
  # Allow job to start
  time.sleep(15)

  for i in range(8):
    _stopJob(i)

  # Allow job to stop
  time.sleep(2)
  result = ce.getCEStatus()
  assert result['UsedProcessors'] == 0

  # Whole node jobs
  result = ce.submitJob('testPoolCEJob_0.py', None)
  assert result['OK'] is True
  result = ce.getCEStatus()
  assert result['UsedProcessors'] == 1

  jobParams = {'mpTag': True, 'wholeNode': True}
  result = ce.submitJob('testPoolCEJob_1.py', None, **jobParams)
  assert result['OK'] is False
  assert "Not enough processors for the job" in result['Message']
  # Allow job to start
  time.sleep(10)

  _stopJob(0)
  # Allow job to stop
  time.sleep(2)

  jobParams = {'mpTag': True, 'wholeNode': True}
  result = ce.submitJob('testPoolCEJob_1.py', None, **jobParams)
  assert result['OK'] is True
  result = ce.getCEStatus()
  assert result['UsedProcessors'] == 4

  # Stop all the jobs if any, cleanup tmp files
  for i in range(8):
    _stopJob(i)
    for ff in ['testPoolCEJob_%s.py' % i, 'stop_job_%s' % i]:
      if os.path.isfile(ff):
        os.unlink(ff)


def test__getProcessorsForJobs():
  ce = PoolComputingElement('TestPoolCE')
  ce.processors = 16

  kwargs = {}
  res = ce._getProcessorsForJobs(kwargs)
  assert res == 1

  kwargs = {'mpTag': False}
  res = ce._getProcessorsForJobs(kwargs)
  assert res == 1

  kwargs = {'mpTag': True}
  res = ce._getProcessorsForJobs(kwargs)
  assert res == 1

  kwargs = {'mpTag': True, 'wholeNode': True}
  res = ce._getProcessorsForJobs(kwargs)
  assert res == 16

  kwargs = {'mpTag': True, 'wholeNode': False}
  res = ce._getProcessorsForJobs(kwargs)
  assert res == 1

  kwargs = {'mpTag': True, 'numberOfProcessors': 4}
  res = ce._getProcessorsForJobs(kwargs)
  assert res == 4

  kwargs = {'mpTag': True, 'numberOfProcessors': 4, 'maxNumberOfProcessors': 8}
  res = ce._getProcessorsForJobs(kwargs)
  assert res == 8

  kwargs = {'mpTag': True, 'numberOfProcessors': 4, 'maxNumberOfProcessors': 32}
  res = ce._getProcessorsForJobs(kwargs)
  assert res == 16

  # something is in use
  ce.processorsPerTask = {1: 4}
  kwargs = {'mpTag': True, 'wholeNode': True}
  res = ce._getProcessorsForJobs(kwargs)
  assert res == 0

  kwargs = {'mpTag': True, 'wholeNode': False}
  res = ce._getProcessorsForJobs(kwargs)
  assert res == 1

  kwargs = {'mpTag': True, 'numberOfProcessors': 2}
  res = ce._getProcessorsForJobs(kwargs)
  assert res == 2

  kwargs = {'mpTag': True, 'maxNumberOfProcessors': 2}
  res = ce._getProcessorsForJobs(kwargs)
  assert res == 2

  kwargs = {'mpTag': True, 'maxNumberOfProcessors': 16}
  res = ce._getProcessorsForJobs(kwargs)
  assert res == 12
