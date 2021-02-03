""" Test class for JobWrapper
"""

# pylint: disable=protected-access, invalid-name

# imports
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import os
import shutil
import pytest
from mock import MagicMock

from DIRAC import gLogger

from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock
from DIRAC.Resources.Catalog.test.mock_FC import fc_mock

from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper import JobWrapper
from DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog import Watchdog
from DIRAC.WorkloadManagementSystem.Client import JobStatus, JobMinorStatus

getSystemSectionMock = MagicMock()
getSystemSectionMock.return_value = 'aValue'

gLogger.setLevel('DEBUG')


def test_InputData(mocker):
  mocker.patch('DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection',
               side_effect=getSystemSectionMock)
  mocker.patch('DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.ModuleFactory',
               side_effect=MagicMock())

  jw = JobWrapper()
  jw.jobArgs['InputData'] = ''
  res = jw.resolveInputData()
  assert res['OK'] is False

  jw = JobWrapper()
  jw.jobArgs['InputData'] = 'pippo'
  jw.dm = dm_mock
  jw.fc = fc_mock
  res = jw.resolveInputData()
  assert res['OK']

  jw = JobWrapper()
  jw.jobArgs['InputData'] = 'pippo'
  jw.jobArgs['LocalSE'] = 'mySE'
  jw.jobArgs['InputDataModule'] = 'aa.bb'
  jw.dm = dm_mock
  jw.fc = fc_mock
  res = jw.resolveInputData()
  assert res['OK']


def test_performChecks():
  wd = Watchdog(pid=os.getpid(),
                exeThread=MagicMock(),
                spObject=MagicMock(),
                jobCPUTime=1000,
                memoryLimit=1024 * 1024,
                jobArgs={'StopSigNumber': 10})
  res = wd._performChecks()
  assert res['OK']


@pytest.mark.slow
def test_execute(mocker):

  mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection",
               side_effect=getSystemSectionMock)
  mocker.patch("DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog.getSystemInstance",
               side_effect=getSystemSectionMock)

  jw = JobWrapper()
  jw.jobArgs = {'Executable': '/bin/ls'}
  res = jw.execute()
  print('jw.execute() returns', str(res))
  assert res['OK']

  shutil.copy('src/DIRAC/WorkloadManagementSystem/JobWrapper/test/script-OK.sh', 'script-OK.sh')
  jw = JobWrapper()
  jw.jobArgs = {'Executable': 'script-OK.sh'}
  res = jw.execute()
  assert res['OK']
  os.remove('script-OK.sh')

  shutil.copy('src/DIRAC/WorkloadManagementSystem/JobWrapper/test/script.sh', 'script.sh')
  jw = JobWrapper()
  jw.jobArgs = {'Executable': 'script.sh', 'Arguments': '111'}
  res = jw.execute()
  assert res['OK']  # In this case the application finished with errors,
  # but the JobWrapper executed successfully
  os.remove('script.sh')

  # this will reschedule
  shutil.copy('src/DIRAC/WorkloadManagementSystem/JobWrapper/test/script-RESC.sh', 'script-RESC.sh')
  jw = JobWrapper()
  jw.jobArgs = {'Executable': 'script-RESC.sh'}
  res = jw.execute()
  if res['OK']:  # FIXME: This may happen depending on the shell - not the best test admittedly!
    print("We should not be here, unless the 'Execution thread status' is equal to 1")
    assert res['OK']
  else:
    assert res['OK'] is False  # In this case the application finished with an error code
    # that the JobWrapper interpreted as "to reschedule"
    # so in this case the "execute" is considered an error
  os.remove('script-RESC.sh')
  os.remove('std.out')


@pytest.mark.parametrize("failedFlag, expectedRes, finalStates", [
    (True, 1, [JobStatus.FAILED, '']),
    (False, 0, [JobStatus.DONE, JobMinorStatus.EXEC_COMPLETE]),
])
def test_finalize(mocker, failedFlag, expectedRes, finalStates):
  mocker.patch('DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection',
               side_effect=getSystemSectionMock)
  mocker.patch('DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.ModuleFactory',
               side_effect=MagicMock())

  jw = JobWrapper()
  jw.jobArgs = {'Executable': '/bin/ls'}
  jw.failedFlag = failedFlag

  res = jw.finalize()

  assert res == expectedRes
  assert jw.jobReport.jobStatusInfo[0][0] == finalStates[0]
  assert jw.jobReport.jobStatusInfo[0][1] == finalStates[1]
