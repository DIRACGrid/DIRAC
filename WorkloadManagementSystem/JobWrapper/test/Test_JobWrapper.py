""" Test class for JobWrapper
"""

# pylint: disable=protected-access, invalid-name

# imports
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import unittest
import importlib
import os
import shutil

from mock import MagicMock, patch

from DIRAC import gLogger

from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock
from DIRAC.Resources.Catalog.test.mock_FC import fc_mock

from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper import JobWrapper
from DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog import Watchdog

getSystemSectionMock = MagicMock()
getSystemSectionMock.return_value = 'aValue'


class JobWrapperTestCase(unittest.TestCase):
  """ Base class for the JobWrapper test cases
  """

  def setUp(self):
    gLogger.setLevel('DEBUG')

  def tearDown(self):
    for f in ['std.out']:
      try:
        os.remove(f)
      except OSError:
        pass


class JobWrapperTestCaseSuccess(JobWrapperTestCase):

  def test_InputData(self):
    myJW = importlib.import_module('DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper')
    myJW.getSystemSection = MagicMock()
    myJW.ModuleFactory = MagicMock()

    jw = JobWrapper()

    jw.jobArgs['InputData'] = ''
    res = jw.resolveInputData()
    self.assertFalse(res['OK'])

    jw = JobWrapper()
    jw.jobArgs['InputData'] = 'pippo'
    jw.dm = dm_mock
    jw.fc = fc_mock
    res = jw.resolveInputData()
    self.assertTrue(res['OK'])

    jw = JobWrapper()
    jw.jobArgs['InputData'] = 'pippo'
    jw.jobArgs['LocalSE'] = 'mySE'
    jw.jobArgs['InputDataModule'] = 'aa.bb'
    jw.dm = dm_mock
    jw.fc = fc_mock
    res = jw.resolveInputData()
    self.assertTrue(res['OK'])

  def test__performChecks(self):
    wd = Watchdog(pid=os.getpid(),
                  exeThread=MagicMock(),
                  spObject=MagicMock(),
                  jobCPUTime=1000,
                  memoryLimit=1024 * 1024,
                  jobArgs={'StopSigNumber': 10})
    res = wd._performChecks()
    self.assertTrue(res['OK'])

  @patch("DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper.getSystemSection", side_effect=getSystemSectionMock)
  @patch("DIRAC.WorkloadManagementSystem.JobWrapper.Watchdog.getSystemInstance", side_effect=getSystemSectionMock)
  def test_execute(self, _patch1, _patch2):
    jw = JobWrapper()
    jw.jobArgs = {'Executable': '/bin/ls'}
    res = jw.execute()
    print('jw.execute() returns', str(res))
    self.assertTrue(res['OK'])

    shutil.copy('WorkloadManagementSystem/JobWrapper/test/script-OK.sh', 'script-OK.sh')
    jw = JobWrapper()
    jw.jobArgs = {'Executable': 'script-OK.sh'}
    res = jw.execute()
    self.assertTrue(res['OK'])
    os.remove('script-OK.sh')

    shutil.copy('WorkloadManagementSystem/JobWrapper/test/script.sh', 'script.sh')
    jw = JobWrapper()
    jw.jobArgs = {'Executable': 'script.sh', 'Arguments': '111'}
    res = jw.execute()
    self.assertTrue(res['OK'])  # In this case the application finished with errors,
    # but the JobWrapper executed successfully
    os.remove('script.sh')

    shutil.copy('WorkloadManagementSystem/JobWrapper/test/script-RESC.sh', 'script-RESC.sh')  # this will reschedule
    jw = JobWrapper()
    jw.jobArgs = {'Executable': 'script-RESC.sh'}
    res = jw.execute()
    if res['OK']:  # FIXME: This may happen depending on the shell - not the best test admittedly!
      print("We should not be here, unless the 'Execution thread status' is equal to 1")
      self.assertTrue(res['OK'])
    else:
      self.assertFalse(res['OK'])  # In this case the application finished with an error code
      # that the JobWrapper interpreted as "to reschedule"
      # so in this case the "execute" is considered an error
    os.remove('script-RESC.sh')

  def test_finalize(self):
    jw = JobWrapper()
    jw.jobArgs = {'Executable': '/bin/ls'}
    res = jw.finalize()
    self.assertTrue(res == 1)  # by default failed flag is True


#############################################################################
# Test Suite run
#############################################################################


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(JobWrapperTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(JobWrapperTestCaseSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
