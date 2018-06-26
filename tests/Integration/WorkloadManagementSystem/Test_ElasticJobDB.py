""" This tests only need the JobElasticDB, and connects directly to it
"""

import unittest
import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.DB.ElasticJobDB import ElasticJobDB


class JobDBTestCase(unittest.TestCase):
  """ Base class for the JobElasticDB test cases
  """

  def setUp(self):
    gLogger.setLevel('DEBUG')
    self.jobDB = ElasticJobDB()

  def tearDown(self):
    self.jobDB = False


class JobParametersCase(JobDBTestCase):
  """  TestJobElasticDB represents a test suite for the JobElasticDB database front-end
  """

  def test_setAndGetJobFromDB(self):
    """
    test_setAndGetJobFromDB tests the functions setJobParameter and getJobParameters in
    WorkloadManagementSystem/DB/JobElasticDB.py

    Test Values:

    100: JobID (int)
    DIRAC: Name (basestring)
    dirac@cern: Value (basestring)
    """
    res = self.jobDB.setJobParameter(100, 'DIRAC', 'dirac@cern')
    self.assertTrue(res['OK'])
    time.sleep(1)
    res = self.jobDB.getJobParameters(100)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['DIRAC'], 'dirac@cern')
    res = self.jobDB.getJobParametersAndAttributes(100)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'][100]['Name'], 'DIRAC')


if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(JobParametersCase)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
