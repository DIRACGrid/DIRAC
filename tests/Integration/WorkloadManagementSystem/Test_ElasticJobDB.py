""" This tests only need the JobElasticDB, and connects directly to it
"""

import sys
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
    res = self.jobDB.setJobParameter(100, 'DIRAC', 'dirac@cern')
    self.assertTrue(res['OK'], res.get('Message'))
    time.sleep(1)
    res = self.jobDB.getJobParameters(100)
    self.assertTrue(res['OK'], res.get('Message'))

    self.assertEqual(res['Value'][100]['DIRAC'], 'dirac@cern', msg="Got %s" % res['Value'][100]['DIRAC'])

    # update it
    res = self.jobDB.setJobParameter(100, 'DIRAC', 'dirac@cern.cern')
    self.assertTrue(res['OK'], res.get('Message'))
    time.sleep(1)
    res = self.jobDB.getJobParameters(100)
    self.assertTrue(res['OK'], res.get('Message'))

    self.assertEqual(res['Value'][100]['DIRAC'], 'dirac@cern.cern', msg="Got %s" % res['Value'][100]['DIRAC'])

    # add one
    res = self.jobDB.setJobParameter(100, 'someKey', 'someValue')
    self.assertTrue(res['OK'], res.get('Message'))
    time.sleep(1)
    res = self.jobDB.getJobParameters(100)
    self.assertTrue(res['OK'], res.get('Message'))

    self.assertEqual(res['Value'][100]['DIRAC'], 'dirac@cern.cern', msg="Got %s" % res['Value'][100]['DIRAC'])
    self.assertEqual(res['Value'][100]['someKey'], 'someValue', msg="Got %s" % res['Value'][100]['someKey'])


if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase(JobParametersCase)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
