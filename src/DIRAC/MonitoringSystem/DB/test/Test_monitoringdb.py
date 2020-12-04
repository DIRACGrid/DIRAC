'''
It is used to test the database modules...
'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import DIRAC.MonitoringSystem.private.DBUtils as moduleTested

__RCSID__ = "$Id$"

################################################################################


class Test_DB(unittest.TestCase):
  """
  """

  def setUp(self):
    '''
    Setup
    '''

    self.moduleTested = moduleTested
    self.testClass = self.moduleTested.DBUtils

  def tearDown(self):
    '''
    Tear down
    '''

    del self.moduleTested
    del self.testClass

################################################################################


class TestDbUtilsChain(Test_DB):
  """
  """

  ################################################################################
  def test_instantiate(self):
    ''' tests that we can instantiate one object of the tested class
    '''

    module = self.testClass("MonitoringDB", 'Test')
    self.assertEqual('DBUtils', module.__class__.__name__)

  ################################################################################
  def test_determineBucketSize(self):

    client = self.testClass("MonitoringDB", 'Test')
    result = client._determineBucketSize(1458130176, 1458226213)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], ('30m', 1800))

    result = client._determineBucketSize(1458055269, 1458660069)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], ('2h', 7200))

    result = client._determineBucketSize(1458617047, 1458660247)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], ('10m', 600))

    result = client._determineBucketSize(1456068334, 1458660334)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], ('8h', 28800))

    result = client._determineBucketSize(1453476373, 1458660373)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], ('12h', 43200))

    result = client._determineBucketSize(1450884571, 1458660571)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], ('1d', 86400))

    result = client._determineBucketSize(1427038370, 1458660770)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], ('4d', 345600))

    result = client._determineBucketSize(1300807841, 1458660641)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], ('7d', 604800))


if __name__ == '__main__':
  testSuite = unittest.defaultTestLoader.loadTestsFromTestCase(Test_DB)
  testSuite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestDbUtilsChain))
  unittest.TextTestRunner(verbosity=2).run(testSuite)
