""" This integration test only need the TaskQueueDB
    (which should of course be properly defined also in the configuration),
    and connects directly to it
"""

import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB


class TQDBTestCase(unittest.TestCase):
  """ Base class for the JobDB test cases
  """

  def setUp(self):
    gLogger.setLevel('DEBUG')
    self.tqDB = TaskQueueDB()

  def tearDown(self):
    pass


class TQChain(TQDBTestCase):
  """ chaining some commands
  """

  def test_basicChain(self):
    """ a basic put - remove
    """
    tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup': 'myGroup', 'Setup': 'aSetup', 'CPUTime': 50000}
    result = self.tqDB.insertJob(123, tqDefDict, 10)
    self.assertTrue(result['OK'])
    result = self.tqDB.getTaskQueueForJobs([123])
    self.assertTrue(result['OK'])
    self.assertTrue(123 in result['Value'].keys())
    tq = result['Value'][123]
    result = self.tqDB.deleteJob(123)
    self.assertTrue(result['OK'])
    result = self.tqDB.cleanOrphanedTaskQueues()
    self.assertTrue(result['OK'])
    result = self.tqDB.deleteTaskQueueIfEmpty(tq)
    self.assertTrue(result['OK'])

  def test_chainWithParameter(self):
    """ put - remove with parameters
    """
    tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup': 'myGroup', 'Setup': 'aSetup', 'CPUTime': 50000}

    # first job
    result = self.tqDB.insertJob(123, tqDefDict, 10)
    self.assertTrue(result['OK'])
    result = self.tqDB.getTaskQueueForJobs([123])
    self.assertTrue(result['OK'])
    tq = result['Value'][123]
    result = self.tqDB.deleteTaskQueue(tq)
    self.assertFalse(result['OK'])  # This will fail because of the foreign key
    result = self.tqDB.cleanOrphanedTaskQueues()
    self.assertTrue(result['OK'])
    result = self.tqDB.deleteTaskQueueIfEmpty(tq) # this won't delete anything
    self.assertTrue(result['OK'])

    # second job
    result = self.tqDB.insertJob(125, tqDefDict, 10)
    self.assertTrue(result['OK'])
    result = self.tqDB.getTaskQueueForJobs([125])
    tq = result['Value'][125]
    result = self.tqDB.deleteTaskQueue(tq)
    self.assertFalse(result['OK'])  # This will fail because of the foreign key
    result = self.tqDB.deleteTaskQueueIfEmpty(tq) # this won't delete anything, as both 123 and 125 are in
    self.assertTrue(result['OK']) # but still it won't fail
    self.assertFalse(result['Value'])
    result = self.tqDB.retrieveTaskQueues()
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'].values()[0],
                     {'OwnerDN': '/my/DN', 'Jobs': 2, 'OwnerGroup': 'myGroup',
                      'Setup': 'aSetup', 'CPUTime': 86400, 'Priority': 1.0})

    # now we will try to delete
    result = self.tqDB.deleteJob(123)
    self.assertTrue(result['OK'])
    result = self.tqDB.deleteJob(125)
    self.assertTrue(result['OK'])
    result = self.tqDB.deleteTaskQueueIfEmpty(tq) # this should now delete tq
    self.assertTrue(result['OK'])
    result = self.tqDB.retrieveTaskQueues()
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], {})

  def test_chainWithParametersComplex(self):
    """ put - remove with parameters
    """
    tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup': 'myGroup', 'Setup': 'aSetup', 'CPUTime': 50000,
                 'BannedSites':['LCG.CERN.ch', 'CLOUD.IN2P3.fr']}
    result = self.tqDB.insertJob(127, tqDefDict, 10)
    self.assertTrue(result['OK'])
    result = self.tqDB.getTaskQueueForJobs([127])
    tq = result['Value'][127]
    result = self.tqDB.deleteTaskQueueIfEmpty(tq) # this won't delete anything, as 127 is in
    self.assertTrue(result['OK']) # but still it won't fail
    self.assertFalse(result['Value'])
    result = self.tqDB.deleteJob(127)
    self.assertTrue(result['OK'])
    result = self.tqDB.deleteTaskQueueIfEmpty(tq) # this should now delete tq
    self.assertTrue(result['OK'])
    result = self.tqDB.retrieveTaskQueues()
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], {})



class TQTests(TQDBTestCase):
  """ Various other tests
  """

  def test_TQ(self):
    """ test of various functions
    """
    tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup': 'myGroup', 'Setup': 'aSetup', 'CPUTime': 50000}
    self.tqDB.insertJob(123, tqDefDict, 10)

    result = self.tqDB.getNumTaskQueues()
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], 1)
    result = self.tqDB.retrieveTaskQueues()
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'].values()[0],
                     {'OwnerDN': '/my/DN', 'Jobs': 1, 'OwnerGroup': 'myGroup',
                      'Setup': 'aSetup', 'CPUTime': 86400, 'Priority': 1.0})
    result = self.tqDB.findOrphanJobs()
    self.assertTrue(result['OK'])
    result = self.tqDB.recalculateTQSharesForAll()
    self.assertTrue(result['OK'])

    # this will also remove the job
    result = self.tqDB.matchAndGetJob({'Setup': 'aSetup', 'CPUTime': 300000})
    self.assertTrue(result['OK'])
    self.assertTrue(result['Value']['matchFound'])
    self.assertTrue(result['Value']['jobId'] in [123, 125])
    tq = result['Value']['taskQueueId']

    result = self.tqDB.deleteTaskQueueIfEmpty(tq)
    self.assertTrue(result['OK'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TQDBTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TQChain))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TQTests))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
