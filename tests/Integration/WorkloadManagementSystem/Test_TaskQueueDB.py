""" This integration test only need the TaskQueueDB
    (which should of course be properly defined also in the configuration),
    and connects directly to it
"""

import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB

class TQDBTestCase( unittest.TestCase ):
  """ Base class for the JobDB test cases
  """

  def setUp( self ):
    gLogger.setLevel( 'DEBUG' )
    self.tqDB = TaskQueueDB()

  def tearDown( self ):
    pass

class TQChain( TQDBTestCase ):
  """ chaining some commands
  """

  def test_basicBasicChain( self ):
    """ a basic put - remove
    """
    tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup':'myGroup', 'Setup':'aSetup', 'CPUTime':50000}
    result = self.tqDB.insertJob( 123, tqDefDict, 10 )
    self.assert_( result['OK'] )
    result = self.tqDB.getTaskQueueForJobs( [123] )
    tq = result['Value'][123]
    result = self.tqDB.deleteTaskQueue( tq )
    self.assertFalse(result['OK']) # This will fail because of the foreign key
    result = self.tqDB.cleanOrphanedTaskQueues()
    self.assertTrue(result['OK'])
    result = self.tqDB.deleteTaskQueueIfEmpty( tq )
    self.assertTrue(result['OK'])

    tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup':'myGroup', 'Setup':'aSetup', 'CPUTime':50000}
    result = self.tqDB.insertJob( 125, tqDefDict, 10 )
    self.assertTrue(result['OK'])
    result = self.tqDB.cleanOrphanedTaskQueues()
    self.assertTrue(result['OK'])
    result = self.tqDB.getTaskQueueForJobs( [125] )
    tq = result['Value'][125]
    result = self.tqDB.deleteTaskQueue( tq )
    self.assertFalse(result['OK']) # This will fail because of the foreign key
    result = self.tqDB.cleanOrphanedTaskQueues()
    self.assertTrue(result['OK'])
    result = self.tqDB.deleteTaskQueueIfEmpty( tq )
    self.assertTrue(result['OK'])

  def test_basicChain( self ):
    """ a basic put - remove
    """
    tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup':'myGroup', 'Setup':'aSetup', 'CPUTime':50000}
    result = self.tqDB.insertJob( 123, tqDefDict, 10 )
    self.assertTrue(result['OK'])
    result = self.tqDB.getTaskQueueForJobs( [123] )
    self.assertTrue(result['OK'])
    self.assertTrue( 123 in result['Value'].keys() )
    tq = result['Value'][123]
    result = self.tqDB.deleteJob( 123 )
    self.assert_( result['OK'] )
    result = self.tqDB.cleanOrphanedTaskQueues()
    self.assertTrue( result['OK'] )
    result = self.tqDB.deleteTaskQueueIfEmpty( tq )
    self.assertTrue( result['OK'] )

class TQTests( TQDBTestCase ):
  """
  """

  def test_TQ( self ):
    """ test of various functions
    """
    tqDefDict = {'OwnerDN': '/my/DN', 'OwnerGroup':'myGroup', 'Setup':'aSetup', 'CPUTime':50000}
    self.tqDB.insertJob( 123, tqDefDict, 10 )

    result = self.tqDB.getNumTaskQueues()
    self.assertTrue(result['OK'])
    self.assertEqual( result['Value'], 1 )
    result = self.tqDB.retrieveTaskQueues()
    self.assertTrue(result['OK'])
    self.assertEqual( result['Value'].values()[0],
                      {'OwnerDN': '/my/DN', 'Jobs': 2L, 'OwnerGroup': 'myGroup',
                       'Setup': 'aSetup', 'CPUTime': 86400L, 'Priority': 1.0} )
    result = self.tqDB.findOrphanJobs()
    self.assertTrue(result['OK'])
    result = self.tqDB.recalculateTQSharesForAll()
    self.assertTrue(result['OK'])

    # this will also remove the job
    result = self.tqDB.matchAndGetJob( {'Setup': 'aSetup', 'CPUTime': 300000} )
    self.assertTrue(result['OK'])
    self.assertTrue( result['Value']['matchFound'] )
    self.assertTrue( result['Value']['jobId'] in [123L, 125L] )
    tq = result['Value']['taskQueueId']

    result = self.tqDB.deleteTaskQueueIfEmpty( tq )
    self.assertTrue(result['OK'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TQDBTestCase)
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TQChain ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TQTests ) )
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
