""" This is a test of the chain
    TransformationClient -> TransformationManagerHandler -> TransformationDB

    It supposes that the DB is present, and that the service is running
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest

from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient

class TestClientTransformationTestCase( unittest.TestCase ):

  def setUp( self ):
    self.transClient = TransformationClient()

  def tearDown( self ):
    pass


class TransformationClientChain( TestClientTransformationTestCase ):

  def test_addAndRemove( self ):
    # add
    res = self.transClient.addTransformation( 'transName', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '' )
    self.assert_( res['OK'] )
    transID = res['Value']

    # try to add again (this should fail)
    res = self.transClient.addTransformation( 'transName', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '' )
    self.assertFalse( res['OK'] )

    # clean
    res = self.transClient.cleanTransformation( transID )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationParameters( transID, 'Status' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'TransformationCleaned' )

    # really delete
    res = self.transClient.deleteTransformation( transID )
    self.assert_( res['OK'] )

    # delete non existing one (fails)
    res = self.transClient.deleteTransformation( transID )
    self.assertFalse( res['OK'] )


  def test_addTasksAndFiles( self ):
    res = self.transClient.addTransformation( 'transName', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '' )
    transID = res['Value']

    # add tasks - no lfns
    res = self.transClient.addTaskForTransformation( transID )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationTasks( {'TransformationID': transID} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 1 )
    res = self.transClient.getTransformationFiles( {'TransformationID': transID} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 0 )

    # add tasks - with lfns
    res = self.transClient.addTaskForTransformation( transID, ['/aa/lfn.1.txt', '/aa/lfn.2.txt'] )
    # fails because the files are not present
    self.assertFalse( res['OK'] )
    # so now adding them
    res = self.transClient.addFilesToTransformation( transID, ['/aa/lfn.1.txt', '/aa/lfn.2.txt',
                                                               '/aa/lfn.3.txt', '/aa/lfn.4.txt'] )
    self.assert_( res['OK'] )

    # now it should be ok
    res = self.transClient.addTaskForTransformation( transID, ['/aa/lfn.1.txt', '/aa/lfn.2.txt'] )
    self.assert_( res['OK'] )
    res = self.transClient.addTaskForTransformation( transID, ['/aa/lfn.3.txt', '/aa/lfn.4.txt'] )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationTasks( {'TransformationID': transID} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 3 )
    index = 1
    for task in res['Value']:
      self.assertEqual( task['ExternalStatus'], 'Created' )
      self.assertEqual( task['TaskID'], index )
      index += 1
    res = self.transClient.getTransformationFiles( {'TransformationID': transID} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 4 )
    for f in res['Value']:
      self.assertEqual( f['Status'], 'Assigned' )

    # now adding a new Transformation with new tasks, and introducing a mix of insertion,
    # to test that the trigger works as it should
    res = self.transClient.addTransformation( 'transName-new', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '' )
    transIDNew = res['Value']
    # add tasks - no lfns
    res = self.transClient.addTaskForTransformation( transIDNew )
    self.assert_( res['OK'] )
    res = self.transClient.addTaskForTransformation( transIDNew )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationTasks( {'TransformationID': transIDNew} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 2 )
    index = 1
    for task in res['Value']:
      self.assertEqual( task['ExternalStatus'], 'Created' )
      self.assertEqual( task['TaskID'], index )
      index += 1
    # now mixing things
    res = self.transClient.addTaskForTransformation( transID )
    self.assert_( res['OK'] )
    res = self.transClient.addTaskForTransformation( transIDNew )
    self.assert_( res['OK'] )
    res = self.transClient.addTaskForTransformation( transID )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationTasks( {'TransformationID': transID} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 5 )
    index = 1
    for task in res['Value']:
      self.assertEqual( task['ExternalStatus'], 'Created' )
      self.assertEqual( task['TaskID'], index )
      index += 1
    res = self.transClient.getTransformationTasks( {'TransformationID': transIDNew} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 3 )
    index = 1
    for task in res['Value']:
      self.assertEqual( task['ExternalStatus'], 'Created' )
      self.assertEqual( task['TaskID'], index )
      index += 1

    # clean
    res = self.transClient.cleanTransformation( transID )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationFiles( {'TransformationID': transID} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 0 )
    res = self.transClient.getTransformationTasks( {'TransformationID': transID} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 0 )

    res = self.transClient.cleanTransformation( transIDNew )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationFiles( {'TransformationID': transIDNew} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 0 )
    res = self.transClient.getTransformationTasks( {'TransformationID': transIDNew} )
    self.assert_( res['OK'] )
    self.assertEqual( len( res['Value'] ), 0 )

    # delete it in the end
    self.transClient.deleteTransformation( transID )
    self.transClient.deleteTransformation( transIDNew )

  def test_mix( self ):
    res = self.transClient.addTransformation( 'transName', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '' )
    transID = res['Value']

    # parameters
    res = self.transClient.setTransformationParameter( transID, 'aParamName', 'aParamValue' )
    self.assert_( res['OK'] )
    res1 = self.transClient.getTransformationParameters( transID, 'aParamName' )
    self.assert_( res1['OK'] )
    res2 = self.transClient.getTransformationParameters( transID, ( 'aParamName', ) )
    self.assert_( res2['OK'] )
    res3 = self.transClient.getTransformationParameters( transID, ['aParamName'] )
    self.assert_( res3['OK'] )
    self.assert_( res1['Value'] == res2['Value'] == res3['Value'] )

    # file status
    lfns = ['/aa/lfn.1.txt', '/aa/lfn.2.txt', '/aa/lfn.3.txt', '/aa/lfn.4.txt']
    res = self.transClient.addFilesToTransformation( transID, lfns )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationFiles( {'TransformationID':transID, 'LFN': lfns} )
    self.assert_( res['OK'] )
    for f in res['Value']:
      self.assertEqual( f['Status'], 'Unused' )
    res = self.transClient.setFileStatusForTransformation( transID, 'Assigned', lfns )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationFiles( {'TransformationID':transID, 'LFN': lfns} )
    for f in res['Value']:
      self.assertEqual( f['Status'], 'Assigned' )
    res = self.transClient.getTransformationStats( transID )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {'Assigned': 4L, 'Total': 4L} )
    res = self.transClient.setFileStatusForTransformation( transID, 'Unused', lfns )
    # tasks
    res = self.transClient.addTaskForTransformation( transID, lfns )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationTasks( {'TransformationID': transID} )
    self.assert_( res['OK'] )
    taskIDs = []
    for task in res['Value']:
      self.assertEqual( task['ExternalStatus'], 'Created' )
      taskIDs.append( task['TaskID'] )
    self.transClient.setTaskStatus( transID, taskIDs, 'Running' )
    res = self.transClient.getTransformationTasks( {'TransformationID': transID} )
    for task in res['Value']:
      self.assertEqual( task['ExternalStatus'], 'Running' )
    res = self.transClient.extendTransformation( transID, 5 )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationTasks( {'TransformationID': transID} )
    self.assertEqual( len( res['Value'] ), 6 )
    res = self.transClient.getTasksToSubmit( transID, 5 )
    self.assert_( res['OK'] )

    # logging
    res = self.transClient.setTransformationParameter( transID, 'Status', 'Active' )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationLogging( transID )
    self.assert_( res['OK'] )
    self.assertAlmostEqual( len( res['Value'] ), 4 )

    # delete it in the end
    self.transClient.cleanTransformation( transID )
    self.transClient.deleteTransformation( transID )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestClientTransformationTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TransformationClientChain ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
