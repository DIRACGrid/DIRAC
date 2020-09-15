""" This is a test of the chain
    TransformationClient -> TransformationManagerHandler -> TransformationDB

    It supposes that the DB is present, and that the service is running
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=invalid-name,wrong-import-position

import unittest
import sys

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient


class TestClientTransformationTestCase(unittest.TestCase):

  def setUp(self):
    self.transClient = TransformationClient()

  def tearDown(self):
    pass


class TransformationClientChain(TestClientTransformationTestCase):

  def test_addAndRemove(self):
    # add
    res = self.transClient.addTransformation('transName', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                             'Manual', '')
    self.assertTrue(res['OK'])
    transID = res['Value']

    # try to add again (this should fail)
    res = self.transClient.addTransformation('transName', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                             'Manual', '')
    self.assertFalse(res['OK'])

    # clean
    res = self.transClient.cleanTransformation(transID)
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationParameters(transID, 'Status')
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], 'TransformationCleaned')

    # really delete
    res = self.transClient.deleteTransformation(transID)
    self.assertTrue(res['OK'])

    # delete non existing one (fails)
    res = self.transClient.deleteTransformation(transID)
    self.assertFalse(res['OK'])

  def test_addTasksAndFiles(self):
    res = self.transClient.addTransformation('transName', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                             'Manual', '')
    self.assertTrue(res['OK'])
    transID = res['Value']

    # add tasks - no lfns
    res = self.transClient.addTaskForTransformation(transID)
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationTasks({'TransformationID': transID})
    self.assertTrue(res['OK'])
    self.assertEqual(len(res['Value']), 1)
    res = self.transClient.getTransformationFiles({'TransformationID': transID})
    self.assertTrue(res['OK'])
    self.assertEqual(len(res['Value']), 0)

    # add tasks - with lfns
    res = self.transClient.addTaskForTransformation(transID, ['/aa/lfn.1.txt', '/aa/lfn.2.txt'])
    # fails because the files are not present
    self.assertFalse(res['OK'])
    # so now adding them
    res = self.transClient.addFilesToTransformation(transID, ['/aa/lfn.1.txt', '/aa/lfn.2.txt',
                                                              '/aa/lfn.3.txt', '/aa/lfn.4.txt'])
    self.assertTrue(res['OK'])

    # now it should be ok
    res = self.transClient.addTaskForTransformation(transID, ['/aa/lfn.1.txt', '/aa/lfn.2.txt'])
    self.assertTrue(res['OK'])
    res = self.transClient.addTaskForTransformation(transID, ['/aa/lfn.3.txt', '/aa/lfn.4.txt'])
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationTasks({'TransformationID': transID})
    self.assertTrue(res['OK'])
    self.assertEqual(len(res['Value']), 3)
    index = 1
    for task in res['Value']:
      self.assertEqual(task['ExternalStatus'], 'Created')
      self.assertEqual(task['TaskID'], index)
      index += 1
    res = self.transClient.getTransformationFiles({'TransformationID': transID})
    self.assertTrue(res['OK'])
    self.assertEqual(len(res['Value']), 4)
    for f in res['Value']:
      self.assertEqual(f['Status'], 'Assigned')

    # now adding a new Transformation with new tasks, and introducing a mix of insertion,
    # to test that the trigger works as it should
    res = self.transClient.addTransformation(
        'transName-new',
        'description',
        'longDescription',
        'MCSimulation',
        'Standard',
        'Manual',
        '')
    transIDNew = res['Value']
    # add tasks - no lfns
    res = self.transClient.addTaskForTransformation(transIDNew)
    self.assertTrue(res['OK'])
    res = self.transClient.addTaskForTransformation(transIDNew)
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationTasks({'TransformationID': transIDNew})
    self.assertTrue(res['OK'])
    self.assertEqual(len(res['Value']), 2)
    index = 1
    for task in res['Value']:
      self.assertEqual(task['ExternalStatus'], 'Created')
      self.assertEqual(task['TaskID'], index)
      index += 1
    # now mixing things
    res = self.transClient.addTaskForTransformation(transID)
    self.assertTrue(res['OK'])
    res = self.transClient.addTaskForTransformation(transIDNew)
    self.assertTrue(res['OK'])
    res = self.transClient.addTaskForTransformation(transID)
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationTasks({'TransformationID': transID})
    self.assertTrue(res['OK'])
    self.assertEqual(len(res['Value']), 5)
    index = 1
    for task in res['Value']:
      self.assertEqual(task['ExternalStatus'], 'Created')
      self.assertEqual(task['TaskID'], index)
      index += 1
    res = self.transClient.getTransformationTasks({'TransformationID': transIDNew})
    self.assertTrue(res['OK'])
    self.assertEqual(len(res['Value']), 3)
    index = 1
    for task in res['Value']:
      self.assertEqual(task['ExternalStatus'], 'Created')
      self.assertEqual(task['TaskID'], index)
      index += 1

    # clean
    res = self.transClient.cleanTransformation(transID)
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationFiles({'TransformationID': transID})
    self.assertTrue(res['OK'])
    self.assertEqual(len(res['Value']), 0)
    res = self.transClient.getTransformationTasks({'TransformationID': transID})
    self.assertTrue(res['OK'])
    self.assertEqual(len(res['Value']), 0)

    res = self.transClient.cleanTransformation(transIDNew)
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationFiles({'TransformationID': transIDNew})
    self.assertTrue(res['OK'])
    self.assertEqual(len(res['Value']), 0)
    res = self.transClient.getTransformationTasks({'TransformationID': transIDNew})
    self.assertTrue(res['OK'])
    self.assertEqual(len(res['Value']), 0)

    # delete it in the end
    self.transClient.deleteTransformation(transID)
    self.transClient.deleteTransformation(transIDNew)

  def test_mix(self):
    res = self.transClient.addTransformation('transName', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                             'Manual', '')
    self.assertTrue(res['OK'])
    transID = res['Value']

    # parameters
    res = self.transClient.setTransformationParameter(transID, 'aParamName', 'aParamValue')
    self.assertTrue(res['OK'])
    res1 = self.transClient.getTransformationParameters(transID, 'aParamName')
    self.assertTrue(res1['OK'])
    res2 = self.transClient.getTransformationParameters(transID, ['aParamName'])
    self.assertTrue(res2['OK'])

    # file status
    lfns = ['/aa/lfn.1.txt', '/aa/lfn.2.txt', '/aa/lfn.3.txt', '/aa/lfn.4.txt']
    res = self.transClient.addFilesToTransformation(transID, lfns)
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationFiles({'TransformationID': transID, 'LFN': lfns})
    self.assertTrue(res['OK'])
    for f in res['Value']:
      self.assertEqual(f['Status'], 'Unused')
      self.assertEqual(f['ErrorCount'], 0)
    res = self.transClient.setFileStatusForTransformation(transID, 'Assigned', lfns)
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationFiles({'TransformationID': transID, 'LFN': lfns})
    for f in res['Value']:
      self.assertEqual(f['Status'], 'Assigned')
      self.assertEqual(f['ErrorCount'], 0)
    res = self.transClient.getTransformationStats(transID)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], {'Assigned': 4, 'Total': 4})
    # Setting files MaxReset from Assigned should increment ErrorCount
    res = self.transClient.setFileStatusForTransformation(transID, 'MaxReset', lfns)
    res = self.transClient.getTransformationFiles({'TransformationID': transID, 'LFN': lfns})
    self.assertTrue(res['OK'])
    for f in res['Value']:
      self.assertEqual(f['Status'], 'MaxReset')
      self.assertEqual(f['ErrorCount'], 1)
    # Cycle through Unused -> Assigned This should not increment ErrorCount
    res = self.transClient.setFileStatusForTransformation(transID, 'Unused', lfns)
    self.assertTrue(res['OK'])
    res = self.transClient.setFileStatusForTransformation(transID, 'Assigned', lfns)
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationFiles({'TransformationID': transID, 'LFN': lfns})
    self.assertTrue(res['OK'])
    for f in res['Value']:
      self.assertEqual(f['Status'], 'Assigned')
      self.assertEqual(f['ErrorCount'], 1)
    # Resetting files Unused from Assigned should increment ErrorCount
    res = self.transClient.setFileStatusForTransformation(transID, 'Unused', lfns)
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationFiles({'TransformationID': transID, 'LFN': lfns})
    self.assertTrue(res['OK'])
    for f in res['Value']:
      self.assertEqual(f['Status'], 'Unused')
      self.assertEqual(f['ErrorCount'], 2)
    res = self.transClient.setFileStatusForTransformation(transID, 'Assigned', lfns)
    self.assertTrue(res['OK'])
    # Set files Processed
    res = self.transClient.setFileStatusForTransformation(transID, 'Processed', lfns)
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationFiles({'TransformationID': transID, 'LFN': lfns})
    self.assertTrue(res['OK'])
    for f in res['Value']:
      self.assertEqual(f['Status'], 'Processed')
      self.assertEqual(f['ErrorCount'], 2)
    # Setting files Unused should have no effect
    res = self.transClient.setFileStatusForTransformation(transID, 'Unused', lfns)
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationFiles({'TransformationID': transID, 'LFN': lfns})
    self.assertTrue(res['OK'])
    for f in res['Value']:
      self.assertEqual(f['Status'], 'Processed')
      self.assertEqual(f['ErrorCount'], 2)
    # Forcing files Unused should work
    res = self.transClient.setFileStatusForTransformation(transID, 'Unused', lfns, force=True)
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationFiles({'TransformationID': transID, 'LFN': lfns})
    self.assertTrue(res['OK'])
    for f in res['Value']:
      self.assertEqual(f['Status'], 'Unused')
      self.assertEqual(f['ErrorCount'], 2)
    # tasks
    res = self.transClient.addTaskForTransformation(transID, lfns)
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationTasks({'TransformationID': transID})
    self.assertTrue(res['OK'])
    taskIDs = []
    for task in res['Value']:
      self.assertEqual(task['ExternalStatus'], 'Created')
      taskIDs.append(task['TaskID'])
    self.transClient.setTaskStatus(transID, taskIDs, 'Running')
    res = self.transClient.getTransformationTasks({'TransformationID': transID})
    for task in res['Value']:
      self.assertEqual(task['ExternalStatus'], 'Running')
    res = self.transClient.extendTransformation(transID, 5)
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationTasks({'TransformationID': transID})
    self.assertEqual(len(res['Value']), 6)
    res = self.transClient.getTasksToSubmit(transID, 5)
    self.assertTrue(res['OK'])

    # logging
    res = self.transClient.setTransformationParameter(transID, 'Status', 'Active')
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationLogging(transID)
    self.assertTrue(res['OK'])
    self.assertAlmostEqual(len(res['Value']), 4)

    # delete it in the end
    self.transClient.cleanTransformation(transID)
    self.transClient.deleteTransformation(transID)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestClientTransformationTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TransformationClientChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
