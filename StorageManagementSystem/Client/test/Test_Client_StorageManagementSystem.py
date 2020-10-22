""" Test for StorageManagement clients
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=protected-access,missing-docstring,invalid-name

import unittest
from mock import MagicMock, patch

from DIRAC import S_OK, S_ERROR
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import getFilesToStage
from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock
import errno

# Use side_effect instead of return_value so the dict in the result is copied
mockObjectSE1 = MagicMock()
mockObjectSE1.getFileMetadata.side_effect = lambda *_: S_OK({
    'Successful': {'/a/lfn/1.txt': {'Accessible': False}},
    'Failed': {},
})
mockObjectSE1.getStatus.side_effect = lambda *_: S_OK({'DiskSE': False, 'TapeSE': True})

mockObjectSE2 = MagicMock()
mockObjectSE2.getFileMetadata.side_effect = lambda *_: S_OK({
    'Successful': {'/a/lfn/2.txt': {'Cached': 1, 'Accessible': True}},
    'Failed': {},
})
mockObjectSE2.getStatus.side_effect = lambda *_: S_OK({'DiskSE': False, 'TapeSE': True})

mockObjectSE3 = MagicMock()
mockObjectSE3.getFileMetadata.side_effect = lambda *_: S_OK({
    'Successful': {},
    'Failed': {'/a/lfn/2.txt': 'error'},
})
mockObjectSE3.getStatus.side_effect = lambda *_: S_OK({'DiskSE': False, 'TapeSE': True})

mockObjectSE4 = MagicMock()
mockObjectSE4.getFileMetadata.side_effect = lambda *_: S_OK({
    'Successful': {},
    'Failed': {'/a/lfn/2.txt': S_ERROR(errno.ENOENT, '')['Message']}
})
mockObjectSE4.getStatus.side_effect = lambda *_: S_OK({'DiskSE': False, 'TapeSE': True})

mockObjectSE5 = MagicMock()
mockObjectSE5.getFileMetadata.side_effect = lambda *_: S_OK({
    'Successful': {'/a/lfn/1.txt': {'Accessible': False}},
    'Failed': {}
})
mockObjectSE5.getStatus.side_effect = lambda *_: S_OK({'DiskSE': True, 'TapeSE': False})

mockObjectSE6 = MagicMock()
mockObjectSE6.getFileMetadata.side_effect = lambda *_: S_OK({
    'Successful': {'/a/lfn/2.txt': {'Cached': 0, 'Accessible': False}},
    'Failed': {}
})
mockObjectSE6.getStatus.side_effect = lambda *_: S_OK({'DiskSE': False, 'TapeSE': True})


mockObjectDMSHelper = MagicMock()
mockObjectDMSHelper.getLocalSiteForSE.side_effect = lambda *_: S_OK('mySite')
mockObjectDMSHelper.getSitesForSE.side_effect = lambda *_: S_OK(['mySite'])


class ClientsTestCase(unittest.TestCase):
  """ Base class for the clients test cases
  """
  def setUp(self):
    from DIRAC import gLogger
    gLogger.setLevel('DEBUG')

  def tearDown(self):
    pass

#############################################################################


class StorageManagerSuccess(ClientsTestCase):

  @patch("DIRAC.StorageManagementSystem.Client.StorageManagerClient.DataManager", return_value=dm_mock)
  @patch("DIRAC.StorageManagementSystem.Client.StorageManagerClient.StorageElement", return_value=mockObjectSE1)
  def test_getFilesToStage_withFilesToStage(self, _patch, _patched):
    """ Test where the StorageElement mock will return files offline
    """
    res = getFilesToStage(['/a/lfn/1.txt'], checkOnlyTapeSEs=False)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['onlineLFNs'], [])
    self.assertIn(res['Value']['offlineLFNs'], [{'SE1': ['/a/lfn/1.txt']},
                                                {'SE2': ['/a/lfn/1.txt']}])
    self.assertEqual(res['Value']['absentLFNs'], {})
    self.assertEqual(res['Value']['failedLFNs'], [])

  @patch("DIRAC.StorageManagementSystem.Client.StorageManagerClient.DataManager", return_value=dm_mock)
  @patch("DIRAC.StorageManagementSystem.Client.StorageManagerClient.StorageElement", return_value=mockObjectSE2)
  def test_getFilesToStage_noFilesToStage(self, _patch, _patched):
    """ Test where the StorageElement mock will return files online
    """
    res = getFilesToStage(['/a/lfn/2.txt'], checkOnlyTapeSEs=False)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['onlineLFNs'], ['/a/lfn/2.txt'])
    self.assertEqual(res['Value']['offlineLFNs'], {})
    self.assertEqual(res['Value']['absentLFNs'], {})
    self.assertEqual(res['Value']['failedLFNs'], [])

  @patch("DIRAC.StorageManagementSystem.Client.StorageManagerClient.DataManager", return_value=dm_mock)
  @patch("DIRAC.StorageManagementSystem.Client.StorageManagerClient.StorageElement", return_value=mockObjectSE3)
  def test_getFilesToStage_seErrors(self, _patch, _patched):
    """ Test where the StorageElement will return failure
    """
    res = getFilesToStage(['/a/lfn/2.txt'], checkOnlyTapeSEs=False)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['onlineLFNs'], [])
    self.assertEqual(res['Value']['offlineLFNs'], {})
    self.assertEqual(res['Value']['absentLFNs'], {})
    self.assertEqual(res['Value']['failedLFNs'], ['/a/lfn/2.txt'])

  @patch("DIRAC.StorageManagementSystem.Client.StorageManagerClient.DataManager", return_value=dm_mock)
  @patch("DIRAC.StorageManagementSystem.Client.StorageManagerClient.StorageElement", return_value=mockObjectSE4)
  def test_getFilesToStage_noSuchFile(self, _patch, _patched):
    """ Test where the StorageElement will return file is absent
    """
    res = getFilesToStage(['/a/lfn/2.txt'], checkOnlyTapeSEs=False)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['onlineLFNs'], [])
    self.assertEqual(res['Value']['offlineLFNs'], {})
    self.assertEqual(res['Value']['absentLFNs'],
                     {'/a/lfn/2.txt': 'No such file or directory ( 2 : File not at SE1,SE2)'})
    self.assertEqual(res['Value']['failedLFNs'], [])

  @patch("DIRAC.StorageManagementSystem.Client.StorageManagerClient.DataManager", return_value=dm_mock)
  @patch("DIRAC.StorageManagementSystem.Client.StorageManagerClient.StorageElement", return_value=mockObjectSE5)
  def test_getFilesToStage_fileInaccessibleAtDisk(self, _patch, _patched):
    """ Test where the StorageElement will return file is unavailable at a Disk SE
    """
    res = getFilesToStage(['/a/lfn/1.txt'], checkOnlyTapeSEs=False)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['onlineLFNs'], [])
    self.assertEqual(res['Value']['offlineLFNs'], {})
    self.assertEqual(res['Value']['absentLFNs'], {})
    self.assertEqual(res['Value']['failedLFNs'], ['/a/lfn/1.txt'])

  @patch("DIRAC.StorageManagementSystem.Client.StorageManagerClient.DataManager", return_value=dm_mock)
  @patch("DIRAC.StorageManagementSystem.Client.StorageManagerClient.StorageElement", return_value=mockObjectSE2)
  def test_getFilesToStage_tapeSEOnly_1(self, _patch, _patched):
    """ Test where the StorageElement will return file is available
    """
    res = getFilesToStage(['/a/lfn/2.txt'], checkOnlyTapeSEs=True)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['onlineLFNs'], ['/a/lfn/2.txt'])
    self.assertEqual(res['Value']['offlineLFNs'], {})
    self.assertEqual(res['Value']['absentLFNs'], {})
    self.assertEqual(res['Value']['failedLFNs'], [])

  @patch("DIRAC.StorageManagementSystem.Client.StorageManagerClient.DataManager", return_value=dm_mock)
  @patch("DIRAC.StorageManagementSystem.Client.StorageManagerClient.StorageElement", return_value=mockObjectSE6)
  def test_getFilesToStage_tapeSEOnly_2(self, _patch, _patched):
    """ Test where the StorageElement will return file is at offline at tape
    """

    with patch("DIRAC.StorageManagementSystem.Client.StorageManagerClient.random.choice",
               new=MagicMock(return_value='SERandom')):
      res = getFilesToStage(['/a/lfn/2.txt'], checkOnlyTapeSEs=True)
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value']['onlineLFNs'], [])
    self.assertEqual(res['Value']['offlineLFNs'], {'SERandom': ['/a/lfn/2.txt']})
    self.assertEqual(res['Value']['absentLFNs'], {})
    self.assertEqual(res['Value']['failedLFNs'], [])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ClientsTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(StorageManagerSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
