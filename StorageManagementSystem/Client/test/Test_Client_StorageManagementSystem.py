""" Test for StorageManagement clients
"""

# pylint: disable=protected-access,missing-docstring,invalid-name

import unittest
from mock import MagicMock, patch

from DIRAC import S_OK, S_ERROR
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import getFilesToStage
from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock
import errno

mockObjectSE1 = MagicMock()
mockObjectSE1.getFileMetadata.return_value = S_OK( {'Successful':{'/a/lfn/1.txt':{'Accessible':False}},
                                                    'Failed':{}} )
mockObjectSE1.getStatus.return_value = S_OK( {'DiskSE': False, 'TapeSE':True} )

mockObjectSE2 = MagicMock()
mockObjectSE2.getFileMetadata.return_value = S_OK( {'Successful':{'/a/lfn/2.txt':{'Cached':1, 'Accessible':True}},
                                                    'Failed':{}} )
mockObjectSE2.getStatus.return_value = S_OK( {'DiskSE': False, 'TapeSE':True} )

mockObjectSE3 = MagicMock()
mockObjectSE3.getFileMetadata.return_value = S_OK( {'Successful':{},
                                                    'Failed':{'/a/lfn/2.txt': 'error'}} )
mockObjectSE3.getStatus.return_value = S_OK( {'DiskSE': False, 'TapeSE':True} )

mockObjectSE4 = MagicMock()
mockObjectSE4.getFileMetadata.return_value = S_OK( {'Successful':{},
                                                    'Failed':{'/a/lfn/2.txt':
                                                              S_ERROR( errno.ENOENT, '' )['Message']}} )
mockObjectSE4.getStatus.return_value = S_OK( {'DiskSE': False, 'TapeSE':True} )

mockObjectSE5 = MagicMock()
mockObjectSE5.getFileMetadata.return_value = S_OK( {'Successful':{'/a/lfn/1.txt':{'Accessible':False}},
                                                    'Failed':{}} )
mockObjectSE5.getStatus.return_value = S_OK( {'DiskSE': True, 'TapeSE':False} )

mockObjectDMSHelper = MagicMock()
mockObjectDMSHelper.getLocalSiteForSE.return_value = S_OK( 'mySite' )
mockObjectDMSHelper.getSitesForSE.return_value = S_OK( ['mySite'] )

class ClientsTestCase( unittest.TestCase ):
  """ Base class for the clients test cases
  """
  def setUp( self ):

    from DIRAC import gLogger
    gLogger.setLevel( 'DEBUG' )

  def tearDown( self ):
    pass

#############################################################################

class StorageManagerSuccess( ClientsTestCase ):

  @patch( "DIRAC.StorageManagementSystem.Client.StorageManagerClient.DataManager", return_value = dm_mock )
  @patch( "DIRAC.StorageManagementSystem.Client.StorageManagerClient.StorageElement", return_value = mockObjectSE1 )
  def test_getFilesToStage_withFilesToStage( self, _patch, _patched ):
    """ Test where the StorageElement mock will return files offline
    """
    res = getFilesToStage( ['/a/lfn/1.txt'], checkOnlyTapeSEs = False )
    self.assertTrue( res['OK'] )
    self.assertEqual( res['Value']['onlineLFNs'], [] )
    self.assertIn( res['Value']['offlineLFNs'], [{'SE1':['/a/lfn/1.txt']},
                                                 {'SE2':['/a/lfn/1.txt']}] )
    self.assertEqual( res['Value']['absentLFNs'], {} )
    self.assertEqual( res['Value']['failedLFNs'], [] )

  @patch( "DIRAC.StorageManagementSystem.Client.StorageManagerClient.DataManager", return_value = dm_mock )
  @patch( "DIRAC.StorageManagementSystem.Client.StorageManagerClient.StorageElement", return_value = mockObjectSE2 )
  def test_getFilesToStage_noFilesToStage( self, _patch, _patched ):
    """ Test where the StorageElement mock will return files online
    """
    res = getFilesToStage( ['/a/lfn/2.txt'], checkOnlyTapeSEs = False )
    self.assertTrue( res['OK'] )
    self.assertEqual( res['Value']['onlineLFNs'], ['/a/lfn/2.txt'] )
    self.assertEqual( res['Value']['offlineLFNs'], {} )
    self.assertEqual( res['Value']['absentLFNs'], {} )
    self.assertEqual( res['Value']['failedLFNs'], [] )

  @patch( "DIRAC.StorageManagementSystem.Client.StorageManagerClient.DataManager", return_value = dm_mock )
  @patch( "DIRAC.StorageManagementSystem.Client.StorageManagerClient.StorageElement", return_value = mockObjectSE3 )
  def test_getFilesToStage_seErrors( self, _patch, _patched ):
    """ Test where the StorageElement will return failure
    """
    res = getFilesToStage( ['/a/lfn/2.txt'], checkOnlyTapeSEs = False )
    self.assertTrue( res['OK'] )
    self.assertEqual( res['Value']['onlineLFNs'], [] )
    self.assertEqual( res['Value']['offlineLFNs'], {} )
    self.assertEqual( res['Value']['absentLFNs'], {} )
    self.assertEqual( res['Value']['failedLFNs'], ['/a/lfn/2.txt'] )

  @patch( "DIRAC.StorageManagementSystem.Client.StorageManagerClient.DataManager", return_value = dm_mock )
  @patch( "DIRAC.StorageManagementSystem.Client.StorageManagerClient.StorageElement", return_value = mockObjectSE4 )
  def test_getFilesToStage_noSuchFile( self, _patch, _patched ):
    """ Test where the StorageElement will return file is absent
    """
    res = getFilesToStage( ['/a/lfn/2.txt'], checkOnlyTapeSEs = False )
    self.assertTrue( res['OK'] )
    self.assertEqual( res['Value']['onlineLFNs'], [] )
    self.assertEqual( res['Value']['offlineLFNs'], {} )
    self.assertEqual( res['Value']['absentLFNs'], {'/a/lfn/2.txt': 'No such file or directory ( 2 : File not at SE2)'} )
    self.assertEqual( res['Value']['failedLFNs'], [] )

  @patch( "DIRAC.StorageManagementSystem.Client.StorageManagerClient.DataManager", return_value = dm_mock )
  @patch( "DIRAC.StorageManagementSystem.Client.StorageManagerClient.StorageElement", return_value = mockObjectSE5 )
  def test_getFilesToStage_fileInaccessibleAtDisk( self, _patch, _patched ):
    """ Test where the StorageElement will return file is unavailable at a Disk SE
    """
    res = getFilesToStage( ['/a/lfn/1.txt'], checkOnlyTapeSEs = False )
    self.assertTrue( res['OK'] )
    self.assertEqual( res['Value']['onlineLFNs'], [] )
    self.assertEqual( res['Value']['offlineLFNs'], {} )
    self.assertEqual( res['Value']['absentLFNs'], {} )
    self.assertEqual( res['Value']['failedLFNs'], ['/a/lfn/1.txt'] )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( StorageManagerSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
