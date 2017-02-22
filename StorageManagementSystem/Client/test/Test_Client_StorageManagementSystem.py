""" Test for StorageManagement clients
"""

# pylint: disable=protected-access,missing-docstring,invalid-name

import unittest
from mock import MagicMock, patch

from DIRAC import S_OK
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import getFilesToStage
from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock

mockObjectSE = MagicMock()
mockObjectSE.getFileMetadata.return_value = S_OK( {'Successful':{'/a/lfn/1.txt':{'Accessible':False},
                                                                 '/a/lfn/2.txt':{'Cached':1, 'Accessible':True}},
                                                   'Failed':{}} )
mockObjectSE.getStatus.return_value = S_OK( {'DiskSE': False, 'TapeSE':True} )


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
  @patch( "DIRAC.StorageManagementSystem.Client.StorageManagerClient.StorageElement", return_value = MagicMock() )
  def test_getFilesToStage( self, _patch, _patched ):
    """ Simple test - the StorageElement mock will return all the files online
    """
    res = getFilesToStage( [] )
    self.assertTrue( res['OK'] )
    self.assertEqual( res['Value']['onlineLFNs'], [] )
    self.assertEqual( res['Value']['offlineLFNs'], {} )

    res = getFilesToStage( ['/a/lfn/1.txt'] )
    self.assertTrue( res['OK'] )
    self.assertEqual( res['Value']['onlineLFNs'], ['/a/lfn/1.txt', '/a/lfn/2.txt'] )
    self.assertEqual( res['Value']['offlineLFNs'], {} )

  @patch( "DIRAC.StorageManagementSystem.Client.StorageManagerClient.DataManager", return_value = dm_mock )
  @patch( "DIRAC.StorageManagementSystem.Client.StorageManagerClient.StorageElement", return_value = mockObjectSE )
  def test_getFilesToStage_withFilesToStage( self, _patch, _patched ):
    """ Test where the StorageElement mock will return files offline
    """
    res = getFilesToStage( ['/a/lfn/1.txt'] )
    self.assertTrue( res['OK'] )
    self.assertEqual( res['Value']['onlineLFNs'], ['/a/lfn/2.txt'] )
    self.assert_( res['Value']['offlineLFNs'], {'SE1': ['/a/lfn/1.txt']} or {'SE2': ['/a/lfn/1.txt']} )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( StorageManagerSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
