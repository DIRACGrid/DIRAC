""" Test for StorageManagement clients
"""

import unittest
import importlib

from mock import MagicMock

from DIRAC import S_OK
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import getFilesToStage
from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock


class ClientsTestCase( unittest.TestCase ):
  """ Base class for the clients test cases
  """
  def setUp( self ):

    from DIRAC import gLogger
    gLogger.setLevel( 'DEBUG' )

    mockObjectSE = MagicMock()
    mockObjectSE.getFileMetadata.return_value = S_OK( {'Successful':{'/a/lfn/1.txt':{'Cached':0},
                                                                     '/a/lfn/2.txt':{'Cached':1}},
                                                       'Failed':{}} )
    mockObjectSE.getStatus.return_value = S_OK( {'DiskSE': False, 'TapeSE':True} )

    self.mockSE = MagicMock()
    self.mockSE.return_value = mockObjectSE

    self.mockDM = MagicMock()
    self.mockDM.return_value = dm_mock


  def tearDown( self ):
    pass

#############################################################################

class StorageManagerSuccess( ClientsTestCase ):

  def test_getFilesToStage( self ):
    res = getFilesToStage( [] )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value']['onlineLFNs'], [] )
    self.assertEqual( res['Value']['offlineLFNs'], {} )

    ourSMC = importlib.import_module( 'DIRAC.StorageManagementSystem.Client.StorageManagerClient' )
    ourSMC.DataManager = self.mockDM
    ourSMC.StorageElement = self.mockSE


    res = getFilesToStage( ['/a/lfn/1.txt'] )
    self.assert_( res['OK'], res )
    self.assertEqual( res['Value']['onlineLFNs'], ['/a/lfn/2.txt'] )
    self.assert_( res['Value']['offlineLFNs'], {'SE1':['/a/lfn/1.txt']} or {'SE2':['/a/lfn/1.txt']} )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( StorageManagerSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
