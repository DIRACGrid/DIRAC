""" Test for WMS clients
"""

import os
import unittest
import importlib

from mock import MagicMock

from DIRAC import S_OK
from DIRAC.WorkloadManagementSystem.Client.DownloadInputData import DownloadInputData

class ClientsTestCase( unittest.TestCase ):
  """ Base class for the clients test cases
  """
  def setUp( self ):

    from DIRAC import gLogger
    gLogger.setLevel( 'DEBUG' )

    mockObjectDM = MagicMock()
    mockObjectDM.getActiveReplicas.return_value = S_OK( {'Successful': {'/a/lfn/1.txt':{'SE1':'/a/lfn/at/SE1.1.txt',
                                                                                        'SE2':'/a/lfn/at/SE2.1.txt'},
                                                                        '/a/lfn/2.txt':{'SE1':'/a/lfn/at/SE1.1.txt'}
                                                                        },
                                                         'Failed':{}} )

    self.mockDM = MagicMock()
    self.mockDM.return_value = mockObjectDM



    mockObjectSE = MagicMock()
    mockObjectSE.getFileMetadata.return_value = S_OK( {'Successful':{'/a/lfn/1.txt':{'Cached':0},
                                                                     '/a/lfn/2.txt':{'Cached':1}},
                                                       'Failed':{}} )
    mockObjectSE.getFile.return_value = S_OK( {'Successful':{'/a/lfn/1.txt':{}},
                                              'Failed':{}} )
    mockObjectSE.getStatus.return_value = S_OK( {'Read': True, 'DiskSE': True} )

    self.mockSE = MagicMock()
    self.mockSE.return_value = mockObjectSE

    self.dli = DownloadInputData( {'InputData': [],
                                   'Configuration': 'boh',
                                   'FileCatalog': S_OK( {'Successful': []} )} )

  def tearDown( self ):
    try:
      os.remove( '1.txt' )
      os.remove( 'InputData_*' )
    except OSError:
      pass

#############################################################################

class DownloadInputDataSuccess( ClientsTestCase ):

  def test_DLIDownloadFromSE( self ):
    ourDLI = importlib.import_module( 'DIRAC.WorkloadManagementSystem.Client.DownloadInputData' )
    ourDLI.StorageElement = self.mockSE

    res = self.dli._downloadFromSE( '/a/lfn/1.txt', 'mySE', {'mySE': []}, 'aGuid' )
    # file won't exist at this point
    self.assertFalse( res['OK'] )

    open( '1.txt', 'w' ).close()
    res = self.dli._downloadFromSE( '/a/lfn/1.txt', 'mySE', {'mySE': []}, 'aGuid' )
    # file would be already local, so no real download
    self.assert_( res['OK'] )
    try: os.remove( '1.txt' )
    except OSError: pass

    # I can't figure out how to simulate a real download here

  def test_DLIDownloadFromBestSE( self ):
    ourDLI = importlib.import_module( 'DIRAC.WorkloadManagementSystem.Client.DownloadInputData' )
    ourDLI.StorageElement = self.mockSE

    res = self.dli._downloadFromBestSE( '/a/lfn/1.txt', {'mySE': []}, 'aGuid' )
    # file won't exist at this point
    self.assertFalse( res['OK'] )

    open( '1.txt', 'w' ).close()
    res = self.dli._downloadFromBestSE( '/a/lfn/1.txt', {'mySE': []}, 'aGuid' )
    # file would be already local, so no real download
    self.assert_( res['OK'] )
    try: os.remove( '1.txt' )
    except OSError: pass

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ClientsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( DownloadInputDataSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
