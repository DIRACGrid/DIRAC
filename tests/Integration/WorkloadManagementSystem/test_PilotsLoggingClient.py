""" This is a test of the chain
    PilotsLoggingClient -> PilotsLoggingHandler -> PilotsLoggingDB

    It supposes that the DB is present, and that the service is running
"""

import unittest
from DIRAC.WorkloadManagementSystem.Client.PilotsLoggingClient import PilotsLoggingClient

class TestPilotsLogging( unittest.TestCase ):

  def setUp( self ):
    self.pilotsLoggingClient = PilotsLoggingClient()

  def tearDown( self ):
    pass

class PilotsLogging( TestPilotsLogging ):

  def test_PilotsLoggingAddGetDelete( self ):

    resp = self.pilotsLoggingClient.addPilotsLogging('11111111-1111-1111-1111-111111111111', 'status1', 'minorStatus1', 1427721819.0, 'test', 0)
    self.assert_(resp['OK'], 'Failed to add PilotsLogging')
    resp = self.pilotsLoggingClient.setPilotsUUIDtoIDMapping('11111111-1111-1111-1111-111111111111', 1)
    self.assert_(resp['OK'], 'Failed to add PilotsUUIDtoIDMapping')
    resp = self.pilotsLoggingClient.getPilotsLogging(1)
    self.assert_(resp['OK'], 'Failed to get PilotsLogging')
    test_sample = {
                   'PilotUUID': '11111111-1111-1111-1111-111111111111',
                   'PilotID': 1,
                   'Status': 'status1',
                   'MinorStatus': 'minorStatus1',
                   'TimeStamp': 1427721819.0,
                   'Source': 'test'
                   }
    self.assertEqual(resp['Value'], [ test_sample ], 'Wrong data comes out of Service')
    resp = self.pilotsLoggingClient.deletePilotsLogging(1)
    self.assert_(resp['OK'], 'Failed to delete PilotsLogging')
    resp = self.pilotsLoggingClient.getPilotsLogging(1)
    self.assert_(resp['OK'], 'Failed to get PilotsLogging')
    self.assertEqual(resp['Value'], [], 'PilotsLogging was not really deleted')

  def test_PilotsLoggingEmptyGetDelete( self ):

    resp = self.pilotsLoggingClient.getPilotsLogging( 1 )
    self.assert_( resp['OK'], 'Failed to get PilotsLogging' )
    resp = self.pilotsLoggingClient.deletePilotsLogging( 1 )
    self.assert_( resp['OK'], 'Failed to delete PilotsLogging' )

  def test_PilotsLoggingDeleteList( self ):

    test_sample1 = {
                   'PilotUUID': '11111111-1111-1111-1111-111111111111',
                   'PilotID': 1,
                   'Status': 'status1',
                   'MinorStatus': 'minorStatus1',
                   'TimeStamp': 1427721819.0,
                   'Source': 'test'
                   }
    test_sample2 = {
                   'PilotUUID': '22222222-2222-2222-2222-222222222222',
                   'PilotID': 2,
                   'Status': 'status2',
                   'MinorStatus': 'minorStatus2',
                   'TimeStamp': 1427721820.0,
                   'Source': 'test'
                   }

    resp = self.pilotsLoggingClient.addPilotsLogging('11111111-1111-1111-1111-111111111111', 'status1', 'minorStatus1', 1427721819.0, 'test', 0)
    self.assert_(resp['OK'], 'Failed to add PilotsLogging')
    resp = self.pilotsLoggingClient.addPilotsLogging('22222222-2222-2222-2222-222222222222', 'status2', 'minorStatus2', 1427721820.0, 'test', 0)
    self.assert_(resp['OK'], 'Failed to add PilotsLogging')
    resp = self.pilotsLoggingClient.setPilotsUUIDtoIDMapping('11111111-1111-1111-1111-111111111111', 1)
    self.assert_(resp['OK'], 'Failed to add PilotsUUIDtoIDMapping')
    resp = self.pilotsLoggingClient.setPilotsUUIDtoIDMapping('22222222-2222-2222-2222-222222222222', 2)
    self.assert_(resp['OK'], 'Failed to add PilotsUUIDtoIDMapping')
    resp = self.pilotsLoggingClient.getPilotsLogging(1)
    self.assert_(resp['OK'], 'Failed to get PilotsLogging')
    self.assertEqual(resp['Value'], [ test_sample1 ], 'Wrong data comes out of Service')
    resp = self.pilotsLoggingClient.getPilotsLogging(2)
    self.assert_(resp['OK'], 'Failed to get PilotsLogging')
    self.assertEqual(resp['Value'], [ test_sample2 ], 'Wrong data comes out of Service')
    resp = self.pilotsLoggingClient.deletePilotsLogging( [1, 2] )
    self.assert_(resp['OK'], 'Failed to delete PilotsLogging')
    resp = self.pilotsLoggingClient.getPilotsLogging(1)
    self.assert_(resp['OK'], 'Failed to get PilotsLogging')
    self.assertEqual(resp['Value'], [], 'PilotsLogging was not really deleted')
    resp = self.pilotsLoggingClient.getPilotsLogging(2)
    self.assert_(resp['OK'], 'Failed to get PilotsLogging')
    self.assertEqual(resp['Value'], [], 'PilotsLogging was not really deleted')


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestPilotsLogging )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( PilotsLogging ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
