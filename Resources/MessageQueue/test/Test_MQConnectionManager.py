"""Unit tests of MQConnectionManager in the DIRAC.Resources.MessageQueue.MConnectionManager
"""

import unittest
from DIRAC.Resources.MessageQueue.MQConnectionManager import MQConnectionManager

class TestMQConnectionManager( unittest.TestCase ):
  def setUp( self ):
    dest1 = {"name":"/queue/test1", "publishers":4, "subscribers":[1,2,4]}
    dest2 = {"name":"/queue/test2", "publishers":2, "subscribers":[1,2]}
    dest3 = {"name":"/topic/test1", "publishers":0, "subscribers":[]}
    dest4 = {"name":"/queue/test3", "publishers":1, "subscribers":[2,3,4]}
    conn1 = {"MQConnector":None, "destinations":[dest1,dest2,dest3]}
    conn2 = {"MQConnector":None, "destinations":[dest4]}
    connectionStorage = {"mardirac3.in2p3.fr":conn1, "testdir.blabla.ch":conn2}
    self.myManager = MQConnectionManager(connectionStorage = connectionStorage)

  def tearDown( self ):
    pass

class TestMQConnectionManager_connectionExists( TestMQConnectionManager ):
  def test_success( self ):
    self.assertTrue(self.myManager.connectionExist("mardirac3.in2p3.fr"))
  def test_failure( self ):
    self.assertFalse(self.myManager.connectionExist("nonexistent"))

class TestMQConnectionManager_getConnection( TestMQConnectionManager ):
  def test_success( self ):
    result = self.myManager.getConnection("testdir.blabla.ch")
    dest = {"name":"/queue/test3", "publishers":1, "subscribers":[2,3,4]}
    expected_conn = {"MQConnector":None, "destinations":[dest]}
    self.assertEqual(result, expected_conn)


  def test_failure( self ):
    result = self.myManager.getConnection("nonexistent")
    self.assertIsNone(result)

class TestMQConnectionManager_addConnectionIfNotExist( TestMQConnectionManager ):
  def test_success( self ):
    myInfo = {"MQConnector":None, "destination":[]}
    result = self.myManager.addConnectionIfNotExist(connectionInfo = myInfo, mqServiceId = "baba.infn.it")
    self.assertEqual(result, myInfo)

  def test_success_existingConnection( self ):
    myInfo = {"MQConnector":None, "destination":[]}
    result = self.myManager.addConnectionIfNotExist(connectionInfo = myInfo, mqServiceId = "testdir.blabla.ch")
    dest = {"name":"/queue/test3", "publishers":1, "subscribers":[2,3,4]}
    expected_conn = {"MQConnector":None, "destinations":[dest]}
    self.assertEqual(result, expected_conn)

class TestMQConnectionManager_deleteConnection( TestMQConnectionManager ):
  def test_success( self ):
    result = self.myManager.deleteConnection(mqServiceId = "testdir.blabla.ch")
    self.assertTrue(result)
    result = self.myManager._connectionStorage.get("testdir.blabla.ch", None)
    self.assertIsNone(result)

  def test_failures_noexistingConnection( self ):
    result = self.myManager.deleteConnection(mqServiceId = "baba.infn.it")
    self.assertFalse(result)


class TestMQConnectionManager_updateConnection( TestMQConnectionManager ):
  def test_success( self ):
    result = self.myManager.updateConnection(mqServiceId = "testdir.blabla.ch", destInfoToAdd = None )
    dest4 = {"name":"/queue/test3", "publishers":1, "subscribers":[2,3,4]}
    expectedConn = {"MQConnector":None, "destinations":[dest4]}
    self.assertEqual(result, expectedConn)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_connectionExists ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_getConnection ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_addConnectionIfNotExist ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_deleteConnection ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_updateConnection ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
