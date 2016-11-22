"""Unit tests of MQConnectionManager in the DIRAC.Resources.MessageQueue.MConnectionManager
"""

import unittest
from DIRAC.Resources.MessageQueue.MQConnectionManager import MQConnectionManager

class TestMQConnectionManager( unittest.TestCase ):
  def setUp( self ):
    dest = {}
    dest.update({"/queue/test1": {"producers":[4], "consumers":[1,2,4]}})
    dest.update({"/queue/test2": {"producers":[2], "consumers":[1,2]}})
    dest.update({"/topic/test1": {"producers":[1], "consumers":[]}})
    dest4 = {"/queue/test3": {"producers":[1], "consumers":[2,3,4]}}
    conn1 = {"MQConnector":None, "destinations":dest}
    conn2 = {"MQConnector":None, "destinations":dest4}
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
    dest = {"/queue/test3": {"producers":[1], "consumers":[2,3,4]}}
    expected_conn = {"MQConnector":None, "destinations":dest}
    self.assertEqual(result, expected_conn)


  def test_failure( self ):
    result = self.myManager.getConnection("nonexistent")
    self.assertIsNone(result)

class TestMQConnectionManager_deleteConnection( TestMQConnectionManager ):
  def test_success( self ):
    result = self.myManager.deleteConnection(mqServiceId = "testdir.blabla.ch")
    self.assertTrue(result)
    result = self.myManager._connectionStorage.get("testdir.blabla.ch", None)
    self.assertIsNone(result)

  def test_failures_noexistingConnection( self ):
    result = self.myManager.deleteConnection(mqServiceId = "baba.infn.it")
    self.assertFalse(result)

class TestMQConnectionManager_addConnection( TestMQConnectionManager ):
  def test_success( self ):
    self.maxDiff = None
    connectionStorage =self.myManager._connectionStorage.copy()
    result = self.myManager.addConnection(mqURI = "test.ncbj.gov.pl::Queue::test1", connector = None, messangerType="producers" )
    self.assertEqual(result, 1)
    dest = {"/queue/test1":{"producers":[1], "consumers":[]}}
    expectedConn = {"MQConnector":None, "destinations":dest}
    connectionStorage.update({"test.ncbj.gov.pl":expectedConn})
    self.assertEqual(connectionStorage, self.myManager._connectionStorage)

  def test_failure( self ):
    pass #add what happens if the entry already exists

class TestMQConnectionManager_updateConnection( TestMQConnectionManager ):
  def test_success( self ):
    result = self.myManager.updateConnection(mqURI = "mardirac3.in2p3.fr::Queue::test1", messangerType = "producers"  )
    self.assertEqual(result, 5)
    result = self.myManager.updateConnection(mqURI = "mardirac3.in2p3.fr::Topic::test1", messangerType = "consumers"  )
    self.assertEqual(result, 1)
    result = self.myManager.updateConnection(mqURI = "testdir.blabla.ch::Queue::test3", messangerType = "consumers"  )
    self.assertEqual(result, 5)

    dest = {}
    dest.update({"/queue/test1": {"producers":[4, 5], "consumers":[1,2,4]}})
    dest.update({"/queue/test2": {"producers":[2], "consumers":[1,2]}})
    dest.update({"/topic/test1": {"producers":[1], "consumers":[1]}})
    dest4 = {"/queue/test3": {"producers":[1], "consumers":[2,3,4,5]}}
    conn1 = {"MQConnector":None, "destinations":dest}
    conn2 = {"MQConnector":None, "destinations":dest4}
    connectionStorage = {"mardirac3.in2p3.fr":conn1, "testdir.blabla.ch":conn2}
    self.assertEqual(self.myManager._connectionStorage, connectionStorage)


#class TestMQConnectionManager_addOrUpdateConnection( TestMQConnectionManager ):
  #def test_success( self ):
    #result = self.myManager.addOrUpdateConnection(mqURI = "testdir.blabla.ch::Queue::test1", params = {}, messangerType = "producer")
    #pass
    #self.assertEqual(result, expectedConn)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_connectionExists ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_getConnection ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_deleteConnection ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_updateConnection ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_addConnection ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_addOrUpdateConnection ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
