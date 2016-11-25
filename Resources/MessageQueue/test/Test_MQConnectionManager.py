"""Unit tests of MQConnectionManager in the DIRAC.Resources.MessageQueue.MConnectionManager
"""

import unittest
from DIRAC.Resources.MessageQueue.MQConnectionManager import MQConnectionManager


from DIRAC.Resources.MessageQueue.MQConnectionManager import _connectionExists
from DIRAC.Resources.MessageQueue.MQConnectionManager import _destinationExists
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getConnection
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getConnector
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getDestinations
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getMessangersId
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getProducersId
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getConsumersId

"""Connection storage
{
mardirac3.in2p3.fr: {'MQConnector':StompConnector, 'destinations':{'/queue/test1':['consumer1', 'producer1'],
                                                          '/queue/test2':['consumer1', 'producer1']}}
blabal.cern.ch: {'MQConnector':None, 'destinations':{'/queue/test2':['consumer2', 'producer2',]}}
}
"""
class TestMQConnectionStorageFunctions(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None  # To show full difference between structures in  case of error
    dest = {}
    dest.update({'/queue/test1': ['producer4', 'consumer1', 'consumer2', 'consumer4']})
    dest.update({'/queue/test2': ['producer2', 'consumer1', 'consumer2']})
    dest.update({'/topic/test1': ['producer1']})
    dest4 = {'/queue/test3': ['producer1', 'consumer2','consumer3','consumer4']}
    conn1 = {'MQConnector':'TestConnector1', 'destinations':dest}
    conn2 = {'MQConnector':'TestConnector2', 'destinations':dest4}
    self.storage = {'mardirac3.in2p3.fr':conn1, 'testdir.blabla.ch':conn2}
  def setDown(self):
    pass

class TestMQConnectionStorageFunctions_connectionExists( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    self.assertTrue(_connectionExists(self.storage, 'mardirac3.in2p3.fr'))
  def test_failure( self ):
    self.assertFalse(_connectionExists(self.storage, 'nonexisting'))

class TestMQConnectionStorageFunctions_destinationExists( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    self.assertTrue(_destinationExists(self.storage,'mardirac3.in2p3.fr',  '/queue/test1'))
  def test_failure( self ):
    self.assertFalse(_destinationExists(self.storage, 'nonexisting', '/queue/test1'))
  def test_failure2( self ):
    self.assertFalse(_destinationExists(self.storage, 'mardirac3.in2p3.fr', '/queue/nonexisting'))

class TestMQConnectionStorageFunctions_getConnection( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    expectedConn = {'MQConnector':'TestConnector2', 'destinations':{'/queue/test3': ['producer1', 'consumer2','consumer3','consumer4']}}
    self.assertEqual(_getConnection(self.storage,'testdir.blabla.ch'),expectedConn)
  def test_failure( self ):
    self.assertEqual(_getConnection(self.storage,'nonexisiting'), {})

class TestMQConnectionStorageFunctions_getConnector( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    self.assertEqual(_getConnector(self.storage,'testdir.blabla.ch'),'TestConnector2')
  def test_failure( self ):
    self.assertIsNone(_getConnector(self.storage,'nonexisiting'))

class TestMQConnectionStorageFunctions_getDestinations( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    expectedDests ={'/queue/test1': ['producer4', 'consumer1', 'consumer2', 'consumer4'], 
                    '/queue/test2': ['producer2', 'consumer1', 'consumer2'],
                    '/topic/test1': ['producer1']}
    print _getDestinations(self.storage,'mardirac3.in2p3.fr')
    print expectedDests
    self.assertEqual(_getDestinations(self.storage,'mardirac3.in2p3.fr'),expectedDests)
  def test_failure( self ):
    self.assertEqual(_getDestinations(self.storage,'nonexisiting'), {})

class TestMQConnectionStorageFunctions_getMessangersId( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    expectedMess =['producer4', 'consumer1', 'consumer2', 'consumer4']
    self.assertEqual(_getMessangersId(self.storage,'mardirac3.in2p3.fr', '/queue/test1'),expectedMess)
  def test_success2( self ):
    expectedMess2 =['producer2', 'consumer1', 'consumer2']
    self.assertEqual(_getMessangersId(self.storage,'mardirac3.in2p3.fr', '/queue/test2'),expectedMess2)
  def test_failure( self ):
    self.assertEqual(_getMessangersId(self.storage,'nonexisiting', '/queue/test2'), [])
  def test_failure2( self ):
    self.assertEqual(_getMessangersId(self.storage,'mardirac3.in2p3.fr', 'nonexisiting'), [])

class TestMQConnectionStorageFunctions_getProducersId( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    expectedMess =['producer4']
    self.assertEqual(_getProducersId(self.storage,'mardirac3.in2p3.fr', '/queue/test1'),expectedMess)
  def test_success2( self ):
    expectedMess2 =['producer2']
    self.assertEqual(_getProducersId(self.storage,'mardirac3.in2p3.fr', '/queue/test2'),expectedMess2)
  def test_failure( self ):
    self.assertEqual(_getProducersId(self.storage,'nonexisiting', '/queue/test2'), [])
  def test_failure2( self ):
    self.assertEqual(_getProducersId(self.storage,'mardirac3.in2p3.fr', 'nonexisiting'), [])

class TestMQConnectionStorageFunctions_getConsumersId( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    expectedMess =[ 'consumer1', 'consumer2', 'consumer4']
    self.assertEqual(_getConsumersId(self.storage,'mardirac3.in2p3.fr', '/queue/test1'),expectedMess)
  def test_success2( self ):
    expectedMess2 =['consumer1', 'consumer2']
    self.assertEqual(_getConsumersId(self.storage,'mardirac3.in2p3.fr', '/queue/test2'),expectedMess2)
  def test_failure( self ):
    self.assertEqual(_getConsumersId(self.storage,'nonexisiting', '/queue/test2'), [])
  def test_failure2( self ):
    self.assertEqual(_getConsumersId(self.storage,'mardirac3.in2p3.fr', 'nonexisiting'), [])

class TestMQConnectionManager( unittest.TestCase ):
  def setUp( self ):
    self.maxDiff = None  # To show full difference between structures in  case of error
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
    result = self.myManager.deleteConnection(mqService = "testdir.blabla.ch")
    self.assertTrue(result['OK'])
    result = self.myManager._connectionStorage.get("testdir.blabla.ch", None)
    self.assertIsNone(result)

  def test_failures_noexistingConnection( self ):
    result = self.myManager.deleteConnection(mqService = "baba.infn.it")
    self.assertFalse(result['OK'])

class TestMQConnectionManager_addConnection( TestMQConnectionManager ):
  def test_success( self ):
    connectionStorage =self.myManager._connectionStorage.copy()
    result = self.myManager.addConnection(mqURI = "test.ncbj.gov.pl::Queue::test1", connector = None, messangerType="producers" )
    self.assertTrue(result['Ok'])
    self.assertEqual(result['Value'], 1)
    dest = {"/queue/test1":{"producers":[1], "consumers":[]}}
    expectedConn = {"MQConnector":None, "destinations":dest}
    connectionStorage.update({"test.ncbj.gov.pl":expectedConn})
    self.assertEqual(connectionStorage, self.myManager._connectionStorage)

  def test_failure( self ):
    pass #add what happens if the entry already exists

class TestMQConnectionManager_updateConnection( TestMQConnectionManager ):
  def test_success( self ):
    result = self.myManager.updateConnection(mqURI = "mardirac3.in2p3.fr::Queue::test1", messangerType = "producers"  )
    self.assertTrue(result['Ok'])
    self.assertEqual(result['Value'], 5)
    result = self.myManager.updateConnection(mqURI = "mardirac3.in2p3.fr::Topic::test1", messangerType = "consumers"  )
    self.assertTrue(result['Ok'])
    self.assertEqual(result['Value'], 1)
    result = self.myManager.updateConnection(mqURI = "testdir.blabla.ch::Queue::test3", messangerType = "consumers"  )
    self.assertTrue(result['Ok'])
    self.assertEqual(result['Value'], 5)

    dest = {}
    dest.update({"/queue/test1": {"producers":[4, 5], "consumers":[1,2,4]}})
    dest.update({"/queue/test2": {"producers":[2], "consumers":[1,2]}})
    dest.update({"/topic/test1": {"producers":[1], "consumers":[1]}})
    dest4 = {"/queue/test3": {"producers":[1], "consumers":[2,3,4,5]}}
    conn1 = {"MQConnector":None, "destinations":dest}
    conn2 = {"MQConnector":None, "destinations":dest4}
    connectionStorage = {"mardirac3.in2p3.fr":conn1, "testdir.blabla.ch":conn2}
    self.assertEqual(self.myManager._connectionStorage, connectionStorage)

class TestMQConnectionManager_closeConnection( TestMQConnectionManager ):
  def test_success( self ):
    result = self.myManager.closeConnection(mqURI = "mardirac3.in2p3.fr::Queue::test1", messangerId = 4, messangerType = "producers")
    self.assertTrue(result['OK'])
    dest = {}
    dest.update({"/queue/test1": {"producers":[], "consumers":[1,2,4]}})
    dest.update({"/queue/test2": {"producers":[2], "consumers":[1,2]}})
    dest.update({"/topic/test1": {"producers":[1], "consumers":[]}})
    dest4 = {"/queue/test3": {"producers":[1], "consumers":[2,3,4]}}
    conn1 = {"MQConnector":None, "destinations":dest}
    conn2 = {"MQConnector":None, "destinations":dest4}
    connectionStorage = {"mardirac3.in2p3.fr":conn1, "testdir.blabla.ch":conn2}
    self.assertEqual(self.myManager._connectionStorage, connectionStorage)

  def test_success2( self ):
    result = self.myManager.closeConnection(mqURI = "mardirac3.in2p3.fr::Topic::test1", messangerId = 1, messangerType = "producers")
    self.assertTrue(result['OK'])
    dest = {}
    dest.update({"/queue/test1": {"producers":[4], "consumers":[1,2,4]}})
    dest.update({"/queue/test2": {"producers":[2], "consumers":[1,2]}})
    dest4 = {"/queue/test3": {"producers":[1], "consumers":[2,3,4]}}
    conn1 = {"MQConnector":None, "destinations":dest}
    conn2 = {"MQConnector":None, "destinations":dest4}
    connectionStorage = {"mardirac3.in2p3.fr":conn1, "testdir.blabla.ch":conn2}
    self.assertEqual(self.myManager._connectionStorage, connectionStorage)

  def test_success3( self ):
    result = self.myManager.closeConnection(mqURI = "testdir.blabla.ch::Queue::test3", messangerId = 3, messangerType = "consumers")
    self.assertTrue(result['OK'])
    result = self.myManager.closeConnection(mqURI = "testdir.blabla.ch::Queue::test3", messangerId = 1, messangerType = "producers")
    self.assertTrue(result['OK'])
    result = self.myManager.closeConnection(mqURI = "testdir.blabla.ch::Queue::test3", messangerId = 2, messangerType = "consumers")
    self.assertTrue(result['OK'])
    result = self.myManager.closeConnection(mqURI = "testdir.blabla.ch::Queue::test3", messangerId = 4, messangerType = "consumers")
    self.assertTrue(result['OK'])
    dest = {}
    dest.update({"/queue/test1": {"producers":[4], "consumers":[1,2,4]}})
    dest.update({"/queue/test2": {"producers":[2], "consumers":[1,2]}})
    dest.update({"/topic/test1": {"producers":[1], "consumers":[]}})
    conn1 = {"MQConnector":None, "destinations":dest}
    connectionStorage = {"mardirac3.in2p3.fr":conn1}
    self.assertEqual(self.myManager._connectionStorage, connectionStorage)

#class TestMQConnectionManager_addOrUpdateConnection( TestMQConnectionManager ):
  #def test_success( self ):
    #result = self.myManager.addOrUpdateConnection(mqURI = "testdir.blabla.ch::Queue::test1", params = {}, messangerType = "producer")
    #pass
    #self.assertEqual(result, expectedConn)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_connectionExists ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_getConnection ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_deleteConnection ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_updateConnection ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_addConnection ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_closeConnection ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_addOrUpdateConnection ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_connectionExists ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_destinationExists ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getConnection ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getConnector ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getDestinations ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getMessangersId ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getProducersId ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getConsumersId ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
