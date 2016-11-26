"""Unit tests of MQConnectionManager in the DIRAC.Resources.MessageQueue.MConnectionManager
   Also, test of internal functions for mq connection storage.
"""

import unittest
from DIRAC.Resources.MessageQueue.MQConnectionManager import MQConnectionManager

from DIRAC.Resources.MessageQueue.MQConnectionManager import _connectionExists
from DIRAC.Resources.MessageQueue.MQConnectionManager import _destinationExists
from DIRAC.Resources.MessageQueue.MQConnectionManager import _MessangerExists
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getConnection
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getConnector
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getDestinations
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getMessangersId
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getMessangersIdWithType
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getAllMessangersInfo
from DIRAC.Resources.MessageQueue.MQConnectionManager import _addMessanger
from DIRAC.Resources.MessageQueue.MQConnectionManager import _removeMessanger

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

class TestMQConnectionStorageFunctions_messangerExists( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    self.assertTrue(_MessangerExists(self.storage,'mardirac3.in2p3.fr',  '/queue/test1','consumer2' ))
    self.assertTrue(_MessangerExists(self.storage,'mardirac3.in2p3.fr',  '/queue/test1','producer4' ))
  def test_failure( self ):
    self.assertFalse(_MessangerExists(self.storage,'noexisting',  '/queue/test1','producer4' ))
  def test_failure2( self ):
    self.assertFalse(_MessangerExists(self.storage, 'mardirac3.in2p3.fr', '/queue/nonexisting','producer4'))
  def test_failure3( self ):
    self.assertFalse(_MessangerExists(self.storage, 'mardirac3.in2p3.fr', '/queue/test1','producer10'))

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

class TestMQConnectionStorageFunctions_getMessangersIdWithType( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    expectedMess =['producer4']
    self.assertEqual(_getMessangersIdWithType(self.storage,'mardirac3.in2p3.fr', '/queue/test1', 'producer'),expectedMess)
  def test_success2( self ):
    expectedMess2 =['producer2']
    self.assertEqual(_getMessangersIdWithType(self.storage,'mardirac3.in2p3.fr', '/queue/test2', 'producer'),expectedMess2)
  def test_success3( self ):
    expectedMess =[ 'consumer1', 'consumer2', 'consumer4']
    self.assertEqual(_getMessangersIdWithType(self.storage,'mardirac3.in2p3.fr', '/queue/test1', 'consumer'),expectedMess)
  def test_success4( self ):
    expectedMess2 =['consumer1', 'consumer2']
    self.assertEqual(_getMessangersIdWithType(self.storage,'mardirac3.in2p3.fr', '/queue/test2', 'consumer'),expectedMess2)
  def test_failure( self ):
    self.assertEqual(_getMessangersIdWithType(self.storage,'nonexisiting', '/queue/test2', 'producer'), [])
  def test_failure2( self ):
    self.assertEqual(_getMessangersIdWithType(self.storage,'mardirac3.in2p3.fr', 'nonexisiting', 'producer'), [])

class TestMQConnectionStorageFunctions_getAllMessangersInfo( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    expectedOutput= ['mardirac3.in2p3.fr/queue/test1/producer4', 'mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'mardirac3.in2p3.fr/topic/test1/producer1', 'testdir.blabla.ch/queue/test3/producer1', 'testdir.blabla.ch/queue/test3/consumer2', 'testdir.blabla.ch/queue/test3/consumer3', 'testdir.blabla.ch/queue/test3/consumer4']
    self.assertEqual(sorted(_getAllMessangersInfo(self.storage)),sorted(expectedOutput))

class TestMQConnectionStorageFunctions_addMessanger( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    expectedOutput= ['mardirac3.in2p3.fr/queue/test1/producer1', 'mardirac3.in2p3.fr/queue/test1/producer4', 'mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'mardirac3.in2p3.fr/topic/test1/producer1', 'testdir.blabla.ch/queue/test3/producer1', 'testdir.blabla.ch/queue/test3/consumer2', 'testdir.blabla.ch/queue/test3/consumer3', 'testdir.blabla.ch/queue/test3/consumer4']
    self.assertTrue(_addMessanger(self.storage,'mardirac3.in2p3.fr', '/queue/test1', 'producer1'))
    self.assertEqual(sorted(_getAllMessangersInfo(self.storage)),sorted(expectedOutput))

  def test_success2( self ):
    # new queue
    expectedOutput= ['mardirac3.in2p3.fr/queue/test5/producer8', 'mardirac3.in2p3.fr/queue/test1/producer4', 'mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'mardirac3.in2p3.fr/topic/test1/producer1', 'testdir.blabla.ch/queue/test3/producer1', 'testdir.blabla.ch/queue/test3/consumer2', 'testdir.blabla.ch/queue/test3/consumer3', 'testdir.blabla.ch/queue/test3/consumer4']
    self.assertTrue(_addMessanger(self.storage,'mardirac3.in2p3.fr', '/queue/test5', 'producer8'))
    self.assertEqual(sorted(_getAllMessangersInfo(self.storage)),sorted(expectedOutput))

  def test_success3( self ):
    # new connection
    expectedOutput= ['mytest.is.the.best/queue/test10/producer24', 'mardirac3.in2p3.fr/queue/test1/producer4', 'mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'mardirac3.in2p3.fr/topic/test1/producer1', 'testdir.blabla.ch/queue/test3/producer1', 'testdir.blabla.ch/queue/test3/consumer2', 'testdir.blabla.ch/queue/test3/consumer3', 'testdir.blabla.ch/queue/test3/consumer4']
    self.assertTrue(_addMessanger(self.storage,'mytest.is.the.best', '/queue/test10', 'producer24'))
    self.assertEqual(sorted(_getAllMessangersInfo(self.storage)),sorted(expectedOutput))
  def test_success4( self ):
    #  two times
    expectedOutput= ['mytest.is.the.best/queue/test10/producer2', 'mytest.is.the.best/queue/test10/producer24', 'mardirac3.in2p3.fr/queue/test1/producer4', 'mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'mardirac3.in2p3.fr/topic/test1/producer1', 'testdir.blabla.ch/queue/test3/producer1', 'testdir.blabla.ch/queue/test3/consumer2', 'testdir.blabla.ch/queue/test3/consumer3', 'testdir.blabla.ch/queue/test3/consumer4']
    self.assertTrue(_addMessanger(self.storage,'mytest.is.the.best', '/queue/test10', 'producer24'))
    self.assertTrue(_addMessanger(self.storage,'mytest.is.the.best', '/queue/test10', 'producer2'))
    self.assertEqual(sorted(_getAllMessangersInfo(self.storage)),sorted(expectedOutput))

  def test_failure( self ):
    # messanger already exists
    expectedOutput= ['mardirac3.in2p3.fr/queue/test1/producer4', 'mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'mardirac3.in2p3.fr/topic/test1/producer1', 'testdir.blabla.ch/queue/test3/producer1', 'testdir.blabla.ch/queue/test3/consumer2', 'testdir.blabla.ch/queue/test3/consumer3', 'testdir.blabla.ch/queue/test3/consumer4']
    self.assertFalse(_addMessanger(self.storage,'mardirac3.in2p3.fr', '/queue/test1', 'producer4'))
    self.assertEqual(sorted(_getAllMessangersInfo(self.storage)),sorted(expectedOutput))

class TestMQConnectionStorageFunctions_removeMessanger( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    expectedOutput= [ 'mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'mardirac3.in2p3.fr/topic/test1/producer1', 'testdir.blabla.ch/queue/test3/producer1', 'testdir.blabla.ch/queue/test3/consumer2', 'testdir.blabla.ch/queue/test3/consumer3', 'testdir.blabla.ch/queue/test3/consumer4']
    self.assertTrue(_removeMessanger(self.storage,'mardirac3.in2p3.fr', '/queue/test1', 'producer4'))
    self.assertEqual(sorted(_getAllMessangersInfo(self.storage)),sorted(expectedOutput))

  def test_success2( self ):
    #remove whole destination /topic/test1 cause only one element
    expectedOutput= [ 'mardirac3.in2p3.fr/queue/test1/producer4','mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2','testdir.blabla.ch/queue/test3/producer1', 'testdir.blabla.ch/queue/test3/consumer2', 'testdir.blabla.ch/queue/test3/consumer3', 'testdir.blabla.ch/queue/test3/consumer4']
    self.assertTrue(_removeMessanger(self.storage,'mardirac3.in2p3.fr', '/topic/test1', 'producer1'))
    self.assertEqual(sorted(_getAllMessangersInfo(self.storage)),sorted(expectedOutput))

  def test_success3( self ):
    expectedOutput= ['mardirac3.in2p3.fr/queue/test1/producer4', 'mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'mardirac3.in2p3.fr/topic/test1/producer1']
    #remove whole connection
    self.assertTrue(_removeMessanger(self.storage,'testdir.blabla.ch', '/queue/test3', 'producer1'))
    self.assertTrue(_removeMessanger(self.storage,'testdir.blabla.ch', '/queue/test3', 'consumer2'))
    self.assertTrue(_removeMessanger(self.storage,'testdir.blabla.ch', '/queue/test3', 'consumer3'))
    self.assertTrue(_removeMessanger(self.storage,'testdir.blabla.ch', '/queue/test3', 'consumer4'))
    self.assertEqual(sorted(_getAllMessangersInfo(self.storage)),sorted(expectedOutput))

  def test_failure( self ):
    #remove nonexisting messanger
    self.assertFalse(_removeMessanger(self.storage,'testdir.blabla.ch', '/queue/test3', 'producer10'))
  def test_failure2( self ):
    #remove nonexisting destination
    self.assertFalse(_removeMessanger(self.storage,'testdir.blabla.ch', '/queue/nonexisting', 'producer1'))
  def test_failure3( self ):
    #remove nonexisting connection
    self.assertFalse(_removeMessanger(self.storage,'nonexisting', '/queue/test103', 'producer1'))

class TestMQConnectionManager( unittest.TestCase ):
  def setUp( self ):
    self.maxDiff = None  # To show full difference between structures in  case of error
    dest = {}
    dest.update({'/queue/test1': ['producer4', 'consumer1', 'consumer2', 'consumer4']})
    dest.update({'/queue/test2': ['producer2', 'consumer1', 'consumer2']})
    dest.update({'/topic/test1': ['producer1']})
    dest4 = {'/queue/test3': ['producer1', 'consumer2','consumer3','consumer4']}
    conn1 = {'MQConnector':'TestConnector1', 'destinations':dest}
    conn2 = {'MQConnector':'TestConnector2', 'destinations':dest4}
    storage = {'mardirac3.in2p3.fr':conn1, 'testdir.blabla.ch':conn2}
    self.myManager = MQConnectionManager(connectionStorage = storage)

  def tearDown( self ):
    pass
class TestMQConnectionManager_updateConnection( TestMQConnectionManager ):
  def test_success( self ):
    result = self.myManager.updateConnection(mqURI = "mardirac3.in2p3.fr::Queue::test1", messangerType = "producer"  )
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], 'producer5')
    expectedOutput= ['mardirac3.in2p3.fr/queue/test1/producer5', 'mardirac3.in2p3.fr/queue/test1/producer4', 'mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'mardirac3.in2p3.fr/topic/test1/producer1', 'testdir.blabla.ch/queue/test3/producer1', 'testdir.blabla.ch/queue/test3/consumer2', 'testdir.blabla.ch/queue/test3/consumer3', 'testdir.blabla.ch/queue/test3/consumer4']
    self.assertEqual(sorted(_getAllMessangersInfo(self.myManager._connectionStorage)),sorted(expectedOutput))

  def test_success2( self ):
    result = self.myManager.updateConnection(mqURI = "mardirac3.in2p3.fr::Topic::test1", messangerType = "consumer"  )
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], 'consumer1')
  def test_success3( self ):
    result = self.myManager.updateConnection(mqURI = "testdir.blabla.ch::Queue::test3", messangerType = "consumer"  )
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], 'consumer5')


  def test_success4( self ):
    #connection does not exist
    result = self.myManager.updateConnection(mqURI = "noexisting.blabla.ch::Queue::test3", messangerType = "consumer"  )
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], 'consumer1')
    expectedOutput= ['noexisting.blabla.ch/queue/test3/consumer1', 'mardirac3.in2p3.fr/queue/test1/producer4', 'mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'mardirac3.in2p3.fr/topic/test1/producer1', 'testdir.blabla.ch/queue/test3/producer1', 'testdir.blabla.ch/queue/test3/consumer2', 'testdir.blabla.ch/queue/test3/consumer3', 'testdir.blabla.ch/queue/test3/consumer4']
    self.assertEqual(sorted(_getAllMessangersInfo(self.myManager._connectionStorage)),sorted(expectedOutput))


#class TestMQConnectionManager_connectionExists( TestMQConnectionManager ):
  #def test_success( self ):
    #self.assertTrue(self.myManager.connectionExist("mardirac3.in2p3.fr"))
  #def test_failure( self ):
    #self.assertFalse(self.myManager.connectionExist("nonexistent"))

#class TestMQConnectionManager_getConnection( TestMQConnectionManager ):
  #def test_success( self ):
    #result = self.myManager.getConnection("testdir.blabla.ch")
    #dest = {"/queue/test3": {"producers":[1], "consumers":[2,3,4]}}
    #expected_conn = {"MQConnector":None, "destinations":dest}
    #self.assertEqual(result, expected_conn)


  #def test_failure( self ):
    #result = self.myManager.getConnection("nonexistent")
    #self.assertIsNone(result)

#class TestMQConnectionManager_deleteConnection( TestMQConnectionManager ):
  #def test_success( self ):
    #result = self.myManager.deleteConnection(mqService = "testdir.blabla.ch")
    #self.assertTrue(result['OK'])
    #result = self.myManager._connectionStorage.get("testdir.blabla.ch", None)
    #self.assertIsNone(result)

  #def test_failures_noexistingConnection( self ):
    #result = self.myManager.deleteConnection(mqService = "baba.infn.it")
    #self.assertFalse(result['OK'])

#class TestMQConnectionManager_addConnection( TestMQConnectionManager ):
  #def test_success( self ):
    #connectionStorage =self.myManager._connectionStorage.copy()
    #result = self.myManager.addConnection(mqURI = "test.ncbj.gov.pl::Queue::test1", connector = None, messangerType="producers" )
    #self.assertTrue(result['Ok'])
    #self.assertEqual(result['Value'], 1)
    #dest = {"/queue/test1":{"producers":[1], "consumers":[]}}
    #expectedConn = {"MQConnector":None, "destinations":dest}
    #connectionStorage.update({"test.ncbj.gov.pl":expectedConn})
    #self.assertEqual(connectionStorage, self.myManager._connectionStorage)

  #def test_failure( self ):
    #pass #add what happens if the entry already exists


#class TestMQConnectionManager_closeConnection( TestMQConnectionManager ):
  #def test_success( self ):
    #result = self.myManager.closeConnection(mqURI = "mardirac3.in2p3.fr::Queue::test1", messangerId = 4, messangerType = "producers")
    #self.assertTrue(result['OK'])
    #dest = {stopConnection
    #dest.update({"/queue/test1": {"producers":[], "consumers":[1,2,4]}})
    #dest.update({"/queue/test2": {"producers":[2], "consumers":[1,2]}})
    #dest.update({"/topic/test1": {"producers":[1], "consumers":[]}})
    #dest4 = {"/queue/test3": {"producers":[1], "consumers":[2,3,4]}}
    #conn1 = {"MQConnector":None, "destinations":dest}
    #conn2 = {"MQConnector":None, "destinations":dest4}
    #connectionStorage = {"mardirac3.in2p3.fr":conn1, "testdir.blabla.ch":conn2}
    #self.assertEqual(self.myManager._connectionStorage, connectionStorage)

  #def test_success2( self ):
    #result = self.myManager.closeConnection(mqURI = "mardirac3.in2p3.fr::Topic::test1", messangerId = 1, messangerType = "producers")
    #self.assertTrue(result['OK'])
    #dest = {stopConnection
    #dest.update({"/queue/test1": {"producers":[4], "consumers":[1,2,4]}})
    #dest.update({"/queue/test2": {"producers":[2], "consumers":[1,2]}})
    #dest4 = {"/queue/test3": {"producers":[1], "consumers":[2,3,4]}}
    #conn1 = {"MQConnector":None, "destinations":dest}
    #conn2 = {"MQConnector":None, "destinations":dest4}
    #connectionStorage = {"mardirac3.in2p3.fr":conn1, "testdir.blabla.ch":conn2}
    #self.assertEqual(self.myManager._connectionStorage, connectionStorage)

  #def test_success3( self ):
    #result = self.myManager.closeConnection(mqURI = "testdir.blabla.ch::Queue::test3", messangerId = 3, messangerType = "consumers")
    #self.assertTrue(result['OK'])
    #result = self.myManager.stopConnection(mqURI = "testdir.blabla.ch::Queue::test3", messangerId = 1, messangerType = "producers")
    #self.assertTrue(result['OK'])
    #result = self.myManager.stopConnection(mqURI = "testdir.blabla.ch::Queue::test3", messangerId = 2, messangerType = "consumers")
    #self.assertTrue(result['OK'])
    #result = self.myManager.stopConnection(mqURI = "testdir.blabla.ch::Queue::test3", messangerId = 4, messangerType = "consumers")
    #self.assertTrue(result['OK'])
    #dest = {stopConnection
    #dest.update({"/queue/test1": {"producers":[4], "consumers":[1,2,4]}})
    #dest.update({"/queue/test2": {"producers":[2], "consumers":[1,2]}})
    #dest.update({"/topic/test1": {"producers":[1], "consumers":[]}})
    #conn1 = {"MQConnector":None, "destinations":dest}
    #connectionStorage = {"mardirac3.in2p3.fr":conn1}
    #self.assertEqual(self.myManager._connectionStorage, connectionStorage)

#class TestMQConnectionManager_addOrUpdateConnection( TestMQConnectionManager ):
  #def test_success( self ):
    #result = self.myManager.addOrUpdateConnection(mqURI = "testdir.blabla.ch::Queue::test1", params = {}, messangerType = "producer")
    #pass
    #self.assertEqual(result, expectedConn)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_updateConnection ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_connectionExists ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_getConnection ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_deleteConnection ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_addConnection ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_closeConnection ) )
  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_addOrUpdateConnection ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_connectionExists ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_destinationExists ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_messangerExists ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getConnection ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getConnector ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getDestinations ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getMessangersId ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getMessangersIdWithType ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_addMessanger ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_removeMessanger ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getAllMessangersInfo) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
