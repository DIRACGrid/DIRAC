"""Unit tests of MQConnectionManager in the DIRAC.Resources.MessageQueue.MConnectionManager
   Also, test of internal functions for mq connection storage.
"""

import unittest
import mock
from DIRAC import S_OK
from DIRAC.Resources.MessageQueue.MQConnectionManager import MQConnectionManager

from DIRAC.Resources.MessageQueue.MQConnectionManager import _connectionExists
from DIRAC.Resources.MessageQueue.MQConnectionManager import _destinationExists
from DIRAC.Resources.MessageQueue.MQConnectionManager import _MessangerExists
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getConnection
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getAllConnections
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getConnector
from DIRAC.Resources.MessageQueue.MQConnectionManager import _setConnector
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getDestinations
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getMessangersId
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getMessangersIdWithType
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getAllMessangersInfo
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getAllMessangersId
from DIRAC.Resources.MessageQueue.MQConnectionManager import _getAllMessangersIdWithType
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

class TestMQConnectionStorageFunctions_getAllConnections( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    expectedOutput = ['testdir.blabla.ch','mardirac3.in2p3.fr']
    self.assertEqual(sorted(_getAllConnections(self.storage)),sorted(expectedOutput))

class TestMQConnectionStorageFunctions_getConnector( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    self.assertEqual(_getConnector(self.storage,'testdir.blabla.ch'),'TestConnector2')
  def test_failure( self ):
    self.assertIsNone(_getConnector(self.storage,'nonexisiting'))

class TestMQConnectionStorageFunctions_setConnector( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    self.assertTrue(_setConnector(self.storage,'testdir.blabla.ch', 'TestConnector5'))
    self.assertEqual(_getConnector(self.storage,'testdir.blabla.ch'),'TestConnector5')
  def test_failure( self ):
    self.assertFalse(_setConnector(self.storage,'nonexisiting', 'TestConnector3'))

class TestMQConnectionStorageFunctions_getDestinations( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    expectedDests ={'/queue/test1': ['producer4', 'consumer1', 'consumer2', 'consumer4'],
                    '/queue/test2': ['producer2', 'consumer1', 'consumer2'],
                    '/topic/test1': ['producer1']}
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

class TestMQConnectionStorageFunctions_getAllMessangersId( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    expectedOutput= ['producer4', 'consumer1', 'consumer2', 'consumer4', 'producer2', 'consumer1', 'consumer2', 'producer1', 'producer1', 'consumer2', 'consumer3', 'consumer4']
    self.assertEqual(sorted(_getAllMessangersId(self.storage)),sorted(expectedOutput))

class TestMQConnectionStorageFunctions_getAllMessangersIdWithType( TestMQConnectionStorageFunctions ):
  def test_success( self ):
    expectedOutput= ['consumer1', 'consumer2', 'consumer4', 'consumer1', 'consumer2','consumer2', 'consumer3', 'consumer4']
    self.assertEqual(sorted(_getAllMessangersIdWithType(self.storage, 'consumer')),sorted(expectedOutput))
    expectedOutput= ['producer4', 'producer2', 'producer1', 'producer1']
    self.assertEqual(sorted(_getAllMessangersIdWithType(self.storage, 'producer')),sorted(expectedOutput))


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

class TestMQConnectionManager_addNewMessanger( TestMQConnectionManager ):
  def test_success( self ):
    result = self.myManager.addNewMessanger(mqURI = "mardirac3.in2p3.fr::Queue::test1", messangerType = "producer"  )
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], 'producer5')
    expectedOutput= ['mardirac3.in2p3.fr/queue/test1/producer5', 'mardirac3.in2p3.fr/queue/test1/producer4', 'mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'mardirac3.in2p3.fr/topic/test1/producer1', 'testdir.blabla.ch/queue/test3/producer1', 'testdir.blabla.ch/queue/test3/consumer2', 'testdir.blabla.ch/queue/test3/consumer3', 'testdir.blabla.ch/queue/test3/consumer4']
    self.assertEqual(sorted(_getAllMessangersInfo(self.myManager._connectionStorage)),sorted(expectedOutput))

  def test_success2( self ):
    result = self.myManager.addNewMessanger(mqURI = "mardirac3.in2p3.fr::Topic::test1", messangerType = "consumer"  )
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], 'consumer5')

  def test_success3( self ):
    result = self.myManager.addNewMessanger(mqURI = "testdir.blabla.ch::Queue::test3", messangerType = "consumer"  )
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], 'consumer5')

  def test_success4( self ):
    #connection does not exist
    result = self.myManager.addNewMessanger(mqURI = "noexisting.blabla.ch::Queue::test3", messangerType = "consumer"  )
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], 'consumer5')
    expectedOutput= ['noexisting.blabla.ch/queue/test3/consumer5', 'mardirac3.in2p3.fr/queue/test1/producer4', 'mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'mardirac3.in2p3.fr/topic/test1/producer1', 'testdir.blabla.ch/queue/test3/producer1', 'testdir.blabla.ch/queue/test3/consumer2', 'testdir.blabla.ch/queue/test3/consumer3', 'testdir.blabla.ch/queue/test3/consumer4']
    self.assertEqual(sorted(_getAllMessangersInfo(self.myManager._connectionStorage)),sorted(expectedOutput))

class TestMQConnectionManager_startConnection( TestMQConnectionManager ):
  def test_success( self ):
    #existing connection
    result = self.myManager.startConnection(mqURI = "mardirac3.in2p3.fr::Queue::test1", params ={}, messangerType = "producer")
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], 'producer5')
    expectedOutput= ['mardirac3.in2p3.fr/queue/test1/producer5', 'mardirac3.in2p3.fr/queue/test1/producer4', 'mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'mardirac3.in2p3.fr/topic/test1/producer1', 'testdir.blabla.ch/queue/test3/producer1', 'testdir.blabla.ch/queue/test3/consumer2', 'testdir.blabla.ch/queue/test3/consumer3', 'testdir.blabla.ch/queue/test3/consumer4']
    self.assertEqual(sorted(_getAllMessangersInfo(self.myManager._connectionStorage)),sorted(expectedOutput))

  @mock.patch('DIRAC.Resources.MessageQueue.MQConnectionManager.MQConnectionManager.createConnectorAndConnect')
  def test_success2( self, mock_createConnectorAndConnect):
    #connection does not exist
    mock_createConnectorAndConnect.return_value = S_OK('MyConnector')
    result = self.myManager.startConnection(mqURI = "noexisting.blabla.ch::Queue::test3", params={}, messangerType = "consumer"  )
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], 'consumer5')
    expectedOutput= ['noexisting.blabla.ch/queue/test3/consumer5', 'mardirac3.in2p3.fr/queue/test1/producer4', 'mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'mardirac3.in2p3.fr/topic/test1/producer1', 'testdir.blabla.ch/queue/test3/producer1', 'testdir.blabla.ch/queue/test3/consumer2', 'testdir.blabla.ch/queue/test3/consumer3', 'testdir.blabla.ch/queue/test3/consumer4']
    self.assertEqual(sorted(_getAllMessangersInfo(self.myManager._connectionStorage)),sorted(expectedOutput))
    self.assertEqual(_getConnector(self.myManager._connectionStorage, 'noexisting.blabla.ch'), 'MyConnector')


class TestMQConnectionManager_stopConnection( TestMQConnectionManager ):
  def test_success( self ):
    result = self.myManager.stopConnection(mqURI = "mardirac3.in2p3.fr::Queue::test1", messangerId = "producer4")
    self.assertTrue(result['OK'])
    expectedOutput= ['mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'mardirac3.in2p3.fr/topic/test1/producer1', 'testdir.blabla.ch/queue/test3/producer1', 'testdir.blabla.ch/queue/test3/consumer2', 'testdir.blabla.ch/queue/test3/consumer3', 'testdir.blabla.ch/queue/test3/consumer4']
    self.assertEqual(sorted(_getAllMessangersInfo(self.myManager._connectionStorage)),sorted(expectedOutput))

  def test_success2( self ):
    result = self.myManager.stopConnection(mqURI = "mardirac3.in2p3.fr::Topic::test1", messangerId = "producer1")
    self.assertTrue(result['OK'])
    expectedOutput= ['mardirac3.in2p3.fr/queue/test1/producer4', 'mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'testdir.blabla.ch/queue/test3/producer1', 'testdir.blabla.ch/queue/test3/consumer2', 'testdir.blabla.ch/queue/test3/consumer3', 'testdir.blabla.ch/queue/test3/consumer4']
    self.assertEqual(sorted(_getAllMessangersInfo(self.myManager._connectionStorage)),sorted(expectedOutput))

  @mock.patch('DIRAC.Resources.MessageQueue.MQConnectionManager.MQConnectionManager.unsubscribe')
  @mock.patch('DIRAC.Resources.MessageQueue.MQConnectionManager.MQConnectionManager.disconnect')
  def test_success3( self, mock_disconnect, mock_unsubscribe ):
    mock_disconnect.return_value = S_OK()
    mock_unsubscribe.return_value = S_OK()
    result = self.myManager.stopConnection(mqURI = "testdir.blabla.ch::Queue::test3", messangerId = "consumer3")
    self.assertTrue(result['OK'])
    result = self.myManager.stopConnection(mqURI = "testdir.blabla.ch::Queue::test3", messangerId = "producer1")
    self.assertTrue(result['OK'])
    result = self.myManager.stopConnection(mqURI = "testdir.blabla.ch::Queue::test3", messangerId = "consumer2")
    self.assertTrue(result['OK'])
    result = self.myManager.stopConnection(mqURI = "testdir.blabla.ch::Queue::test3", messangerId = "consumer4")
    self.assertTrue(result['OK'])
    expectedOutput= ['mardirac3.in2p3.fr/queue/test1/producer4', 'mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'mardirac3.in2p3.fr/topic/test1/producer1']
    self.assertEqual(sorted(_getAllMessangersInfo(self.myManager._connectionStorage)),sorted(expectedOutput))

class TestMQConnectionManager_removeAllConnections( TestMQConnectionManager ):
  @mock.patch('DIRAC.Resources.MessageQueue.MQConnectionManager.MQConnectionManager.disconnect')
  def test_success( self, mock_disconnect):
    mock_disconnect.return_value = S_OK()
    result = self.myManager.removeAllConnections()
    self.assertTrue(result['OK'])
    expectedOutput= []
    self.assertEqual(sorted(_getAllMessangersInfo(self.myManager._connectionStorage)),sorted(expectedOutput))

class TestMQConnectionManager_getAllMessangers( TestMQConnectionManager ):
  def test_success( self ):
    result = self.myManager.getAllMessangers()
    self.assertTrue(result['OK'])
    expectedOutput= ['mardirac3.in2p3.fr/queue/test1/producer4', 'mardirac3.in2p3.fr/queue/test1/consumer1', 'mardirac3.in2p3.fr/queue/test1/consumer2', 'mardirac3.in2p3.fr/queue/test1/consumer4', 'mardirac3.in2p3.fr/queue/test2/producer2', 'mardirac3.in2p3.fr/queue/test2/consumer1', 'mardirac3.in2p3.fr/queue/test2/consumer2', 'mardirac3.in2p3.fr/topic/test1/producer1', 'testdir.blabla.ch/queue/test3/producer1', 'testdir.blabla.ch/queue/test3/consumer2', 'testdir.blabla.ch/queue/test3/consumer3', 'testdir.blabla.ch/queue/test3/consumer4']
    self.assertEqual(sorted(_getAllMessangersInfo(self.myManager._connectionStorage)),sorted(expectedOutput))

class TestMQConnectionManager_getConnector( TestMQConnectionManager ):
  def test_success( self ):
    result = self.myManager.getConnector('mardirac3.in2p3.fr')
    self.assertTrue(result['OK'])
  def test_failure( self ):
    result = self.myManager.getConnector('nonexistent.in2p3.fr')
    self.assertEqual(result['Message'], 'Failed to get the MQConnector!')

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_addNewMessanger ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_startConnection ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_stopConnection ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_removeAllConnections ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_getAllMessangers ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionManager_getConnector ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_connectionExists ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_destinationExists ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_messangerExists ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getConnection ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getAllConnections ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getConnector ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_setConnector ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getDestinations ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getMessangersId ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getMessangersIdWithType ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_addMessanger ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_removeMessanger ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getAllMessangersInfo) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getAllMessangersId) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQConnectionStorageFunctions_getAllMessangersIdWithType) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
