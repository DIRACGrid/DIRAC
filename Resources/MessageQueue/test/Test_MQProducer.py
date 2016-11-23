"""Unit tests of MQProducer interface in the DIRAC.Resources.MessageQueue.MProducerQ
"""

import unittest
from DIRAC import S_OK, S_ERROR
from DIRAC.Resources.MessageQueue.MQProducer import MQProducer
from DIRAC.Resources.MessageQueue.MQConnectionManager import MQConnectionManager
from DIRAC.Resources.MessageQueue.MQConnector import MQConnector

class FakeMQConnector( MQConnector ):

  def __init__( self, params={} ):
    super( FakeMQConnector, self ).__init__()

  def start( self):
    return "FakeMQConnection started"

  def stop(self):
    return "FakeMQConnection stopped"

  def disconnect(self):
    return S_OK("FakeMQConnection disconnecting")

  def get(self, destination = ''):
    return "FakeMQConnection getting message"

  def put(self, message, destination = ''):
    return S_OK("FakeMQConnection sending message: " + str(message) + " to: " + destination)


class TestMQProducer( unittest.TestCase ):
  def setUp( self ):
    dest = {}
    dest1 = {"/queue/FakeQueue":{ "publishers":[4], "subscribers":[1,2,4]}}
    dest2 = {"/queue/test2":{ "publishers":[2], "subscribers":[1,2]}}
    dest.update(dest1)
    dest.update(dest2)
    conn1 = {"MQConnector": FakeMQConnector(), "destinations":dest}
    connectionStorage = {"fake.cern.ch":conn1}
    self.myManager = MQConnectionManager(connectionStorage = connectionStorage)
  def tearDown( self ):
    pass
class TestMQProducer_put( TestMQProducer):
  def test_success( self ):
    producer = MQProducer(mqManager = self.myManager, mqURI  = "fake.cern.ch::Queue::FakeQueue", producerId = 1)
    result = producer.put("wow!")
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], "FakeMQConnection sending message: wow! to: /queue/FakeQueue")

  def test_failure( self ):
    producer = MQProducer(mqManager = self.myManager, mqURI  = "bad.cern.ch::Queue::FakeQueue", producerId = 1)
    result = producer.put("wow!")
    self.assertFalse(result['OK'])
    #todo add proper error 

class TestMQProducer_close( TestMQProducer):
  def test_success( self ):
    producer = MQProducer(mqManager = self.myManager, mqURI  = "fake.cern.ch::Queue::FakeQueue", producerId = 1)
    result = producer.close()
    self.assertTrue(result['OK'])
    #producer should not be able to send anything after disconnecting
    result = producer.put("wow!")
    self.assertFalse(result['OK'])

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestMQProducer )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQProducer_put))
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQProducer_close))
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
