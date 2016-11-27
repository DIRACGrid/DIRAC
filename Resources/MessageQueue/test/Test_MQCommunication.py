"""Unit tests of MQCommunication interface in the DIRAC.Resources.MessageQueue.MQCommunication
"""

import unittest
from DIRAC.Resources.MessageQueue.MQCommunication import createConsumer
from DIRAC.Resources.MessageQueue.MQCommunication import createProducer
from DIRAC.Resources.MessageQueue.MQCommunication import setupConnection
from DIRAC import S_OK, S_ERROR
import mock
import time

class TestMQCommunication( unittest.TestCase ):
  def setUp( self ):
    pass
  def tearDown( self ):
    pass

class TestMQCommunication_myProducer( TestMQCommunication):
  @mock.patch('DIRAC.Resources.MessageQueue.MQCommunication.getMQParamsFromCS')
  def test_success(self, mock_getMQParamsFromCS):
    mock_getMQParamsFromCS.return_value = S_OK({'VHost':'/', 'Queue':'test2', 'MQType':'Stomp', 'Host':'localhost', 'Port':'61613', 'User':'ala', 'Password':'ala'})
    result = createProducer(mqURI = 'localhost::Queue::test2')
    self.assertTrue(result['OK'])
    producer = result['Value']
    result = producer.put( 'blabla')
    self.assertTrue(result['OK'])
    result = producer.put( 'blable')
    mock_getMQParamsFromCS.return_value = S_OK({'VHost':'/', 'Queue':'test2', 'MQType':'Stomp', 'Host':'localhost', 'Port':'61613', 'User':'ala', 'Password':'ala'})
    result = createProducer(mqURI = 'localhost::Queue::test2')
    self.assertTrue(result['OK'])
    producer2 = result['Value']
    result = producer2.put( 'blabla2')
    self.assertTrue(result['OK'])
    result = producer2.put( 'blable2')
    self.assertTrue(result['OK'])
    mock_getMQParamsFromCS.return_value = S_OK({'VHost':'/', 'Queue':'test3', 'MQType':'Stomp', 'Host':'localhost', 'Port':'61613', 'User':'ala', 'Password':'ala'})
    result = createProducer(mqURI = 'localhost::Queue::test3')
    self.assertTrue(result['OK'])
    producer3 = result['Value']
    result = producer3.put( 'blabla3')
    self.assertTrue(result['OK'])
    result = producer3.put( 'blable3')
    self.assertTrue(result['OK'])
    result = producer2.close()
    self.assertTrue(result['OK'])
    producer3.close()

class TestMQCommunication_myConsumer( TestMQCommunication):
  @mock.patch('DIRAC.Resources.MessageQueue.MQCommunication.getMQParamsFromCS')
  def test_success(self, mock_getMQParamsFromCS):
    mock_getMQParamsFromCS.return_value = S_OK({'VHost':'/', 'Queue':'test2', 'MQType':'Stomp', 'Host':'localhost', 'Port':'61613', 'User':'ala', 'Password':'ala'})
    consumer = createConsumer(mqURI = 'localhost::Queue::test2')['Value']
    producer2 = createProducer(mqURI = 'localhost::Queue::test2')['Value']
    result = producer2.put( 'blabla2')
    self.assertTrue(result['OK'])
    result = producer2.put( 'blable3')
    self.assertTrue(result['OK'])
    time.sleep(2)
    result = consumer.get()
    self.assertTrue(result['OK'])
    result = consumer.get()
    self.assertTrue(result['OK'])

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestMQCommunication )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQCommunication_myProducer))
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMQCommunication_myConsumer))
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
