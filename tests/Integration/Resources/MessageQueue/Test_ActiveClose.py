from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import unittest
import time
import logging
import sys
import os
import mock
from DIRAC.Resources.MessageQueue.MQCommunication import createProducer
import DIRAC.Resources.MessageQueue.MQCommunication as MQComm
from DIRAC.Resources.MessageQueue.StompMQConnector import StompMQConnector as MyStompConnector
from DIRAC import S_OK, S_ERROR

root = logging.getLogger()
root.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)

TEST_CONFIG = """
Resources
{
  MQServices
  {
    mardirac3.in2p3.fr
    {
      MQType = Stomp
      Host = localhost
      VHost = /
      Port = 61613
      User = guest
      Password = guest
      Queues
      {
        test1
        {
          Acknowledgement = False
        }

        test2
        {
          Acknowledgement = False
        }
      }
      Topics
      {
        test1
        {
          Acknowledgement = False
        }
      }
    }
    testdir.blabla.ch
    {
      MQType = Stomp
      VHost = /
      Host = localhost
      Port = 61613
      SSLVersion = TLSv1
      HostCertificate =
      HostKey =
      User = guest
      Password = guest
      Queues
      {
        test4
        {
          Acknowledgement = False
        }
      }
    }
  }
}
"""


def pseudoCS(mqURI):
  paramsQ1 = {
      'VHost': '/',
      'Queues': 'test1',
      'MQType': 'Stomp',
      'Host': 'localhost',
      'Port': '61613',
      'User': 'guest',
      'Password': 'guest',
      'Acknowledgement ': 'False'}
  paramsQ2 = {
      'VHost': '/',
      'Queues': 'test2',
      'MQType': 'Stomp',
      'Host': 'localhost',
      'Port': '61613',
      'User': 'guest',
      'Password': 'guest',
      'Acknowledgement ': 'False'}
  paramsT3 = {
      'VHost': '/',
      'Topics': 'test1',
      'MQType': 'Stomp',
      'Host': 'localhost',
      'Port': '61613',
      'User': 'guest',
      'Password': 'guest',
      'Acknowledgement ': 'False'}
  paramsQ4 = {
      'VHost': '/',
      'Queues': 'test4',
      'MQType': 'Stomp',
      'Host': 'localhost',
      'Port': '61613',
      'User': 'guest',
      'Password': 'guest',
      'Acknowledgement ': 'False'}
  if mqURI == 'mardirac3.in2p3.fr::Queue::test1':
    return S_OK(paramsQ1)
  elif mqURI == 'mardirac3.in2p3.fr::Queue::test2':
    return S_OK(paramsQ2)
  elif mqURI == 'mardirac3.in2p3.fr::Topic::test1':
    return S_OK(paramsT3)
  elif mqURI == 'testdir.blabla.ch::Queue::test4':
    return S_OK(paramsT3)
  else:
    return S_ERROR("Bad mqURI")


class Test_MQProducers(unittest.TestCase):

  reconnectWasCalled = False

  def setUp(self):
    MQComm.connectionManager.removeAllConnections()
    Test_MQProducers.reconnectWasCalled = False

  def tearDown(self):
    MQComm.connectionManager.removeAllConnections()
    Test_MQProducers.reconnectWasCalled = False

  def getFirstConnection(self, mqConnection):
    result = MQComm.connectionManager.getConnector(mqConnection)
    connector = result['Value']
    return connector.connections.values().next()


class Test_MQProducers_1(Test_MQProducers):

  @mock.patch('DIRAC.Resources.MessageQueue.MQCommunication.getMQParamsFromCS', side_effect=pseudoCS)
  def test_success(self, mock_getMQParamsFromCS):
    result = createProducer(mqURI='mardirac3.in2p3.fr::Queue::test1')
    self.assertTrue(result['OK'])
    producer = result['Value']

    conn1 = self.getFirstConnection('mardirac3.in2p3.fr')
    self.assertTrue(conn1.is_connected())

    result = producer.put('blabla')
    self.assertTrue(result['OK'])
    result = producer.put('blabla')
    self.assertTrue(result['OK'])

    self.assertTrue(conn1.is_connected())
    result = producer.close()
    self.assertTrue(result['OK'])
    result = producer._connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = []
    self.assertEqual(sorted(messengers), sorted(expected))
    self.assertFalse(conn1.is_connected())


class Test_MQProducers_2(Test_MQProducers):

  @mock.patch('DIRAC.Resources.MessageQueue.MQCommunication.getMQParamsFromCS', side_effect=pseudoCS)
  def test_success(self, mock_getMQParamsFromCS):
    result = createProducer(mqURI='mardirac3.in2p3.fr::Queue::test1')
    self.assertTrue(result['OK'])
    producerFirst = result['Value']
    conn1 = self.getFirstConnection('mardirac3.in2p3.fr')
    self.assertTrue(conn1.is_connected())

    result = producerFirst.put('blabla')
    self.assertTrue(result['OK'])
    result = producerFirst.put('blabla')
    self.assertTrue(result['OK'])

    self.assertTrue(conn1.is_connected())

    producers = []
    for i in range(20):
      result = createProducer(mqURI='mardirac3.in2p3.fr::Queue::test1')
      self.assertTrue(result['OK'])
      producer = result['Value']
      producers.append(producer)
      result = producer.put('blabla')
      self.assertTrue(result['OK'])
      result = producer.put('blabla')
      self.assertTrue(result['OK'])

    result = producerFirst.close()
    self.assertTrue(result['OK'])

    self.assertTrue(conn1.is_connected())
    time.sleep(3)
    self.assertTrue(conn1.is_connected())

    for p in producers:
      result = p.close()
      self.assertTrue(result['OK'])
    self.assertFalse(conn1.is_connected())


class Test_MQProducers_3(Test_MQProducers):

  @mock.patch('DIRAC.Resources.MessageQueue.MQCommunication.getMQParamsFromCS', side_effect=pseudoCS)
  def test_success(self, mock_getMQParamsFromCS):

    conn1 = None
    # creating and closing
    for i in range(20):
      result = createProducer(mqURI='mardirac3.in2p3.fr::Queue::test1')
      if i == 1:
        conn1 = self.getFirstConnection('mardirac3.in2p3.fr')
      self.assertTrue(result['OK'])
      producer = result['Value']
      result = producer.put('blabla')
      self.assertTrue(result['OK'])
      result = producer.put('blabla')
      self.assertTrue(result['OK'])
      result = producer.close()
      self.assertTrue(result['OK'])

    self.assertFalse(conn1.is_connected())
    time.sleep(3)
    self.assertFalse(conn1.is_connected())


def pseudoReconnect():
  Test_MQProducers.reconnectWasCalled = True


def pseudocreateMQConnector(parameters=None):
  obj = MyStompConnector(parameters)
  return S_OK(obj)


def stopServer():
  os.system("rabbitmqctl stop_app")


def startServer():
  os.system("rabbitmqctl start_app")


class Test_MQProducers_4(Test_MQProducers):

  @mock.patch('DIRAC.Resources.MessageQueue.StompMQConnector.StompMQConnector.reconnect', side_effect=pseudoReconnect)
  @mock.patch('DIRAC.Resources.MessageQueue.MQConnectionManager.createMQConnector', side_effect=pseudocreateMQConnector)
  @mock.patch('DIRAC.Resources.MessageQueue.MQCommunication.getMQParamsFromCS', side_effect=pseudoCS)
  def test_success(self, mock_getMQParamsFromCS, mock_createMQConnector, mock_reconnect):
    result = createProducer(mqURI='mardirac3.in2p3.fr::Queue::test1')
    self.assertTrue(result['OK'])
    producer = result['Value']
    result = producer.put('blabla')
    self.assertTrue(result['OK'])
    result = producer.put('blabla')
    self.assertTrue(result['OK'])
    result = producer.close()
    self.assertTrue(result['OK'])
    self.assertFalse(Test_MQProducers.reconnectWasCalled)
    result = producer.close()
    self.assertFalse(Test_MQProducers.reconnectWasCalled)
    for _ in range(20):
      result = createProducer(mqURI='mardirac3.in2p3.fr::Queue::test1')
      self.assertTrue(result['OK'])
      producer = result['Value']
      result = producer.put('blabla')
      self.assertTrue(result['OK'])
      result = producer.put('blabla')
      self.assertTrue(result['OK'])
      result = producer.close()
      self.assertTrue(result['OK'])

    self.assertFalse(Test_MQProducers.reconnectWasCalled)
    time.sleep(3)
    self.assertFalse(Test_MQProducers.reconnectWasCalled)


class Test_MQProducers_5(Test_MQProducers):
  @mock.patch('DIRAC.Resources.MessageQueue.MQCommunication.getMQParamsFromCS', side_effect=pseudoCS)
  def test_success(self, mock_getMQParamsFromCS):
    result = createProducer(mqURI='mardirac3.in2p3.fr::Queue::test1')
    self.assertTrue(result['OK'])
    conn1 = self.getFirstConnection('mardirac3.in2p3.fr')
    self.assertTrue(conn1.is_connected())
    producer = result['Value']
    result = producer.put('blabla')
    self.assertTrue(result['OK'])
    # correct permissions must be set.
    # stopServer()
    # self.assertFalse(conn1.is_connected())
    # time.sleep(5)
    # startServer()
    # time.sleep(10) #this value will be timeout dependend so if it is to short it will fail.
    # self.assertTrue(conn1.is_connected())
    result = producer.put('blabla')
    self.assertTrue(result['OK'])

    result = producer.close()
    self.assertTrue(result['OK'])
    self.assertFalse(conn1.is_connected())

    result = producer._connectionManager.getAllMessengers()
    self.assertTrue(result['OK'])
    messengers = result['Value']
    expected = []
    self.assertEqual(sorted(messengers), sorted(expected))


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(Test_MQProducers)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_MQProducers_1))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_MQProducers_2))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_MQProducers_3))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_MQProducers_4))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_MQProducers_5))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
