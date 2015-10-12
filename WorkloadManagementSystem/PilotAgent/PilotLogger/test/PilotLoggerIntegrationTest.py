""" Integration tests for PilotLogger.
    It is assumed that the RabbitMQ server is set up with the rabbitmq_auth_mechanism_ssl and rabbitmq_stomp
    modules enabled and listening at the server_ip:61614 socket. In the RabbitMQ server there must be a queue
    available: /queue/test .
    For a moment server_ip will be set
    to 127.0.0.1. The TestStompConsumer class is provided.
"""

import unittest
import os
import json
from DIRAC.WorkloadManagementSystem.PilotAgent.PilotLogger.PilotLogger import PilotLogger, getPilotUUIDFromFile
from DIRAC.WorkloadManagementSystem.PilotAgent.PilotLogger.PilotLogger import send, connect
from DIRAC.WorkloadManagementSystem.PilotAgent.PilotLogger.PilotLoggerTools import generateUniqueIDAndSaveToFile
from DIRAC.WorkloadManagementSystem.PilotAgent.PilotLogger.TestStompConsumer  import TestStompConsumer


class TestPilotLoggerIntegration( unittest.TestCase ):

  def setUp( self ):
    self.networkCfg=[('127.0.0.1',int(61614))]
    self.sslCfg = {
    'key_file':'/home/krzemien/workdir/lhcb/dirac_development/certificates/client/key.pem',
    'cert_file' : '/home/krzemien/workdir/lhcb/dirac_development/certificates/client/cert.pem',
    'ca_certs' : '/home/krzemien/workdir/lhcb/dirac_development/certificates/testca/cacert.pem'
    }
    self.consumer = TestStompConsumer()
    self.testFile = 'UUID_file_to_test'
    generateUniqueIDAndSaveToFile( self.testFile )
    self.logger = PilotLogger(self.testFile)
  def tearDown( self ):
    pass
    try:
      os.remove( self.testFile )
    except OSError:
      pass


class Test_connect( TestPilotLoggerIntegration ):

  def test_success( self ):
    conn = connect(self.networkCfg, self.sslCfg)
    self.assertTrue(conn)

  def test_failure_bad_port( self ):
    netCfg_bad = [('127.0.0.1',int(61666))]
    conn = connect(netCfg_bad, self.sslCfg)
    self.assertFalse(conn)

  def test_failure_bad_ssl_path( self ):
    sslCfg_bad = self.sslCfg.copy()
    sslCfg_bad['key_file'] = 'blabla'
    conn = connect(self.networkCfg, sslCfg_bad)
    self.assertFalse(conn)

class Test_send( TestPilotLoggerIntegration ):

  def test_success( self ):
    conn = connect(self.networkCfg, self.sslCfg)
    source_msg = 'toto'
    result = send(source_msg, '/queue/test', conn)
    self.assertTrue(result)
    self.consumer.start()
    msgs = self.consumer.stopAndReturnAllMessages()
    self.assertTrue(msgs)
    last_msg = msgs[-1]
    self.assertEqual(source_msg, last_msg)

  def test_failure_bad_connection_handler( self ):
    conn = None
    result = send('toto', '/queue/test', conn)
    self.assertFalse(result)

  def test_failure_bad_queue_name( self ):
    conn = connect(self.networkCfg, self.sslCfg)
    send('toto', '/queue/Nonexist', conn)

class TestPilotLogger_SendMessage( TestPilotLoggerIntegration ):

  def test_success( self ):
    sourceStatus = 'test_status'
    result = self.logger.sendMessage(sourceStatus)
    self.assertTrue(result)

    self.consumer.start()
    msgs = self.consumer.stopAndReturnAllMessages()
    self.assertTrue(msgs)
    #PilotLoggin messages are JSON, so we need to decode it
    last_msg = json.loads(msgs[-1])
    recStatus = last_msg["minorStatus"]
    self.assertEqual(sourceStatus, recStatus)

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestPilotLoggerIntegration )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( Test_connect ))
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( Test_send ))
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestPilotLogger_SendMessage ))

  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

