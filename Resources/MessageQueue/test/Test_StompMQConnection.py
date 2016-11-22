"""
Tests of class for management Stomp MQ connections
"""

import DIRAC.Resources.MessageQueue.StompMQConnection as module
import unittest

from mock.mock import MagicMock

__RCSID__ = "$Id$"


HOST = 'mq.dirac.net'
PORT = 61631
VHOST = 'vhost'
USER = 'guest'
PASSWORD = 'guest'
QUEUE = 'TestQueue'
TOPIC = 'TestTopic'
ACKNOWLEDGEMENT = 'True'

PARAMETERS = \
{
  'Host': HOST,
  'Port': PORT,
  'VHost': VHOST,
  'User': USER,
  'Password': PASSWORD,
  'Queue': QUEUE,
  'Topic': TOPIC,
  'Acknowledgement': ACKNOWLEDGEMENT
}

SSL_VERSION = 'TLSv1'
HOSTCERT = '/etc/hostcert.pem'
HOSTKEY = '/etc/hostkey.pem'

SSL_PARAMETERS = PARAMETERS.copy()
SSL_PARAMETERS.update( {
  'SSLVersion': SSL_VERSION,
  'HostCertificate': HOSTCERT,
  'HostKey': HOSTKEY
} )
del SSL_PARAMETERS['Queue']

IP1 = '192.168.10.23'
IP2 = '172.16.18.40'
GETHOSTBYNAME = ( HOST, [], [IP1, IP2] )


# stubs
def testCallback():
  pass

listeners = {}
def setListener( name, listener ):
  listeners[name] = listener

def getListener( name ):
  return listeners[name]

class StompMQConnectionSuccessTestCase( unittest.TestCase ):
  """ Test class to check success scenarios.
  """

  def setUp(self):

    # external dependencies
    module.socket.gethostbyname_ex = MagicMock( return_value = GETHOSTBYNAME )

    module.time = MagicMock()
    module.ssl = MagicMock()
    # module.ssl.PROTOCOL_TLSv1 = 1

    module.makeGuid = MagicMock( return_value = 'guid:1234' )

    # fake connection object
    connectionMock = MagicMock()
    connectionMock.is_connected.return_value = False

    module.stomp.Connection = MagicMock()
    module.stomp.Connection.return_value = connectionMock
    module.stomp.ConnectionListener = MagicMock()

    # prepare test object
    self.mqconnection = module.StompMQConnection()
    self.mqconnection.log = MagicMock()

  def test_createStompListener( self ):

    connection = module.stomp.Connection()
    listener = module.StompListener( testCallback, ACKNOWLEDGEMENT, connection )

    self.assertEqual( listener.callback, testCallback )
    self.assertEqual( listener.ack, ACKNOWLEDGEMENT )
    self.assertEqual( listener.connection, connection )

  def test_makeConnection( self ):

    result = self.mqconnection.setupConnection( PARAMETERS, testCallback )
    self.assertTrue( result['OK'] )

    # check parameters
    self.assertEqual( self.mqconnection.callback, testCallback )
    # self.assertEqual( self.mqconnection.receiver, RECEIVER )
    self.assertEqual( self.mqconnection.destination, '/queue/%s' % QUEUE )
    for ip in [IP1, IP2]:
      self.assertIsNotNone( self.mqconnection.connection[ip] )
    
    # check calls
    module.stomp.Connection.assert_any_call( 
                                            [ ( IP1, int( PORT ) ) ],
                                            username = USER,
                                            passcode = PASSWORD,
                                            vhost = VHOST
                                           )
    module.stomp.Connection.assert_any_call( 
                                            [ ( IP2, int( PORT ) ) ],
                                            username = USER,
                                            passcode = PASSWORD,
                                            vhost = VHOST
                                           )

  def test_makeSSLConnection( self ):

    result = self.mqconnection.setupConnection( SSL_PARAMETERS, testCallback )
    self.assertTrue( result['OK'] )

    # check parameters
    self.assertEqual( self.mqconnection.callback, testCallback )
    # self.assertEqual( self.mqconnection.receiver, RECEIVER )
    self.assertEqual( self.mqconnection.destination, '/topic/%s' % TOPIC )
    for ip in [IP1, IP2]:
      self.assertIsNotNone( self.mqconnection.connection[ip] )

    # check calls
    module.stomp.Connection.assert_any_call( 
                                            [ ( IP1, int( PORT ) ) ],
                                            use_ssl = True,
                                            ssl_version = module.ssl.PROTOCOL_TLSv1,
                                            ssl_key_file = HOSTKEY,
                                            ssl_cert_file = HOSTCERT,
                                            username = USER,
                                            passcode = PASSWORD,
                                            vhost = VHOST,
                                           )
    
    module.stomp.Connection.assert_any_call( 
                                            [ ( IP2, int( PORT ) ) ],
                                            use_ssl = True,
                                            ssl_version = module.ssl.PROTOCOL_TLSv1,
                                            ssl_key_file = HOSTKEY,
                                            ssl_cert_file = HOSTCERT,
                                            username = USER,
                                            passcode = PASSWORD,
                                            vhost = VHOST,
                                           )
    
class StompMQConnectionFailureTestCase( unittest.TestCase ):
  """ Test class to check failure scenarios.
  """

  def setUp( self ):

    # external dependencies
    module.socket.gethostbyname_ex = MagicMock( return_value = GETHOSTBYNAME )

    module.time = MagicMock()
    module.ssl = MagicMock()

    module.makeGuid = MagicMock( return_value = 'guid:1234' )

    # fake connection object
    self.connectionMock = MagicMock()
    self.connectionMock.is_connected.return_value = False

    module.stomp.Connection = MagicMock()
    module.stomp.Connection.return_value = self.connectionMock

    # prepare test object
    self.mqconnection = module.StompMQConnection()
    self.mqconnection.log = MagicMock()


  def test_connectionError( self ):

    self.connectionMock.connect.side_effect = Exception()

    result = self.mqconnection.setupConnection()
    self.assertFalse( result['OK'] )

    result = self.mqconnection.setupConnection( PARAMETERS )
    self.assertFalse( result['OK'] )

    result = self.mqconnection.setupConnection( PARAMETERS, testCallback )
    self.assertFalse( result['OK'] )


  def test_invalidSSLVersion( self ):

    parameters = SSL_PARAMETERS.copy()
    parameters['SSLVersion'] = '1234'

    result = self.mqconnection.setupConnection( parameters, testCallback )
    self.assertFalse( result['OK'] )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( StompMQConnectionSuccessTestCase )
  suite.addTests( unittest.defaultTestLoader.loadTestsFromTestCase( StompMQConnectionFailureTestCase ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
