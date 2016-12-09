"""
StompMQConnector tests  
"""

import DIRAC.Resources.MessageQueue.StompMQConnector as module
import unittest

from mock import MagicMock

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

class StompMQConnectorSuccessTestCase( unittest.TestCase ):
  """ Test class to check success scenarios.
  """

  def setUp(self):

    # external dependencies
    module.socket.gethostbyname_ex = MagicMock( return_value = GETHOSTBYNAME )

    module.time = MagicMock()
    module.ssl = MagicMock()

    connectionMock = MagicMock()
    connectionMock.is_connected.return_value = False

    module.stomp.Connection = MagicMock()
    module.stomp.Connection.return_value = connectionMock
    module.stomp.ConnectionListener = MagicMock()

    # prepare test object
    self.mqConnector = module.StompMQConnector()
    self.mqConnector.log = MagicMock()

  def test_createStompListener( self ):
    connection = module.stomp.Connection()
    messengerId = 'producer1'
    listener = module.StompListener( testCallback, ACKNOWLEDGEMENT, connection, messengerId )

    self.assertEqual( listener.callback, testCallback )
    self.assertEqual( listener.ack, ACKNOWLEDGEMENT )
    self.assertEqual( listener.connection, connection )
    self.assertEqual( listener.mId, messengerId )

  def test_makeConnection( self ):
    result = self.mqConnector.setupConnection( parameters = PARAMETERS)
    self.assertTrue( result['OK'] )

    # check parameters
    PARAMETERS.update({'IP':IP1})
    self.assertEqual( sorted(self.mqConnector.parameters), sorted(PARAMETERS) )
    
    # check calls
    module.stomp.Connection.assert_any_call( 
                                            [ ( IP1, int( PORT ) ) ],
                                            vhost = VHOST
                                           )

  def test_makeSSLConnection( self ):

    result = self.mqConnector.setupConnection( SSL_PARAMETERS)
    self.assertTrue( result['OK'] )

    ## check parameters
    for ip in [IP1]:
      self.assertIsNotNone( self.mqConnector.connection[ip] )

    # check calls
    module.stomp.Connection.assert_any_call( 
                                            [ ( IP1, int( PORT ) ) ],
                                            use_ssl = True,
                                            ssl_version = module.ssl.PROTOCOL_TLSv1,
                                            ssl_key_file = HOSTKEY,
                                            ssl_cert_file = HOSTCERT,
                                            vhost = VHOST,
                                           )
    
class StompMQConnectorFailureTestCase( unittest.TestCase ):
  """ Test class to check failure scenarios.
  """

  def setUp( self ):

    # external dependencies
    module.socket.gethostbyname_ex = MagicMock( return_value = GETHOSTBYNAME )

    module.time = MagicMock()
    module.ssl = MagicMock()

    # fake connection object
    self.connectionMock = MagicMock()
    self.connectionMock.is_connected.return_value = False

    module.stomp.Connection = MagicMock()
    module.stomp.Connection.return_value = self.connectionMock

    # prepare test object
    self.mqConnector = module.StompMQConnector()
    self.mqConnector.log = MagicMock()


  def test_invalidSSLVersion( self ):

    parameters = SSL_PARAMETERS.copy()
    parameters['SSLVersion'] = '1234'

    result = self.mqConnector.setupConnection( parameters )
    self.assertFalse( result['OK'] )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( StompMQConnectorSuccessTestCase )
  suite.addTests( unittest.defaultTestLoader.loadTestsFromTestCase( StompMQConnectorFailureTestCase ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
