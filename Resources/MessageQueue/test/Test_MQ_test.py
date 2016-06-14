"""
Tests for the MQListener and MQPublisher helper classes
"""

import unittest
import time
import threading
import socket
import json

from DIRAC import S_OK
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.Core.Utilities.CFG import CFG
from DIRAC.Resources.MessageQueue.MQListener import MQListener
from DIRAC.Resources.MessageQueue.MQPublisher import MQPublisher
from DIRAC.Resources.MessageQueue.MQConnection import MQConnectionError
from DIRAC.Core.Utilities.DErrno import cmpError, EMQCONN, EMQNOM

__RCSID__ = "$Id$"

TEST_CONFIG = """
Resources
{
  MQServices
  {
    mardirac3.in2p3.fr
    {
      MQType = Stomp
      Host = mardirac3.in2p3.fr
      Port = 9165
      User = guest
      Password = guest
      Queues
      {
        TestQueue
        {
          Acknowledgement = True
        }
      }
    }
  }
}
"""

TEST_BAD_CONFIG = """
Resources
{
  MQServices
  {
    rubbish.in2p3.fr
    {
      MQType = Stomp
      Host = rubbish.in2p3.fr
      Port = 9165
      User = guest
      Password = guest
      Queues
      {
        TestQueueBad
        {
          Acknowledgement = True
        }
      }
    }
  }
}
"""

def checkHost( host, port ):

  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  try:
    s.connect( (host, port) )
    result = True
  except socket.error as e:
    result = False
  s.close()
  return result

class MQTestCase( unittest.TestCase ):
  """ Base test class. Defines all the method to test
  """

  def setUp(self):

    mqCFG = CFG()
    mqCFG.loadFromBuffer( TEST_CONFIG )
    gConfigurationData.mergeWithLocal( mqCFG )
    mqCFG.loadFromBuffer( TEST_BAD_CONFIG )
    gConfigurationData.mergeWithLocal( mqCFG )

  @unittest.skipIf( not checkHost( 'mardirac3.in2p3.fr', 9165 ), "Test MQ host is not reachable" )
  def test_listener_publisher(self):
    """ Test simple fully predefined queue with a simple listener
    """

    publisher = None
    with self.assertRaises( MQConnectionError ):
      publisher = MQPublisher( "rubbish.in2p3.fr::TestQueueBad" )

    try:
      publisher = MQPublisher( "TestQueue" )
    except MQConnectionError as exc:
      self.fail( "Fail to create Publisher: %s" % str( exc ) )

    if publisher is None:
      self.fail( "Unknown failure when instantiating MQPublisher" )

    listener = None
    try:
      listener = MQListener( "TestQueue" )
    except MQConnectionError as exc:
      self.fail( "Fail to create Listener: %s" % str( exc ) )

    if listener is None:
      self.fail( "Unknown failure when instantiating MQListener" )

    # Drain messages on the server if any
    result = S_OK()
    while result['OK']:
      result = listener.get()

    message = "Hello World"

    result = publisher.put( message )
    self.assertTrue( result['OK'] )

    time.sleep( 1 )
    result = listener.get()

    print result

    self.assertTrue( result['OK'] )
    self.assertTrue( result['Value'] == message )

    # No more messages available
    result = listener.get()
    self.assertTrue( not result['OK'] )
    self.assertTrue( cmpError( result, EMQNOM ) )

    result = publisher.stop()
    self.assertTrue( result['OK'] )
    result = listener.stop()
    self.assertTrue( result['OK'] )

  def callbackTest( self, headers, message ):

    self.message = json.loads( message )
    self.messageID = headers['message-id']
    return S_OK()

  @unittest.skipIf( not checkHost( 'mardirac3.in2p3.fr', 9165 ), "Test MQ host is not reachable" )
  #@unittest.skip
  def test_consumer( self ):
    """ Test listener in a consumer mode
    """
    self.message = ''

    try:
      publisher = MQPublisher( "TestQueue" )
      listener = MQListener( "TestQueue", callback = self.callbackTest )
    except MQConnectionError as exc:
      self.fail( "Fail to create Publisher: %s" % str( exc ) )

    consumerThread = threading.Thread( target = listener.run )
    consumerThread.start()

    message = 'Hello consumer'
    publisher.put( message )
    time.sleep(1)
    self.assertEqual( self.message, message )

    result = listener.stop()
    self.assertTrue( result['OK'] )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( MQTestCase )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
