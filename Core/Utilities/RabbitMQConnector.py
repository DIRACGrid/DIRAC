"""
Class for management of RabbitMQ connections
"""

__RCSID__ = "$Id$"

import json
import stomp
import threading
from DIRAC.Core.Utilities.MQConnector import MQConnector
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

class RabbitConnection( MQConnector ):
  """
  Class for management of RabbitMQ connections
  Allows to both send and receive messages from a queue
  The class also implements callback functions to be able to act as a listener and receive messages
  """

  def __init__( self ):
    self.host = None
    self.port = None
    self.user = None
    self.vH = None
    self.password = None
    self.queueName = None
    self.type = None

    self.receiver = False
    self.msgList = []
    self.lock = threading.Lock()
    self.connection = None

  # Callback functions for message receiving mode

  def defaultCallback( self, headers, message ):
    """
    Default callback function called every time something is read from the queue
    """
    dictionary = json.loads( message )
    with self.lock:
      self.msgList.append( dictionary )

  # Rest of functions

  def setupConnection( self, system, queueName, receive = False, messageCallback = None ):
    """
    Establishes a new connection to RabbitMQ
    system indicates in which System the queue works
    queueName is the name of the queue to read from/write to
    receive indicates whether this object will read from the queue or read from it
    exchange indicates whether the destination will be a exchange (True) or a queue (False). Only taken into account if receive = True
    messageCallback is the function to be called when a new message is received from the queue ( only receiver mode ).
    If None, the defaultCallback method is used instead
    """
    self.receiver = receive

    if self.receiver:
      if messageCallback:
        self.on_message = messageCallback
      else:
        self.on_message = self.defaultCallback

    # Read parameters from CS
    result = self.setQueueParameters( system, queueName )
    if not result[ 'OK' ]:
      return result

    # Make the actual connection
    try:
      self.connection = stomp.Connection( [ ( self.host, int( self.port ) ) ], vhost = self.vH )
      self.connection.start()
      self.connection.connect( username = self.user, passcode = self.password )

      if self.receiver:
        self.connection.set_listener( '', self )
        self.connection.subscribe( destination = '/queue/%s' % self.queueName, id = self.queueName, headers = { 'persistent': 'true' } )
    except Exception as e:
      return S_ERROR( 'Failed to setup connection: %s' % e )

    return S_OK( 'Setup successful' )

  def put( self, message ):
    """
    Sends a message to the queue
    message contains the body of the message
    """
    try:
      if isinstance( message, list ):
        for msg in message:
          self.connection.send( body = json.dumps( msg ), destination = '/queue/%s' % self.queueName )
      else:
        self.connection.send( body = json.dumps( message ), destination = '/queue/%s' % self.queueName )
    except Exception as e:
      return S_ERROR( 'Failed to send message: %s' % e )

    return S_OK( 'Message sent successfully' )

  def get( self ):
    """
    Retrieves a message from the queue ( if any )
    Returns S_ERROR if there are no messages in the queue
    This method is only useful if the default behaviour for the message callback is being used
    """
    if not self.receiver:
      return S_ERROR( 'Instance not configured to receive messages' )

    with self.lock:
      if self.msgList:
        msg = self.msgList.pop( 0 )
      else:
        return S_ERROR( 'No messages in queue' )

    return S_OK( msg )

  def unsetupConnection( self ):
    """
    Disconnects from the RabbitMQ server
    """
    try:
      self.connection.disconnect()
    except Exception as e:
      return S_ERROR( 'Failed to disconnect: %s' % e )

    return S_OK( 'Disconnection successful' )
