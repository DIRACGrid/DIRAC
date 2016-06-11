"""
Class for management of RabbitMQ connections
"""

__RCSID__ = "$Id$"

import json
import stomp
import threading
from DIRAC.Resources.MessageQueue.MQConnection import MQConnection
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

class StompMQConnection( MQConnection ):
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
    self.acknowledgement = False
    self.msgList = []
    self.lock = threading.Lock()
    self.connection = None
    self.valid = False

  # Callback functions for message receiving mode

  def on_message( self, headers, message ):
    result = self.callback( headers, message )
    if self.acknowledgement:
      if result['OK']:
        self.connection.ack( headers['message-id'], self.subscriptionID )
      else:
        self.connection.nack( headers['message-id'], self.subscriptionID )

  def defaultCallback( self, headers, message ):
    """
    Default callback function called every time something is read from the queue
    """
    dictionary = json.loads( message )
    with self.lock:
      self.msgList.append( dictionary )

  # Rest of functions

  def setupConnection( self, parameters = {}, receive = False, messageCallback = None ):
    """
    Establishes a new connection to RabbitMQ
    system indicates in which System the queue works
    queueName is the name of the queue to read from/write to
    parameters is a dictionary with the parameters for the queue. It should include the following parameters:
    'Host', 'Port', 'User', 'VH' and 'Type'. Otherwise, the function will return an error
    receive indicates whether this object will read from the queue or read from it
    messageCallback is the function to be called when a new message is received from the queue ( only receiver mode ).
    If None, the defaultCallback method is used instead
    """
    self.callback = messageCallback
    self.receiver = receive
    if self.receiver:
      if messageCallback:
        self.callback = messageCallback
      else:
        self.callback = self.defaultCallback

    if parameters:
      self.parameters.update( parameters )

    # Make the actual connection
    host = self.parameters.get( 'Host' )
    port = self.parameters.get( 'Port', 61613 )
    vhost = self.parameters.get( 'VHost' )
    user = self.parameters.get( 'User', 'guest' )
    password = self.parameters.get( 'Password', 'guest' )
    self.queueName = self.parameters.get( 'Queue' )
    headers = {}
    if "Persistent" in self.parameters and self.parameters['Persistent'].lower() in ['true', 'yes', '1']:
      headers = { 'persistent': 'true' }
    try:
      self.connection = stomp.Connection( [ ( host, int( port ) ) ], vhost = vhost )
      self.connection.start()
      self.connection.connect( username = user, passcode = password )

      if self.receiver:
        ack = 'auto'
        if self.parameters.get( 'Acknowledgement', '' ).lower() in ['true', 'yes', '1']:
          self.acknowledgement = True
          ack = 'client-individual'
        self.subscriptionID = self.queueName
        self.connection.set_listener( '', self )
        self.connection.subscribe( destination = '/queue/%s' % self.queueName,
                                   id = self.subscriptionID,
                                   ack = ack,
                                   headers = headers )
    except Exception as e:
      return S_ERROR( 'Failed to setup connection: %s' % e )

    self.valid = True
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

  def disconnect( self ):
    """
    Disconnects from the Stomp server
    """
    try:
      self.connection.disconnect()
    except Exception as e:
      return S_ERROR( 'Failed to disconnect: %s' % e )

    return S_OK( 'Disconnection successful' )
