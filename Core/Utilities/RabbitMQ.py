"""
Class for management of RabbitMQ connections
"""

__RCSID__ = "$Id$"

import json
import stomp
import threading
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

class RabbitConnection( object ):
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
    self.exchangeName = None
    self.password = None

    self.receiver = False
    self.msgList = []
    self.lock = threading.Lock()
    self.connection = None

  # Callback functions for message receiving mode

  def on_error( self, headers, message ):
    """
    Callback function called when an error happens
    """
    gLogger.error( message )

  def defaultCallback( self, headers, message ):
    """
    Default callback function called every time something is read from the queue
    """
    dictionary = json.loads( message )
    with self.lock:
      self.msgList.append( dictionary )

  # Rest of functions

  def __setQueueParameter( self, system, queueName, parameter ):
    """
    Reads a given MessageQueueing parameter from the CS and sets the appropriate variable in the class with its value
    system is the DIRAC system where the queue works
    queueName is the name of the queue
    parameter is the name of the parameter to be read and set
    """

    # Utility function to lowercase the first letter of a string ( to create valid variable names )
    toLowerFirst = lambda s: s[:1].lower() + s[1:] if s else ''

    setup = gConfig.getValue( '/DIRAC/Setup', '' )

    # Get the parameter from the CS and set it
    result = gConfig.getOption( '/Systems/%s/%s/MessageQueueing/%s/%s' % ( system, setup, queueName, parameter ) )
    if not result[ 'OK' ]:
      return S_ERROR( 'Failed to get the parameter \'%s\' for RabbitMQ: %s' % ( parameter, result[ 'Message' ] ) )
    setattr( self, toLowerFirst( parameter ), result[ 'Value' ] )

    return S_OK( 'Queue parameters set successfully' )

  def setupConnection( self, system, queueName, receive = False, messageCallback = None ):
    """
    Establishes a new connection to RabbitMQ
    system indicates in which System the queue works
    queueName is the name of the queue to read from/write to
    receive indicates whether this object will read from the queue or read from it
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
    for parameter in [ 'Host', 'Port', 'User', 'VH', 'ExchangeName' ]:
      result = self.__setQueueParameter( system, queueName, parameter )
      if not result[ 'OK' ]:
        return result

    result = gConfig.getOption( '/LocalInstallation/MessageQueueing/Password' )
    if not result[ 'OK' ]:
      return S_ERROR( 'Failed to get the password for RabbitMQ: %s' % result[ 'Message' ] )
    self.password = result[ 'Value' ]

    # Make the actual connection
    try:
      self.connection = stomp.Connection( [ ( self.host, int( self.port ) ) ], vhost = self.vH )
      self.connection.start()
      self.connection.connect( username = self.user, passcode = self.password )

      if self.receiver:
        self.connection.set_listener( '', self )
        self.connection.subscribe( destination = '/topic/%s' % self.exchangeName, id = queueName, headers = { 'persistent': 'true' } )
      else:
        self.connection.subscribe( destination = '/topic/%s' % self.exchangeName, id = queueName )
    except Exception, e:
      return S_ERROR( 'Failed to setup connection: %s' % e )

    return S_OK( 'Setup successful' )

  def send( self, message ):
    """
    Sends a message to the queue
    message contains the body of the message
    """
    try:
      if isinstance( message, list ):
        for msg in message:
          self.connection.send( body = json.dumps( msg ), destination = '/topic/%s' % self.exchangeName )
      else:
        self.connection.send( body = json.dumps( message ), destination = '/topic/%s' % self.exchangeName )
    except Exception, e:
      return S_ERROR( 'Failed to send message: %s' % e )

    return S_OK( 'Message sent successfully' )

  def receive( self ):
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
    except Exception, e:
      return S_ERROR( 'Failed to disconnect: %s' % e )

    return S_OK( 'Disconnection successful' )
