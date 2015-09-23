"""
Class for management of RabbitMQ connections
"""

__RCSID__ = "$Id$"

import datetime, threading, json, stomp
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

class RabbitInterface( object ):
  """
  Class for management of RabbitMQ connections
  Allows to both send and receive messages from a queue
  The class also implements callback functions to be able to act as a listener and receive messages
  """

  msgList = []
  lock = threading.Lock()

  def on_error( self, headers, message ):
    """
    Callback function called when an error happens
    """
    gLogger.error( message )

  def on_message( self, headers, message ):
    """
    Callback function called every time something is read from the queue
    """
    dictionary = json.loads( message )
    with self.lock:
      self.msgList.append( dictionary )

  def setupConnection( self, system, queueName, receive = False ):
    """
    Establishes a new connection to RabbitMQ
    system indicates in which System the queue works
    queueName is the name of the queue to read from/write to
    receive indicates whether this object will read from the queue or read from it
    """
    self.receiver = receive

    self.queueName = queueName
    setup = gConfig.getValue( '/DIRAC/Setup', '' )

    result = gConfig.getOption( '/Systems/%s/%s/MessageQueueing/%s/Host' % ( system, setup, queueName ) )
    if not result[ 'OK' ]:
      return S_ERROR( 'Failed to get the host for RabbitMQ: %s' % result[ 'Message' ] )
    host = result[ 'Value' ]

    result = gConfig.getOption( '/Systems/%s/%s/MessageQueueing/%s/Port' % ( system, setup, queueName ) )
    if not result[ 'OK' ]:
      return S_ERROR( 'Failed to get the port for RabbitMQ: %s' % result[ 'Message' ] )
    port = result[ 'Value' ]

    result = gConfig.getOption( '/Systems/%s/%s/MessageQueueing/%s/User' % ( system, setup, queueName ) )
    if not result[ 'OK' ]:
      return S_ERROR( 'Failed to get the user for RabbitMQ: %s' % result[ 'Message' ] )
    user = result[ 'Value' ]

    result = gConfig.getOption( '/Systems/%s/%s/MessageQueueing/%s/VH' % ( system, setup, queueName ) )
    if not result[ 'OK' ]:
      return S_ERROR( 'Failed to get the virtual host for RabbitMQ: %s' % result[ 'Message' ] )
    vHost = result[ 'Value' ]

    result = gConfig.getOption( '/Systems/%s/%s/MessageQueueing/%s/ExchangeName' % ( system, setup, queueName ) )
    if not result[ 'OK' ]:
      return S_ERROR( 'Failed to get the exchange for RabbitMQ: %s' % result[ 'Message' ] )
    self.exchange = result[ 'Value' ]

    result = gConfig.getOption( '/LocalInstallation/MessageQueueing/Password' )
    if not result[ 'OK' ]:
      return S_ERROR( 'Failed to get the password for RabbitMQ: %s' % result[ 'Message' ] )
    password = result[ 'Value' ]

    try:
      self.connection = stomp.Connection( [ ( host, int( port ) ) ], user, password, vhost = vHost )
      self.connection.start()
      self.connection.connect( username = user, passcode = password )

      if self.receiver:
        self.connection.set_listener( '', self )
        self.connection.subscribe( destination = '/topic/%s' % self.exchange, id = self.queueName, headers = { 'persistent': 'true' } )
      else:
        self.connection.subscribe( destination = '/topic/%s' % self.exchange, id = self.queueName )
    except Exception, e:
      return S_ERROR( 'Failed to setup connection: %s' % e )

    return S_OK( 'Setup successful' )

  def send( self, message ):
    """
    Sends a message to the queue
    message contains the body of the message
    """
    try:
      if type( message ) == list:
        for msg in message:
          self.connection.send( body = json.dumps( msg ), destination = '/topic/%s' % self.exchange )
      else:
        self.connection.send( body = json.dumps( message ), destination = '/topic/%s' % self.exchange )
    except Exception, e:
      return S_ERROR( 'Failed to send message: %s' % e )

    return S_OK( 'Message sent successfully' )

  def receive( self ):
    """
    Retrieves a message from the queue ( if any )
    Returns S_ERROR if there are no messages in the queue
    """
    if not self.receiver:
      return S_ERROR( 'Instance not configured to receive messages' )

    with self.lock:
      if len( self.msgList ) > 0:
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
      return S_ERROR( 'Failed to send message: %s' % e )

    return S_OK( 'Disconnection successful' )
