"""
Class for management of Stomp MQ connections, e.g. RabbitMQ
"""

__RCSID__ = "$Id$"

import json
import stomp
import threading

from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.Resources.MessageQueue.MQConnection import MQConnection
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.DErrno import EMQNOM, EMQUKN, EMQCONN

class StompMQConnection( MQConnection ):
  """
  Class for management of Stomp connections
  Allows to both send and receive messages from a queue
  The class also implements callback functions to be able to act as a listener and receive messages
  """

  MANDATORY_PARAMETERS = [ 'Host', 'MQType' ]

  def __init__( self ):

    super( StompMQConnection, self ).__init__()
    self.queueName = None

    self.callback = None
    self.receiver = False
    self.acknowledgement = False
    self.subscriptionID = None
    self.msgList = []
    self.lock = threading.Lock()
    self.connection = None

  # Callback functions for message receiving mode

  def on_message( self, headers, message ):
    """
    Callback function called upon receiving a message

    :param dict headers: headers of the MQ message
    :param json message: json string of the message
    """
    result = self.callback( headers, message )
    if self.acknowledgement:
      if result['OK']:
        self.connection.ack( headers['message-id'], self.subscriptionID )
      else:
        self.connection.nack( headers['message-id'], self.subscriptionID )

  def defaultCallback( self, headers, message ):
    """
    Default callback function called every time something is read from the queue

    :param dict headers: headers of the MQ message
    :param json message: json string of the message
    """
    dictionary = json.loads( message )
    with self.lock:
      self.msgList.append( dictionary )
    return S_OK()

  # Rest of functions

  def setupConnection( self, parameters = None, receive = False, messageCallback = None ):
    """
    Establishes a new connection to a Stomp server, e.g. RabbitMQ

    :param dict parameters: dictionary with additional MQ parameters if any
    :param bool receive: flag to enable the MQ connection for getting message
    :param func messageCallback: function to be called when a new message is received from the queue ( only receiver mode ).
                                If None, the defaultCallback method is used instead
    :return: S_OK/S_ERROR
    """

    self.receiver = receive
    if self.receiver:
      if messageCallback:
        self.callback = messageCallback
      elif not self.callback:
        self.callback = self.defaultCallback

    if parameters is not None:
      self.parameters.update( parameters )

    # Make the actual connection
    host = self.parameters.get( 'Host' )
    port = self.parameters.get( 'Port', 61613 )
    vhost = self.parameters.get( 'VHost' )
    user = self.parameters.get( 'User', 'guest' )
    password = self.parameters.get( 'Password', 'guest' )
    self.queueName = self.parameters.get( 'Queue' )
    headers = {}
    if self.parameters.get( 'Persistent', '' ).lower() in ['true', 'yes', '1']:
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
        self.subscriptionID = makeGuid()[:8]
        self.connection.set_listener( '', self )
        self.connection.subscribe( destination = '/queue/%s' % self.queueName,
                                   id = self.subscriptionID,
                                   ack = ack,
                                   headers = headers )
    except Exception as e:
      return S_ERROR( EMQCONN, 'Failed to setup connection: %s' % e )

    self.alive = True
    return S_OK( 'Setup successful' )

  def put( self, message ):
    """
    Sends a message to the queue
    message contains the body of the message

    :param str message: string or any json encodable structure
    """
    try:
      if isinstance( message, ( list, set, tuple ) ):
        for msg in message:
          self.connection.send( body = json.dumps( msg ), destination = '/queue/%s' % self.queueName )
      else:
        self.connection.send( body = json.dumps( message ), destination = '/queue/%s' % self.queueName )
    except Exception as e:
      return S_ERROR( EMQUKN, 'Failed to send message: %s' % e )

    return S_OK( 'Message sent successfully' )

  def get( self ):
    """
    Retrieves a message from the queue ( if any ). This method is only valid
    if the default behaviour for the message callback is being used

    :return: S_OK( message )/S_ERROR if there are no messages in the queue

    """
    if not self.receiver:
      return S_ERROR( EMQUKN, 'StompMQConnection is not configured to receive messages' )

    with self.lock:
      if self.msgList:
        msg = self.msgList.pop( 0 )
      else:
        return S_ERROR( EMQNOM, 'No messages in queue' )

    return S_OK( msg )

  def disconnect( self ):
    """
    Disconnects from the Stomp server

    :return: S_OK/S_ERROR
    """
    self.alive = False
    try:
      if self.subscriptionID:
        self.connection.unsubscribe( self.subscriptionID )
      self.connection.disconnect()
    except Exception as e:
      return S_ERROR( EMQUKN, 'Failed to disconnect: %s' % str( e ) )

    return S_OK( 'Disconnection successful' )
