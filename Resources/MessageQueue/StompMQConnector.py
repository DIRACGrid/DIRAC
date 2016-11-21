"""
Class for management of Stomp MQ connections, e.g. RabbitMQ
"""

import json
import socket
import stomp
import ssl
import time

from DIRAC.Core.Utilities.File                  import makeGuid
from DIRAC.Resources.MessageQueue.MQConnection  import MQConnector
from DIRAC.Core.Security                        import Locations
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.DErrno import EMQUKN, EMQCONN

__RCSID__ = "$Id$"

class StompMQConnection( MQConnector ):
  """
  Class for management of Stomp connections
  Allows to both send and receive messages from a queue
  """

  MANDATORY_PARAMETERS = [ 'Host', 'MQType' ]

  def __init__( self ):
    
    super( StompMQConnection, self ).__init__()
    self.destination = None

    self.subscriptionID = None
    self.connection = {}

  # Rest of functions
  def setupConnection( self, parameters = None, messageCallback = MQConnector.defaultCallback ):
    """
    Establishes a new connection to a Stomp server, e.g. RabbitMQ

    :param dict parameters: dictionary with additional MQ parameters if any
    :param func messageCallback: function to be called when a new message is received from the queue ( only receiver mode ).
                                If None, the defaultCallback method is used instead
    :return: S_OK/S_ERROR
    """

    if parameters is not None:
      self.parameters.update( parameters )

    # Make the actual connection
    host = self.parameters.get( 'Host' )
    port = self.parameters.get( 'Port', 61613 )
    vhost = self.parameters.get( 'VHost' )
    user = self.parameters.get( 'User' )
    password = self.parameters.get( 'Password' )

    queueName = self.parameters.get( 'Queue' )
    topicName = self.parameters.get( 'Topic' )

    sslVersion = self.parameters.get( 'SSLVersion' )
    hostcert = self.parameters.get( 'HostCertificate' )
    hostkey = self.parameters.get( 'HostKey' )
    
    # get local key and certificate if not available via configuration
    if sslVersion and not ( hostcert or hostkey ):
      paths = Locations.getHostCertificateAndKeyLocation()
      if not paths:
        return S_ERROR( 'Could not find a certificate!' )
      else:
        hostcert = paths[0]
        hostkey = paths[1]

    if queueName:
      self.destination = '/queue/%s' % queueName
    elif topicName:
      self.destination = '/topic/%s' % topicName

    headers = {}
    if self.parameters.get( 'Persistent', '' ).lower() in ['true', 'yes', '1']:
      headers = { 'persistent': 'true' }
    try:

      # get IP addresses of brokers
      brokers = socket.gethostbyname_ex( host )
      self.log.info( 'Broker name resolves to %s IP(s)' % len( brokers[2] ) )

      if sslVersion is None:
        pass
      elif sslVersion == 'TLSv1':
        sslVersion = ssl.PROTOCOL_TLSv1
      else:
        return S_ERROR( EMQCONN, 'Invalid SSL version provided: %s' % sslVersion )

      for ip in brokers[2]:
        if sslVersion:
          self.connection[ip] = stomp.Connection( 
                                                  [ ( ip, int( port ) ) ],
                                                  use_ssl = True,
                                                  ssl_version = sslVersion,
                                                  ssl_key_file = hostkey,
                                                  ssl_cert_file = hostcert,
                                                  username = user,
                                                  passcode = password,
                                                  vhost = vhost,
                                                )
        else:
          self.connection[ip] = stomp.Connection( 
                                                  [ ( ip, int( port ) ) ],
                                                  username = user,
                                                  passcode = password,
                                                  vhost = vhost
                                                )

        self.log.debug( "Connecting %s ..." % ip )
        self.connection[ip].start()
        self.connection[ip].connect()

        time.sleep( 1 )
        if self.connection[ip].is_connected():
          self.log.info( "Connected to %s:%s" % ( ip, port ) )

        if messageCallback:
          self.callback = messageCallback

          ack = 'auto'
          if self.parameters.get( 'Acknowledgement', '' ).lower() in ['true', 'yes', '1']:
            acknowledgement = True
            ack = 'client-individual'

          listener = StompListener( self.callback, acknowledgement, self.connection[ip] )

          self.connection[ip].set_listener( '', listener )

          self.subscriptionID = makeGuid()[:8]
          self.connection[ip].subscribe( destination = self.destination,
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
          self.connection.send( body = json.dumps( msg ), destination = self.destination )
      else:
        self.connection.send( body = json.dumps( message ), destination = self.destination )
    except Exception as e:
      return S_ERROR( EMQUKN, 'Failed to send message: %s' % e )

    return S_OK( 'Message sent successfully' )


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


class StompListener ( stomp.ConnectionListener ):
  """
  Internal listener class responsible for handling new messages and errors.
  """

  def __init__( self, callback, ack, connection ):
    """
    Initializes the internal listener object

    :param func callback: an MQConnect.defaultCallback compatible function
    :param bool ack: if set to true an acknowledgement will be send back to the sender
    :param connection: a stomp.Connection object used to send the acknowledgement
    """

    self.callback = callback
    self.ack = ack
    self.connection = connection

    self.log = gLogger.getSubLogger( 'StompListener' )

  def on_message( self, headers, body ):
    """
    Callback function called upon receiving a message

    :param dict headers: message headers
    :param json body: message body
    """

    result = self.callback( headers, json.loads( body ) )
    if self.ack:
      if result['OK']:
        self.connection.ack( headers['message-id'], self.subscriptionID )
      else:
        self.connection.nack( headers['message-id'], self.subscriptionID )

  def on_error( self, headers, message ):
    """ Callback function called when an error happens
    """

    self.log.error( message )
