"""
Class for management of Stomp MQ connections, e.g. RabbitMQ
"""

import json
import socket
import stomp
import ssl
import time

from DIRAC.Resources.MessageQueue.MQConnector  import MQConnector
from DIRAC.Core.Security import Locations
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.DErrno import EMQUKN, EMQCONN



class StompMQConnector( MQConnector ):
  """
  Class for management of message queue connections
  Allows to both send and receive messages from a queue
  """

  def __init__( self, parameters = {} ):
    """ Standard constructor
    """
    super( StompMQConnector, self ).__init__()
    self.log = gLogger.getSubLogger( self.__class__.__name__ )
    self.parameters = parameters
    self.connection =  None

  def setupConnection( self, parameters = None ):
    #"""
    #Establishes a new connection to a Stomp server, e.g. RabbitMQ
    #:param dict parameters: dictionary with additional MQ parameters if any
    #:return: S_OK/S_ERROR
    #"""

    if parameters is not None:
      self.parameters.update( parameters )

    #Check that the minimum set of parameters is present
    if not all( p in parameters for p in ( 'Host', 'VHost' )):
      return S_ERROR( 'Input parameters are missing!' )

    # Make the actual connection
    host = self.parameters.get( 'Host' )
    port = self.parameters.get( 'Port', 61613 )
    vhost = self.parameters.get( 'VHost' )

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

      #Im taking the first ip on the list
      ip = next( iter( brokers[2] or [] ), None )
      self.parameters.update( {'IP':ip} )

      if ip:
        if sslVersion:
          self.connection = stomp.Connection(
                                              [ ( ip, int( port ) ) ],
                                              use_ssl = True,
                                              ssl_version = sslVersion,
                                              ssl_key_file = hostkey,
                                              ssl_cert_file = hostcert,
                                              vhost = vhost,
                                             )
        else:
          self.connection = stomp.Connection(
                                              [ ( ip, int( port ) ) ],
                                              vhost = vhost
                                             )

    except Exception as e:
      return S_ERROR( EMQCONN, 'Failed to setup connection: %s' % e )
    return S_OK( 'Setup successful' )

  def put( self, message, parameters = None ):
    """
    Sends a message to the queue
    message contains the body of the message

    :param str message: string or any json encodable structure
    """
    destination = parameters.get( 'destination', '' )
    try:
      if isinstance( message, ( list, set, tuple ) ):
        for msg in message:
          self.connection.send( body = json.dumps( msg ), destination = destination )
      else:
        self.connection.send( body = json.dumps( message ), destination = destination )
    except Exception as e:
      return S_ERROR( EMQUKN, 'Failed to send message: %s' % e )
    return S_OK( 'Message sent successfully' )

  def connect( self, parameters = None ):
    port = self.parameters.get( 'Port', 61613 )
    ip = self.parameters.get( 'IP', '' )
    user = self.parameters.get( 'User' )
    password = self.parameters.get( 'Password' )
    try:
      self.connection.start()
      self.connection.connect( username = user, passcode = password )
      time.sleep( 1 )
      if self.connection.is_connected():
        self.log.info( "Connected to %s:%s" % ( ip, port ) )
        return S_OK( "Connected to %s:%s" % ( ip, port ) )
      else:
        return S_ERROR( EMQCONN, "Failed to connect to  %s:%s" % ( ip, port ) )
    except Exception as e:
      return S_ERROR( EMQCONN, 'Failed to connect: %s' % e )

  def disconnect( self, parameters = None ):
    """
    Disconnects from the message queue server
    """
    try:
      self.connection.disconnect()
    except Exception as e:
      return S_ERROR( EMQUKN, 'Failed to disconnect: %s' % str( e ) )
    return S_OK( 'Disconnection successful' )

  def subscribe( self, parameters = None ):
    mId = parameters.get( 'messengerId', '' )
    callback = parameters.get( 'callback', None )
    dest = parameters.get( 'destination', '' )
    headers = {}
    if self.parameters.get( 'Persistent', '' ).lower() in ['true', 'yes', '1']:
      headers = { 'persistent': 'true' }
    ack = 'auto'
    acknowledgement = False
    if self.parameters.get( 'Acknowledgement', '' ).lower() in ['true', 'yes', '1']:
      acknowledgement = True
      ack = 'client-individual'
    if not callback:
      self.log.error( "No callback specified!" )
    listener = StompListener( callback, acknowledgement, self.connection, mId )

    self.connection.set_listener( '', listener )
    self.connection.subscribe( destination = dest,
                               id = mId,
                               ack = ack,
                               headers = headers )
    return S_OK( 'Subscription successful' )

  def unsubscribe( self, parameters ):
    dest = parameters.get( 'destination', '' )
    mId = parameters.get( 'messengerId', '' )
    try:
      self.connection.unsubscribe( destination = dest, id = mId )
    except Exception as e:
      return S_ERROR( EMQUKN, 'Failed to unsubscibe: %s' % str( e ) )
    return S_OK( 'Unsubscription successful' )

class StompListener ( stomp.ConnectionListener ):
  """
  Internal listener class responsible for handling new messages and errors.
  """

  def __init__( self, callback, ack, connection, messengerId ):
    """
    Initializes the internal listener object

    :param func callback: a defaultCallback compatible function
    :param bool ack: if set to true an acknowledgement will be send back to the sender
    :param connection: a stomp.Connection object used to send the acknowledgement
    """

    self.log = gLogger.getSubLogger( 'StompListener' )
    if not callback:
      self.log.error( 'Error initializing StompMQConnector!callback is None' )
    self.callback = callback
    self.ack = ack
    self.mId = messengerId
    self.connection = connection


  def on_message( self, headers, body ):
    """
    Function called upon receiving a message
    :param dict headers: message headers
    :param json body: message body
    """
    result = self.callback( headers, json.loads( body ) )
    if self.ack:
      if result['OK']:
        self.connection.ack( headers['message-id'], self.mId )
      else:
        self.connection.nack( headers['message-id'], self.mId )

  def on_error( self, headers, message ):
    """ Function called when an error happens
    """
    self.log.error( message )
