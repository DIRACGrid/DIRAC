"""
Class for management of Stomp MQ connections, e.g. RabbitMQ
"""

import json
import random
import os
import socket
import ssl
import time
import stomp

from DIRAC.Resources.MessageQueue.MQConnector import MQConnector
from DIRAC.Core.Security import Locations
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.DErrno import EMQUKN, EMQCONN


class StompMQConnector(MQConnector):
  """
  Class for management of message queue connections
  Allows to both send and receive messages from a queue
  """
  # Setting for the reconnection handling by stomp interface.
  # See e.g. the description of Transport class in
  # https://github.com/jasonrbriggs/stomp.py/blob/master/stomp/transport.py
  RECONNECT_SLEEP_INITIAL = 1  # [s]  Initial delay before reattempting to establish a connection.
  RECONNECT_SLEEP_INCREASE = 0.5  # Factor by which sleep delay is increased 0.5 means increase by 50%.
  RECONNECT_SLEEP_MAX = 120  # [s] The maximum delay that can be reached independent of increasing procedure.
  RECONNECT_SLEEP_JITTER = 0.1  # Random factor to add. 0.1 means a random number from 0 to 10% of the current time.
  RECONNECT_ATTEMPTS_MAX = 1e4  # Maximum attempts to reconnect.

  PORT = 61613

  def __init__(self, parameters=None):
    """ Standard constructor
    """
    super(StompMQConnector, self).__init__()
    self.log = gLogger.getSubLogger(self.__class__.__name__)
    self.connections = {}

    if 'DIRAC_DEBUG_STOMP' in os.environ:
      gLogger.enableLogsFromExternalLibs()

  def setupConnection(self, parameters=None):
    """
     Establishes a new connection to a Stomp server, e.g. RabbitMQ

    Args:
      parameters(dict): dictionary with additional MQ parameters if any.
    Returns:
      S_OK/S_ERROR
    """

    if parameters is not None:
      self.parameters.update(parameters)

    # Check that the minimum set of parameters is present
    if not all(p in parameters for p in ('Host', 'VHost')):
      return S_ERROR('Input parameters are missing!')

    reconnectSleepInitial = self.parameters.get('ReconnectSleepInitial', StompMQConnector.RECONNECT_SLEEP_INITIAL)
    reconnectSleepIncrease = self.parameters.get('ReconnectSleepIncrease', StompMQConnector.RECONNECT_SLEEP_INCREASE)
    reconnectSleepMax = self.parameters.get('ReconnectSleepMax', StompMQConnector.RECONNECT_SLEEP_MAX)
    reconnectSleepJitter = self.parameters.get('ReconnectSleepJitter', StompMQConnector.RECONNECT_SLEEP_JITTER)
    reconnectAttemptsMax = self.parameters.get('ReconnectAttemptsMax', StompMQConnector.RECONNECT_ATTEMPTS_MAX)

    host = self.parameters.get('Host')
    port = self.parameters.get('Port', StompMQConnector.PORT)
    vhost = self.parameters.get('VHost')

    sslVersion = self.parameters.get('SSLVersion')
    hostcert = self.parameters.get('HostCertificate')
    hostkey = self.parameters.get('HostKey')

    connectionArgs = {'vhost': vhost,
                      'keepalive': True,
                      'reconnect_sleep_initial': reconnectSleepInitial,
                      'reconnect_sleep_increase': reconnectSleepIncrease,
                      'reconnect_sleep_max': reconnectSleepMax,
                      'reconnect_sleep_jitter': reconnectSleepJitter,
                      'reconnect_attempts_max': reconnectAttemptsMax}

    # We use ssl credentials and not user-password.
    if sslVersion is not None:
      if sslVersion == 'TLSv1':
        sslVersion = ssl.PROTOCOL_TLSv1
        # get local key and certificate if not available via configuration
        if not (hostcert or hostkey):
          paths = Locations.getHostCertificateAndKeyLocation()
          if not paths:
            return S_ERROR('Could not find a certificate!')
          hostcert = paths[0]
          hostkey = paths[1]
        connectionArgs.update({
                              'use_ssl': True,
                              'ssl_version': sslVersion,
                              'ssl_key_file': hostkey,
                              'ssl_cert_file': hostcert})
      else:
        return S_ERROR(EMQCONN, 'Invalid SSL version provided: %s' % sslVersion)

    try:
      # Get IP addresses of brokers and ignoring two first returned arguments which are hostname and aliaslist.
      _, _, ip_addresses = socket.gethostbyname_ex(host)
      self.log.info('Broker name resolved', 'to %s IP(s)' % len(ip_addresses))

      for ip in ip_addresses:
        connectionArgs.update({'host_and_ports': [(ip, int(port))]})
        self.log.debug("Connection args: %s" % str(connectionArgs))
        self.connections[ip] = stomp.Connection(**connectionArgs)

    except Exception as e:
      return S_ERROR(EMQCONN, 'Failed to setup connection: %s' % e)
    return S_OK('Setup successful')

  def reconnect(self):
    res = self.connect(self.parameters)
    if not res:
      return S_ERROR(EMQCONN, "Failed to reconnect")
    return S_OK('Reconnection successful')

  def put(self, message, parameters=None):
    """
    Sends a message to the queue
    message contains the body of the message

    Args:
      message(str): string or any json encodable structure.
      parameters(dict): parameters with 'destination' key defined.
    """
    destination = parameters.get('destination', '')
    error = False

    # Randomize the brokers to spread the load
    randConn = self.connections.values()
    random.shuffle(randConn)

    for connection in randConn:
      try:
        if isinstance(message, (list, set, tuple)):
          for msg in message:
            connection.send(body=json.dumps(msg), destination=destination)
            error = False
            break
        else:
          connection.send(body=json.dumps(message), destination=destination)
          error = False
          break
      except Exception as e:
        error = e

    if error is not False:
      return S_ERROR(EMQUKN, 'Failed to send message: %s' % error)

    return S_OK('Message sent successfully')

  def connect(self, parameters=None):
    host = self.parameters.get('Host')
    port = self.parameters.get('Port')
    user = self.parameters.get('User')
    password = self.parameters.get('Password')

    connected = False
    for ip, connection in self.connections.iteritems():
      try:
        listener = connection.get_listener('ReconnectListener')
        if listener is None:
          listener = ReconnectListener(self.reconnect)
          connection.set_listener('ReconnectListener', listener)
        connection.start()
        connection.connect(username=user, passcode=password)
        time.sleep(1)
        if connection.is_connected():
          self.log.info("Connected to %s:%s" % (ip, port))
          connected = True
      except Exception as e:
        self.log.error('Failed to connect: %s' % e)

    if not connected:
      return S_ERROR(EMQCONN, "Failed to connect to  %s" % host)
    return S_OK("Connected to %s" % host)

  def disconnect(self, parameters=None):
    """
    Disconnects from the message queue server
    """
    fail = False
    for connection in self.connections.itervalues():
      try:
        if connection.get_listener('ReconnectListener'):
          connection.remove_listener('ReconnectListener')
        connection.disconnect()
      except Exception as e:
        self.log.error('Failed to disconnect: %s' % e)
        fail = True

    if fail:
      return S_ERROR(EMQUKN, 'Failed to disconnect from at least one broker')
    return S_OK('Successfully disconnected from all brokers')

  def subscribe(self, parameters=None):
    mId = parameters.get('messengerId', '')
    callback = parameters.get('callback', None)
    dest = parameters.get('destination', '')
    headers = {}
    if self.parameters.get('Persistent', '').lower() in ['true', 'yes', '1']:
      headers = {'persistent': 'true'}
    ack = 'auto'
    acknowledgement = False
    if self.parameters.get('Acknowledgement', '').lower() in ['true', 'yes', '1']:
      acknowledgement = True
      ack = 'client-individual'
    if not callback:
      self.log.error("No callback specified!")

    fail = False
    for connection in self.connections.itervalues():
      try:
        listener = StompListener(callback, acknowledgement, connection, mId)
        connection.set_listener('StompListener', listener)
        connection.subscribe(destination=dest,
                             id=mId,
                             ack=ack,
                             headers=headers)
      except Exception as e:
        self.log.error('Failed to subscribe: %s' % e)
        fail = True
    if fail:
      return S_ERROR(EMQUKN, 'Failed to subscribe to at least one broker')
    return S_OK('Subscription successful')

  def unsubscribe(self, parameters):
    dest = parameters.get('destination', '')
    mId = parameters.get('messengerId', '')
    fail = False
    for connection in self.connections.itervalues():
      try:
        connection.unsubscribe(destination=dest, id=mId)
      except Exception as e:
        self.log.error('Failed to unsubscribe: %s' % e)
        fail = True

    if fail:
      return S_ERROR(EMQUKN, 'Failed to unsubscribe from at least one destination')
    return S_OK('Successfully unsubscribed from all destinations')


class ReconnectListener (stomp.ConnectionListener):
  """
  Internal listener class responsible for reconnecting in case of disconnection.
  """

  def __init__(self, callback=None):
    """
    Initializes the internal listener object

    Args:
      callback: a function called when disconnection happens.
    """

    self.log = gLogger.getSubLogger('ReconnectListener')
    self.callback = callback

  def on_disconnected(self):
    """ Callback function called after disconnecting from broker.
    """
    self.log.warn('Disconnected from broker')
    try:
      if self.callback:
        self.callback()
    except Exception as e:
      self.log.error("Unexpected error while calling reconnect callback: %s" % e)


class StompListener (stomp.ConnectionListener):
  """
  Internal listener class responsible for handling new messages and errors.
  """

  def __init__(self, callback, ack, connection, messengerId):
    """
    Initializes the internal listener object

    Args:
      callback: a defaultCallback compatible function.
      ack(bool): if set to true an acknowledgement will be send back to the sender.
      messengerId(str): messenger identifier sent with acknowledgement messages.
    """

    self.log = gLogger.getSubLogger('StompListener')
    if not callback:
      self.log.error('Error initializing StompMQConnector!callback is None')
    self.callback = callback
    self.ack = ack
    self.mId = messengerId
    self.connection = connection

  def on_message(self, headers, body):
    """
    Function called upon receiving a message

    :param dict headers: message headers
    :param json body: message body
    """
    result = self.callback(headers, json.loads(body))
    if self.ack:
      if result['OK']:
        self.connection.ack(headers['message-id'], self.mId)
      else:
        self.connection.nack(headers['message-id'], self.mId)

  def on_error(self, headers, message):
    """ Function called when an error happens

    Args:
      headers(dict): message headers.
      body(json): message body.
    """
    self.log.error(message)
