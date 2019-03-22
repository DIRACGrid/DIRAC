"""
Class for management of Stomp MQ connections, e.g. RabbitMQ
"""

import json
import os
import socket
import ssl
import time
import stomp

from DIRAC.Resources.MessageQueue.MQConnector import MQConnector
from DIRAC.Core.Security import Locations
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.DErrno import EMQUKN, EMQCONN

def callFunctionForBrokers(func, connections,  *args,**kwargs):
  for connection in connections:
    res = func(connection = connection,**kwargs)
    if res:
      return True
  return False 

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
    :param dict parameters: dictionary with additional MQ parameters if any
    :return: S_OK/S_ERROR
    """

    if parameters is not None:
      self.parameters.update(parameters)

    # Check that the minimum set of parameters is present
    if not all(p in parameters for p in ('Host', 'VHost')):
      return S_ERROR('Input parameters are missing!')

    host = self.parameters.get('Host')
    port = self.parameters.get('Port', StompMQConnector.PORT)

    result = self.generateConnectionArgs()
    if not result['OK']:
      #add returned error to propagate
      return S_ERROR('Error generating connection Args')
    connectionArgs = result['Value']
    self.log.debug("Connection args: %s" % str(connectionArgs))

    try:
      # Get IP addresses of brokers and ignoring two first returned arguments which are hostname and aliaslist.
      _, _, ip_addresses = socket.gethostbyname_ex(host)
      self.log.info('Broker name resolved', 'to %s IP(s)' % len(ip_addresses))

      for ip in ip_addresses:
        self.connections[ip] = stomp.Connection(host_and_ports =[(ip, int(port))], **connectionArgs)
        self.log.debug("Host and port: %s" % str([(ip, int(port))]))
        #WK to change!!! listener = StompListener(callback, acknowledgement, self.connections[ip], mId)
        #WK acknowledgment must be consumer-specific?
        listener = StompListener(self.connections[ip],self.reconnect)
        self.connections[ip].set_listener('StompListener', listener)

    except Exception as e:
      return S_ERROR(EMQCONN, 'Failed to setup connection: %s' % e)
    return S_OK('Setup successful')

  def generateConnectionArgs(self):

    reconnectSleepInitial = self.parameters.get('ReconnectSleepInitial', StompMQConnector.RECONNECT_SLEEP_INITIAL)
    reconnectSleepIncrease = self.parameters.get('ReconnectSleepIncrease', StompMQConnector.RECONNECT_SLEEP_INCREASE)
    reconnectSleepMax = self.parameters.get('ReconnectSleepMax', StompMQConnector.RECONNECT_SLEEP_MAX)
    reconnectSleepJitter = self.parameters.get('ReconnectSleepJitter', StompMQConnector.RECONNECT_SLEEP_JITTER)
    reconnectAttemptsMax = self.parameters.get('ReconnectAttemptsMax', StompMQConnector.RECONNECT_ATTEMPTS_MAX)

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
    return S_OK(connectionArgs)

  def reconnect(self, subscribedConsumers, tryReconnect = False):
    print 'in reconnect'
    if tryReconnect:
      #Think how to deal with the disconnecting on purpose
      #error handling
      self.connect()
      # currentConsumers = dict(subscribedConsumers)
      print 'currentConsumers:' 
      print subscribedConsumers

      # for cId,v in currentConsumers:
        # res = callFunctionForBrokers(self._subscribe,  connections = [v['connection']],  ack=v['ack'], mId= cId, dest=v['dest'], headers=v['headers'], callback=v['callback'] )
        # if not res:
          # return S_ERROR(EMQUKN, 'Failed to subscribe to at least one broker')
      return S_OK('Reconnection successful')
    return S_OK('Reconnection  not done')



  def callFunctionForAnyBroker(self, func, *args,**kwargs):
    ok = False
    for ip, connection in self.connections.iteritems():
      if func(connection = connection,ip=ip, **kwargs):
        ok = True
    return ok


  def put(self, message, parameters=None):
    """
    Sends a message to the queue
    message contains the body of the message

    :param str message: string or any json encodable structure
    """
    destination = parameters.get('destination', '')
    res = callFunctionForBrokers(self._put, connections = self.connections.itervalues(), message=message, destination = destination)
    if not res:
      return S_ERROR(EMQUKN, 'Failed to send message: %s' % message)
    return S_OK('Message sent successfully')


  def _put(self,connection, message, destination):
    try:
      if isinstance(message, (list, set, tuple)):
        for msg in message:
          connection.send(body=json.dumps(msg), destination=destination)
      else:
        connection.send(body=json.dumps(message), destination=destination)
    except Exception as e:
      return False
    return True

  def connect(self, parameters=None):
    host = self.parameters.get('Host')
    port = self.parameters.get('Port')
    user = self.parameters.get('User')
    password = self.parameters.get('Password')

    res = self.callFunctionForAnyBroker(self._connect, port=port, user=user, password=password)
    if not res:
      return S_ERROR(EMQCONN, "Failed to connect to  %s" % host)
    return S_OK("Connected to %s" % host)

  def _connect(self, connection, ip, port, user, password):
    try:
      connection.start()
      connection.connect(username=user, passcode = password, wait = True)
      time.sleep(1)
      if connection.is_connected():
        self.log.info("Connected to %s:%s" % (ip, port))
    except Exception as e:
      self.log.error('Failed to connect: %s' % e)
      return False
    return True
        
  def disconnect(self, parameters=None):
    """
    Disconnects from the message queue server
    """
    res = callFunctionForBrokers(self._disconnect,  connections = self.connections.itervalues())
    if not res:
      return S_ERROR(EMQUKN, 'Failed to disconnect from at least one broker')
    return S_OK('Successfully disconnected from all brokers')

  def _disconnect(self, connection):
    try:
      connection.disconnect()
      connection.get_listener('StompListener').removeAllConsumersInfo()
    except Exception as e:
      self.log.error('Failed to disconnect: %s' % e)
      return False
    return True

  def subscribe(self, parameters):
    mId = parameters.get('messengerId', '')
    dest = parameters.get('destination', '')
    headers = {}
    if self.parameters.get('Persistent', '').lower() in ['true', 'yes', '1']:
      headers = {'persistent': 'true'}
    ack = 'auto'
    acknowledgement = False
    if self.parameters.get('Acknowledgement', '').lower() in ['true', 'yes', '1']:
      acknowledgement = True
      ack = 'client-individual'

    callback = parameters.get('callback')
    res = callFunctionForBrokers(self._subscribe,  connections = self.connections.itervalues(),  ack=ack, mId=mId, dest=dest, headers=headers, callback=callback  )
    if not res:
      return S_ERROR(EMQUKN, 'Failed to subscribe to at least one broker')
    return S_OK('Subscription successful')

  def _subscribe(self, connection,  ack, mId, dest, headers, callback= None):
    try:
      connection.subscribe(destination=dest,
                           id=mId,
                           ack=ack,
                           headers=headers)
      #crazy hack for this moment
      connection.get_listener('StompListener').addConsumerInfo(mId, connection, ack, dest, headers, callback)
      # addConsumerId(mId,connection, ack, dest, headers,callback)

    except Exception as e:
      self.log.error('Failed to subscribe: %s' % e)
      return False
    return True


  def unsubscribe(self, parameters):
    dest = parameters.get('destination', '')
    mId = parameters.get('messengerId', '')
    print 'in unsubscribe'
    print 'dest %s'% str(dest)
    print 'mId %s'% str(mId)
    res = callFunctionForBrokers(self._unsubscribe, connections = self.connections.itervalues(),  destination=dest, mId=mId)
    if not res:
      return S_ERROR(EMQUKN, 'Failed to unsubscribe from at least one destination')
    return S_OK('Successfully unsubscribed from all destinations')


  def _unsubscribe(self, connection, destination, mId):
    try:
      connection.unsubscribe(destination=destination, id=mId)
      connection.get_listener('StompListener').removeConsumerInfo(consumerId = mId)
    except Exception as e:
      print "what exception?"
      print e
      self.log.error('Failed to unsubscribe: %s' % e)
      return False
    return True


class StompListener (stomp.ConnectionListener):
  """
  Internal listener class responsible for handling new messages and errors.
  """

  def __init__(self, connection, callbackOnDisconnected=None):
    """
    Initializes the internal listener object

    :param func callback: a defaultCallback compatible function
    :param bool ack: if set to true an acknowledgement will be send back to the sender
    :param str messengerId: messenger identifier sent with acknowledgement messages.

    """

    self.log = gLogger.getSubLogger('StompListener')
    # self.ack = ack
    # self.mId = messengerId
    self.connection = connection
    self.callbackOnDisconnected = callbackOnDisconnected
    self.consumersInfo = {}

  def addConsumerInfo(self, mId, connection, ack, dest, headers, callback):
    if mId not in self.consumersInfo:
      self.consumersInfo[mId]={"connection":connection,"ack":ack, "dest":dest, "headers": headers, "callback":callback}

  def removeConsumerInfo(self,consumerId):
    del self.consumersInfo[consumerId]

  def removeAllConsumersInfo(self):
    self.consumersInfo.clear()

  def on_message(self, headers, body):
    """
    Function called upon receiving a message
    :param dict headers: message headers
    :param json body: message body
    """
    if 'subscription' in headers:
      consumerId = headers['subscription']
      if consumerId in self.consumersInfo:
        callback = self.consumersInfo[consumerId]['callback']
        if callback is not None:
          result = callback(headers, json.loads(body))
        print "ack value:"
        print self.consumersInfo[consumerId]['ack']
        if self.consumersInfo[consumerId]['ack'] == 'client-individual':
          if result['OK']:
            self.connection.ack(headers['message-id'], consumerId)
          else:
            self.connection.nack(headers['message-id'], consumerId)

  def on_error(self, headers, message):
    """ Function called when an error happens
    """
    self.log.error(message)

  def on_disconnected(self):
    """ Callback function called after disconnecting from broker.
    """
    #maybe it should be atomic?
    #also what if we want to disconnect for real?
    self.log.warn('Disconnected from broker')
    if self.callbackOnDisconnected:
      self.callbackOnDisconnected(subscribedConsumers = self.consumersInfo)
