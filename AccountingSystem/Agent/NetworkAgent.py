__RCSID__ = "$Id: $"
import time
import ssl
import stomp
import socket
import json

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                  import AgentModule
from DIRAC.Core.Security                          import Locations
from DIRAC.AccountingSystem.Client.Types.Network  import Network
from DIRAC.ConfigurationSystem.Client.Config      import gConfig
from DIRAC.ConfigurationSystem.Client.CSAPI       import CSAPI

from messaging.stomppy import MessageListener
from datetime import datetime

# required by stomp.py
import logging
logging.basicConfig()


class NetworkAgent ( AgentModule ):
  """
  Class to retrieve messages containing perfSONAR data.
  """

  # supported messages
  MESSAGE_TYPES = [
                    'packet-loss-rate',
                    # 'packet-count-sent',
                    # 'packet-count-lost',
                    # 'packet-trace',
                    # 'throughput',
                    'histogram-owdelay'
                  ]

  HostToDiracNameDict = {}  # one dictionary for all message listeners

  def initialize( self ):

    # API initialization is required to get the up-to-date configuration from CS
    self.csAPI = CSAPI()
    self.csAPI.initialize()

    # get paths to the key and certificate
    paths = Locations.getHostCertificateAndKeyLocation()
    if not paths:
      self.log.error( "getHostCertificateAndKeyLocation() returned 'False'" )
      return S_ERROR( 'Could not find a certificate!' )
    else:
      self.hostCertPath = paths[0]
      self.hostKeyPath = paths[1]

    self.brokerName = self.am_getOption( 'MQBrokerName', None )  # FQDN of the broker
    self.brokerPort = int( self.am_getOption( 'MQBrokerPort', None ) )  # port at which broker is listening

    self.brokerConnections = {}  # holds connections that were established

    return S_OK()

  def execute( self ):
    '''
    During each cycle update the internal site name dictionary,
    check connections and show statistics.
    '''

    self.updateNameDictionary()
    self.checkConnections()

    for ip, connection in self.brokerConnections.iteritems():
      listener = connection.get_listener( ip )
      self.log.info( "Broker IP: %s" % ip )
      self.log.info( "\tReceived messages:           %s" % listener.messagesCount )
      self.log.info( "\tPacket-loss-rate datapoints: %s" % listener.PLREventsCount )
      self.log.info( "\tOne-way-delay datapoints:    %s" % listener.OWDEventsCount )

    return S_OK()

  def checkConnections ( self ):
    '''
    Check if all required connections are OK (try to reconnect if needed).
    '''

    if self.brokerName is not None and self.brokerPort is not None:

      # get IP addresses of the brokers and connect to them
      brokers = socket.gethostbyname_ex( self.brokerName )
      self.log.info( 'Broker name resolves to %s IP(s)' % len( brokers[2] ) )

      for ip in brokers[2]:
        try:
          connection = self.brokerConnections[ip]
        except KeyError:
          connection = None

        # (re)connect to the brokers
        if connection is None or not connection.is_connected():
          new_connection = self.connectToBroker( ip )
          if new_connection is not None:
            self.brokerConnections[ip] = new_connection
          elif connection is None:
            del self.brokerConnections[ip]

    else:
      self.log.warn( 'Broker name and port are not set in the configuration!' )


  def connectToBroker( self, ip ):
    '''
    Connect to a message broker at given IP and subscribe required topics.
    '''

    connection = stomp.Connection( 
                                  [( ip, self.brokerPort )],
                                  use_ssl = True,
                                  ssl_version = ssl.PROTOCOL_TLSv1,
                                  ssl_key_file = self.hostKeyPath,
                                  ssl_cert_file = self.hostCertPath,
                                  reconnect_sleep_initial = 10,
                                  reconnect_attempts_max = 1
                                 )

    self.log.debug( "Setting up the message listener (%s)." % ip )
    connection.set_listener( ip, self.NetworkMessagesListener( self.log ) )

    self.log.debug( "Starting the connection (%s)." % ip )
    connection.start()

    self.log.debug( "Connecting to the broker (%s)." % ip )
    connection.connect()
    time.sleep( 1 )

    if connection.is_connected():
      self.log.info( "Connected to %s:%s" % ( ip, self.brokerPort ) )
      self.log.debug( "Subscribing topics" )
      for eventType in self.MESSAGE_TYPES:
        connection.subscribe( destination = '/topic/perfsonar.' + eventType, ack = 'auto' )

      return connection

    else:
      self.log.error( "Could not connect to %s:%s" % ( ip, self.brokerPort ) )
      return None

  def updateNameDictionary( self ):
    '''
    Update the internal host-to-dirac name dictionary.
    '''

    result = gConfig.getConfigurationTree( '/Resources/Sites', 'Network/', '/Enabled' )
    if not result['OK']:
      self.log.error( "getConfigurationTree() failed with message: %s" % result['Message'] )
      return S_ERROR( 'Unable to fetch perfSONAR endpoints from CS.' )

    tmpDict = {}
    for path, value in result['Value'].iteritems():
      if value == 'True':
        elements = path.split( '/' )
        diracName = elements[4]
        hostName = elements[6]
        tmpDict[hostName] = diracName

    NetworkAgent.HostToDiracNameDict = tmpDict


  class NetworkMessagesListener( MessageListener ):
    '''
    Internal message listener class that handle messages with perfSONAR data.
    '''

    def __init__( self, log ):
      self.net = Network()

      # counters
      self.messagesCount = 0
      self.PLREventsCount = 0  # packet-loss-rate events
      self.OWDEventsCount = 0  # one-way-delay events

      self.log = log.getSubLogger( 'NetworkMessagesListener' )

    def error( self, message ):
      '''
      Log message errors if they appear.
      '''

      self.log.warn( "Message error: %s" % message )

    def message( self, message ):
      self.messagesCount += 1

      # prepare DB entity
      body = json.loads( message.get_body() )

      meta_data = {
              'SourceIP': body['meta']['source'],
              'SourceHostName': body['meta']['input_source'],
              'DestinationIP': body['meta']['destination'],
              'DestinationHostName': body['meta']['input_destination'],
             }

      # skip data from unknown source or destination
      try:
        meta_data['Source'] = NetworkAgent.HostToDiracNameDict[body['meta']['input_source']]
      except KeyError:
        self.log.debug( "Unknown source: %s (message skipped)" % body['meta']['input_source'] )
        return

      try:
        meta_data['Destination'] = NetworkAgent.HostToDiracNameDict[body['meta']['input_destination']]
      except KeyError:
        self.log.debug( "Unknown destination: %s (message skipped)" % body['meta']['input_destination'] )
        return

      self.net.setValuesFromDict( meta_data )

      if body.has_key( 'summaries' ):
        for summary in body['summaries']:

          # we are interested in 5 minutes summaries only
          if summary['summary_window'] == '300':
            for data in summary['summary_data']:

                # look for supported event types
                if summary['event_type'] == 'packet-loss-rate':
                  self.PLREventsCount += 1
                  self.net.setValueByKey( 'PacketLossRate', data[1] * 100 )


                elif summary['event_type'] == 'histogram-owdelay' and summary['summary_type'] == 'statistics':
                  self.OWDEventsCount += 1

                  # approximate jitter value as OWDMax - OWDMin
                  self.net.setValueByKey( 'Jitter', data[1]['maximum'] - data[1]['minimum'] )
                  self.net.setValueByKey( 'OneWayDelay', data[1]['mean'] )
                else:
                  continue

                # set the time and commit data to the database
                self.net.setStartTime( datetime.utcfromtimestamp( float( data[0] ) ) )
                self.net.setEndTime( datetime.utcfromtimestamp( float( data[0] ) + 300 ) )
                self.net.delayedCommit()
