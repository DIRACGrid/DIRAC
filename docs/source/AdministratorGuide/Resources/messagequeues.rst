.. _configuration_message_queues:

==============
Message Queues
==============

Message Queues are services for passing messages between DIRAC components.
These services are not part necessarily of the DIRAC software and are provided
by third parties. Access to the services is done via logical Queues (or Topics).
Queues and Topics are two popular variation of the MQ communication model.
In the first case, the messages from the queue are typically delivered to the subscribed
consumers one by one. One message will be received by exactly one consumer.
If no consumer is available, then the messages are stored in the queue. Many consumers connected
to the same queue can be used for the load balancing purposes.
The topic architecture can be see as implementation of the publish-subscribe pattern. The messages
are typically grouped in categories (e.g. by assigning the label called topic), and consumers
subscribe to chosen topics. When the message becomes available, it is send to all
subscribed consumers.
Detailed implementation of Topic/Queue mechanism can differ dependent e.g. MQ broker used.

The available implementation of the Message Queue uses Stomp protocol.
All the Stomp-dependent details are encapsulated in StompMQConnector class,
which extends the generic MQConnector class. 
It is possible to provide a self-defined connector by extending the 
MQConnector class.

A commented example of the Message Queues configuration is provided below.
Each option value is representing its default value::

    Resources
    {
      # General section for all the MessageQueue service. Each subsection is
      # dedicated to a particular MQ server
      MQServices
      {
        # MQ server section. The name of the section is arbitrary, not necessarily
        # the host name
        mardirac3.in2p3.fr
        {
          # The MQ type defines the protocol by which the service is accessed.
          # Currently only Stomp protocol is available. Mandatory option
          MQType = Stomp
          # The MQ server host name
          Host = mardirac3.in2p3.fr
          # The MQ server port number
          Port = 9165
          # Virtual host
          VHost = /
          # User name to access the MQ server (not needed if you are using SSL authentication)
          User = guest
          # Password to access the MQ server. (not needed if you are using SSL authentication)
          # This option should never be defined
          # in the Global Configuration, only in the local one
          Password = guest
          # if SSLVersion is set, then you are connecting using a certificate host/key pair
          # You can also provide a location for the host/key certificates with the options
          # "HostCertificate" and "HostKey" (which take a path as value)
          # and when these options are not set, the standard DIRAC locations will be used
          SSLVersion = TLSv1
          # General section containing subsections per Message Queue. Multiple Message
          # Queues can be defined by MQ server
          Queues
          {
            # Message Queue section. The name of the section is defining the name
            # of the Message Queue
            TestQueue
            {
              # Option defines if messages reception is acknowledged by the listener
              Acknowledgement = True
              # Option defines if the Message Queue is persistent or not
              Persistent = False
            }
          }
        }
      }
    }

Once Message Queues are defined in the configuration, they can be used in the DIRAC codes
like described in :ref:`development_use_mq`, for example::

   from DIRAC.Resources.MessageQueue.MQCommunication import createProducer
   from DIRAC.Resources.MessageQueue.MQCommunication import createConsumer

   result = createProducer( "mardirac3.in2p3.fr::Queues::TestQueue" )
   if result['OK']:
      producer = result['Value']

   result = createConsumer( "mardirac3.in2p3.fr::Queues::TestQueue" )
   if result['OK']:
      consumer = result['Value']

   result = producer.put( message )
   result = consumer.get( message )
   if result['OK']:
     message = result['Value']


In order not to spam the logs, the log output of Stomp is always silence, unless the environment variable `DIRAC_DEBUG_STOMP` is set to any value.


Message Queue nomenclature in DIRAC
-----------------------------------

* MQ - Message Queue System e.g. RabbitMQ
* mqMessenger - processes that send or receive messages to/from the MQ system.
  We define two types of messengers: consumer (MQConsumer class) and producer (MQProducer class).
* mqDestination is the endpoint of MQ systems. We define two kind of destinations: Queue or Topic.
  which correspond  to two type of communication schemes between MQ and consumers/producers.
* mqService - unique identifier that characterises an MQ resource in the DIRAC CS. mqService can have one or more topics and/or queues assigned.
* mqConnection: authenticated link between an MQ and one or more producers or/and consumers. The link can be characterised by mqService.
* mqURI - pseudo URI identifier that univocally identifies the destination.
  It has the following format mqService::mqDestinationType::mqDestination name e.g."mardirac3.in2p3.fr::Queues::TestQueue" or
  "mardirac3.in2p3.fr::Topics::TestTopic".
* mqType - type of the MQ communication protocol e.g. Stomp.
* MQConnector - provides abstract interface to communicate with a given MQ system. It can be specialized e.g.  StompMQConnector.
