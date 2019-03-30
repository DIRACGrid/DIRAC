.. _development_use_mq:

================
Message Queues
================

Message Queues are fully described in the DIRAC Configuration as explained in the
:ref:`configuration_message_queues`. In the code, Message Queues can be used to publish
messages which are arbitrary json structures. The *MQProducer* objects are used in this case:

.. code-block:: python

   from DIRAC.Resources.MessageQueue.MQCommunication import createProducer

   result = createProducer( "mardirac3.in2p3.fr::Queues::TestQueue" )
   if result['OK']:
      producer = result['Value']
   # Publish a message which is an arbitrary json structure
   result = producer.put( message )

The Messages are received by consumers. Consumers are objects of the MQConsumer class.
These objects can request messages explicitly:

.. code-block:: python

   from DIRAC.Resources.MessageQueue.MQCommunication import createConsumer

   result = createConsumer( "mardirac3.in2p3.fr::Queues::TestQueue" )
   if result['OK']:
      consumer = result['Value']
   result = consumer.get( message )
   if result['OK']:
     message = result['Value']

consumers can be instantiated with a callback function that will be called automatically
when new messages will arrive:

.. code-block:: python

  from DIRAC.Resources.MessageQueue.MQCommunication import createConsumer

  def myCallback( headers, message ):
    <function implementation>

   result = createConsumer( "mardirac3.in2p3.fr::Queues::TestQueue", callback = myCallback )
   if result['OK']:
      consumer = result['Value']


The destination name (queue or topic) in the consumer/producer instantiation must be given as
fully qualified name like "mardirac3.in2p3.fr::Queues::TestQueue" or
"mardirac3.in2p3.fr::Topics::TestTopic".

====================================
Message Queue nomenclature in DIRAC
====================================

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
