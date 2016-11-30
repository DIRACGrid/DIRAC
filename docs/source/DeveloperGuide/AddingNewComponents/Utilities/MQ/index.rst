==================================
Message Queue connection tools
==================================

Users are supposed to use MQConsumer and MQProducer classes.
The creation of Producers and Consumers is handled by the MQCommunication interface, that
provides two functions: createProducer( mqURI ) and createConsumer( mqURI, callback ).
All parameters describing the MQ destination e.g. queue or topic are loaded from the Configuration
System. mqURI is a pseudo-URI identifier that points to the registry in the Configuration System.
The code can be used  as simply as the following:

.. code-block:: python

    producer = createProducer( "mardirac3.in2p3.fr::Queue::MyQueue" )
    consumer = createConsumer( "mardirac3.in2p3.fr::Queue::MyQueue" )
    producer.put( message )
    consumer.get( message )

Or:

.. code-block:: python

    producer = createProducer( "mardirac3.in2p3.fr::Queue::MyQueue" )
    consumer = createConsumer( "mardirac3.in2p3.fr::Queue::MyQueue", callback = myCallback )
    producer.put( message )

Example of the MQ configuration description is the following:::

    Resources
    {
      MQServices
      {
        mardirac3.in2p3.fr
        {
          MQType = Stomp
          VHost = /
          Host = mardirac3.in2p3.fr
          Port = 9165
          User = guest
          Password = guest
          Queues
          {
            TestQueue
            {
              Acknowledgement = True
            }
          }
        }
        testdir.cern.ch
        {
          MQType = Stomp
          VHost = /
          Host = testdir.cern.ch
          Port = 61613
          SSLVersion = TLSv1
          HostCertificate =/my/host/cert
          HostKey =/my/key
          Queues
          {
            test4
            {
              Acknowledgement = True
            }
          }
        }
      }
    }

The destination name (queue or topic) in the consumer/producer instantiation must be given as
fully qualified name like "mardirac3.in2p3.fr::Queue::TestQueue" or
"mardirac3.in2p3.fr::Topic::TestTopic".

==================================
Message Queue nomenclature in DIRAC
==================================

* MQ - Message Queue System e.g. RabbitMQ
* mqMessenger - processes that send or receive messages to/from the MQ system.
  We define two types of messengers: consumer (MQConsumer class) and producer (MQProducer class).
* mqDestination is the endpoint of MQ systems. We define two kind of destinations: Queue or Topic.
  which correspond  to two type of communication schemes between MQ and consumers/producers.
* mqService - unique identifier that characterises an MQ resource in the DIRAC CS. mqService can have one or more topics and/or queues assigned.
* mqConnection: authenticated link between an MQ and one or more producers or/and consumers. The link can be characterised by mqService.
* mqURI - pseudo URI identifier that univocally identifies the destination.
  It has the following format mqService::mqDestinationType::mqDestination name e.g."mardirac3.in2p3.fr::Queue::TestQueue" or
  "mardirac3.in2p3.fr::Topic::TestTopic".
* mqType - type of the MQ communication protocol e.g. Stomp.
* MQConnector - provides abstract interface to communicate with a given MQ system. It can be specialized e.g.  StompMQConnector.
