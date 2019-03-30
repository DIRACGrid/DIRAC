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
