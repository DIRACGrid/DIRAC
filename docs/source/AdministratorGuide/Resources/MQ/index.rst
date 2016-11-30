==================================
RabbitMQ  administration tools
==================================

DIRAC provides an interface to the internal RabbitMQ user database via the RabbitMQAdmin class.

.. code-block:: python

    producer = createProducer( "mardirac3.in2p3.fr::Queue::MyQueue" )
    consumer = createConsumer( "mardirac3.in2p3.fr::Queue::MyQueue" )
    producer.put( message )
    consumer.get( message )


==========================================
Synchronization of RabbitMQ user database
==========================================

To be added
