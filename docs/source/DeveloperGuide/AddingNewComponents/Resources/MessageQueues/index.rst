.. _development_use_mq:

================
Message Queues
================

Message Queues are fully described in the DIRAC Configuration as explained in the
:ref:`configuration_message_queues`. In the code, Message Queues can be used to publish
messages which are arbitrary json structures. The *MQPublisher* objects are used in this case:

.. code-block:: python

   from DIRAC.Resources.MessageQueue.MQPublisher import MQPublisher

   publisher = MQPublisher( "TestQueue" )
   # Publish a message which is an arbitrary json structure
   result = publisher.put( message )

The Messages are received by listeners. Listeners are objects of the MQListener class.
These objects can request messages explicitly:

.. code-block:: python

   from DIRAC.Resources.MessageQueue.MQListener import MQListener

   listener = MQListener( "TestQueue" )
   result = listener.get( message )
   if result['OK']:
     message = result['Value']

Listeners can be instantiated with a callback function that will be called automatically
when new messages will arrive:

.. code-block:: python

  from DIRAC.Resources.MessageQueue.MQListener import MQListener

  def myCallback( headers, message ):
    <function implementation>

  listener = MQListener( "MyQueue", callback = myCallback )
  # Blocking call
  listener.run()

