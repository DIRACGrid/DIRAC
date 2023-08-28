=====================
Simple Message Queues
=====================

.. warning ::

   Technology preview, will likely change interface


The very abstracted implementation of MQ in DIRAC offers flexibility, but at the cost of complexity and limitations (multiple host behind a broker alias for example). A simpler implementation is proposed, but is still under development. At the moment, this simpler implementation only supports ``Stomp``, but that is the only one used so far.

See :py:mod:`DIRAC.Resources.MessageQueue.Simple.StompInterface` for a more detailed documentation.

How to migrate
==============

The configuration of the MQ can remain the same in the CS.

The difference is in the code.

.. code-block:: python

  # Before
  from DIRAC.Resources.MessageQueue.MQCommunication import createProducer, createConsumer

  # New
  from DIRAC.Resources.MessageQueue.Simple.StompInterface import createProducer, createConsumer


The ``mqURI`` should also be changed to just the service name. For example

.. code-block:: python

  # Before
  result = createProducer("Monitoring::Queues::dirac.monitoring")

  # New
  result = createProducer("Monitoring", destination="dirac.monitoring")
  # or, if there is only one destination defined in the CS
  # but this should be avoided
  result = createProducer("Monitoring")


The simpler interface can also listen to multiple destinations.

.. code-block:: python

  # Before
  result = createConsumer("Monitoring::Queues::dirac.monitoring")

  # New
  result = createConsumer("Monitoring", destinations=["dirac.monitoring"])
  # or, if no destination is specified, will listen to ALL the destinations
  # in the CS
  result = createProducer("Monitoring")


There is however a compatibility layer, such that full ``mqURI`` are still accepted.

Consumer are now driven by ``listener`` class instead of callback functions. Please see :py:mod:`DIRAC.Resources.MessageQueue.Simple.StompInterface` for example on how to use it



.. warning ::

  The generic implementation was always doing a ``json.dumps`` before sending, and always doing a ``json.loads`` when upon receiving. The simple interface does not do it. You have to manage the serialization yourself
