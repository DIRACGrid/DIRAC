==================================
Message Queue connection tools
==================================

Users are supposed to use MQListener and MQPublisher classes which can be used as simply as the following:

.. code-block:: python

    publisher = MQPublisher( "MyQueue" )
    listener = MQListener( "MyQueue" )
    publisher.put( message )
    listener.get( message )

Or:

.. code-block:: python

    publisher = MQPublisher( "MyQueue" )
    listener = MQListener( "MyQueue", callback = myCallback )
    publisher.put( message )
    listener.run()

Example of the MQ configuration description is the following:::

    Resources
    {
      MQServices
      {
        mardirac3.in2p3.fr
        {
          MQType = Stomp
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
      }
    }

The queueName in the listener/publisher instantiation can be given as just the Queue name
(if no ambiguities ) or fully qualified name like "mardirac3.in2p3.fr::TestQueue" to avoid
ambiguities.