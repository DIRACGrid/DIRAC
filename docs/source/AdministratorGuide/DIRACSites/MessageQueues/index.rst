.. _configuration_message_queues:

==================
Message Queues
==================

Message Queues are services for passing messages between DIRAC components.
These services are not part necessarily of the DIRAC software and are provided
by third parties. Access to the services is done via logical Queues which are
described in the *Resources* section of the DIRAC configuration.

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
          # User name to access the MQ server
          User = guest
          # Password to access the MQ server. This option should never be defined
          # in the Global Configuration, only in the local one
          Password = guest
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

   from DIRAC.Resources.MessageQueue.MQPublisher import MQPublisher
   from DIRAC.Resources.MessageQueue.MQListener import MQListener
   ...
   publisher = MQPublisher( "TestQueue" )
   listener = MQListener( "TestQueue" )
   ...
   result = publisher.put( message )
   ...
   result = listener.get( message )
   if result['OK']:
     message = result['Value']

