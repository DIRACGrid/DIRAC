.. _configuration_message_queues:

==================
Message Queues
==================

Message Queues are services for passing messages between DIRAC components.
These services are not part necessarily of the DIRAC software and are provided
by third parties. Access to the services is done via logical Queues (or Topics) which are
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

   result = createProducer( "mardirac3.in2p3.fr::Queue::TestQueue" )
   if result['OK']:
      producer = result['Value']

   result = createConsumer( "mardirac3.in2p3.fr::Queue::TestQueue" )
   if result['OK']:
      consumer = result['Value']

   result = producer.put( message )
   result = consumer.get( message )
   if result['OK']:
     message = result['Value']


In order not to spam the logs, the log output of Stomp is always silence, unless the environment variable `DIRAC_DEBUG_STOMP` is set to any value
