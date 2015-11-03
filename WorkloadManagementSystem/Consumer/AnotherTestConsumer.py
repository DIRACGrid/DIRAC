"""
  Example implementation of a concreta consumer module
  which can be loaded by ConsumerReactor.
"""
from DIRAC.Core.Base.ConsumerModule import ConsumerModule


class AnotherTestConsumer( ConsumerModule ):
  """Just a simple example of the consumer implementation
     AnotherTestConsumer uses RabbitInterface object
     to connect to the queue.
     See RabbitInterface class for more info.
  """

  #or guarantee that it will never be overwritten
  def initialize( self, systemConsumerModuleName ):
    super(AnotherTestConsumer, self).initialize( systemConsumerModuleName )

  def consume( self, headers, message ):
    print "I have just eatean a tasty message:%s" % message
