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
  def __init__ ( self ):
    #super(AnotherTestConsumer,self).__init__()
    pass

  #or guarantee that it will never be overwritten
  def initialize( self ):
    super(AnotherTestConsumer, self).initialize()

  def consume( self, headers, message ):
    print "I have just eatean a tasty message:%s" % message
  
