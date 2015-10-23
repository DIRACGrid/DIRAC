"""
  DIRAC class which is the abstract class for the consumers

  All DIRAC consumers must inherit from the basic class ConsumerModule
  and override its methods
"""

from DIRAC import S_ERROR
from DIRAC.Core.Utilities.MQConnector import MQConnector

class ConsumerModule(object):
  """ Base class for all consumer modules

      This class is used by the ConsumertReactor class to steer the execution of
      DIRAC consumers.
  """

  def __init__( self ):
    self._MQConnector = None

  def initialize( self ):
    """
    """
    print "really?"
    #based on some arguments decide what type of MQ and if the connection should be blocking or not?
    from DIRAC.Core.Utilities.RabbitMQ import RabbitInterface
    self._MQConnector = RabbitInterface()
    self._MQConnector.connectBlocking(system ="MyRabbitSystem",
                        queueName = "testQueue",
                        receive = True,
                        messageCallback = self.consume)

  def consume( self, headers, message ):
    """ Function must be overriden in the implementation
    """
    raise NotImplementedError('That should be implemented')
