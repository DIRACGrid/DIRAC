"""
  Example implementation of a concreta consumer module
  which can be loaded by ConsumerReactor.
"""
import time
from DIRAC.Core.Base.ConsumerModule import ConsumerModule
from DIRAC.Core.Utilities.RabbitMQ import RabbitInterface
from DIRAC import gConfig
from DIRAC import S_ERROR

def getCurrentWMSystem():
  """Returns the current system name for WorkloadManagement
  """
  setup = gConfig.getValue( '/DIRAC/Setup', '' )
  return gConfig.getValue('DIRAC/Setups/' + setup +'/WorkloadManagement', '')

class SimpleLogConsumer( ConsumerModule ):
  """Just a simple example of the consumer implementation
     SimpleLogConsumer uses RabbitInterface object
     to connect to the queue.
     See RabbitInterface class for more info.
  """
  def __init__ ( self ):
    """ Method creates rabbitConnector
    """
    self.rabbitConnector = RabbitInterface()
    self.rabbitSystem = "MyRabbitSystem" #in this system all rabbit related options must be placed

  def execute( self ):
    """ Methods establishes the connection with rabbitMQ server
        sleeps for 50 seconds and checks if there is a message in the queue.
        It prints it on the screen and after that just disconnects.
    """
    result = self.rabbitConnector.setupConnection("MyRabbitSystem", "testQueue", True)
    if not result[ 'OK' ]:
      print result['Message']
      return S_ERROR( 'Failed to established connection to RabbitMQ server: %s' % result[ 'Message' ] )
    time.sleep(30)
    result = self.rabbitConnector.receive()
    if not result[ 'OK' ]:
      return S_ERROR( 'Failed to receive any message from the queue' % result[ 'Message' ] )
    print result['Value']
    self.rabbitConnector.unsetupConnection()
