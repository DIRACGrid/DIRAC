"""
  DIRAC class which is the abstract class for the consumers

  All DIRAC consumers must inherit from the basic class ConsumerModule
  and override its methods
"""

#from DIRAC import S_ERROR
from DIRAC.WorkloadManagementSystem.Consumer.ConsumerTools import getConsumerOption
from DIRAC.WorkloadManagementSystem.Consumer.ConsumerTools import getConsumerSection
#from DIRAC.Core.Utilities.MQConnector import MQConnector

class ConsumerModule(object):
  """ Base class for all consumer modules

      This class is used by the ConsumertReactor class to steer the execution of
      DIRAC consumers.
  """

  def __init__( self ):
    self._MQConnector = None
    self._MQConnectorProperties = {
                                    'MQConnectorSystem' : 'MyRabbitSystem',
                                    'MQConnectorModuleName' : 'DIRAC.Core.Utilities.RabbitMQConnector',
                                    'MQConnectorClassName' : 'RabbitConnection',
                                    'Host' : '',
                                    'Port' : '',
                                    'Queue' : '',
                                    'VH' : '',
                                    'ExchangeName' : '',
                                    'Type' : ''
                                  }

  def initialize( self, systemConsumerModuleName ):
    """
    """

    self._MQConnector = self._loadMQConnector( moduleName = self._MQConnectorProperties[ 'MQConnectorModuleName' ],
                                               className = self._MQConnectorProperties[ 'MQConnectorClassName' ])
    self._setMQConnectorProperties( systemConsumerModuleName )
    res = self._MQConnector.blockingConnection(system = self._MQConnectorProperties[ 'MQConnectorSystem' ],
                                               queueName = self._MQConnectorProperties[ 'Queue' ],
                                               receive = True,
                                               messageCallback = self.consume
                                              )

  def _loadMQConnector( self, moduleName = 'DIRAC.Core.Utilities.RabbitMQConnector', className = 'RabbitConnection' ):
    """
    """
    myModule = __import__ (moduleName, fromlist = [moduleName])
    connectorClass = getattr(myModule, className)
    return connectorClass()

  def _setMQConnectorProperties( self, systemConsumerModuleName ):
    """
    """
    consumerSection = getConsumerSection(systemConsumerModuleName)
    for option in self._MQConnectorProperties:
      res = getConsumerOption( option, consumerSection)
      if res[ 'OK' ]:
        self._MQConnectorProperties[option] = res[ 'Value' ]
      else:
        print 'Error: consumer option: %s was not found in section: %s' % ( option, consumerSection )

  def consume( self, headers, message ):
    """ Function must be overriden in the implementation
    """
    raise NotImplementedError('That should be implemented')
