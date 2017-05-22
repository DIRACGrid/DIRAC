"""
  DIRAC class which is the abstract class for the consumers

  All DIRAC consumers must inherit from the basic class ConsumerModule
  and override its methods
"""

from DIRAC.Core.Utilities.ConsumerTools import getConsumerOption
from DIRAC.Core.Utilities.ConsumerTools import getConsumerSection
from DIRAC import gLogger

class ConsumerModule(object):
  """ Base class for all consumer modules

      This class is used by the ConsumerReactor class to steer the execution of
      DIRAC consumers.
  """

  def __init__( self , systemConsumerModuleName ):
    self.log = gLogger.getSubLogger(systemConsumerModuleName)
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
    """Reads properties related to MQ connector from CS and
       loads MQconnector object. Finally the blocking connection
       is called so the consumer starts listening for the incoming
       messages.
    Args:
      systemConsumerModuleName(str): in form of [DIRAC System Name]/[DIRAC Consumer Name]
    """

    self._setMQConnectorProperties( systemConsumerModuleName )
    self._MQConnector = self._loadMQConnector( moduleName = self._MQConnectorProperties[ 'MQConnectorModuleName' ],
                                               className = self._MQConnectorProperties[ 'MQConnectorClassName' ])
    self._MQConnector.blockingConnection(system = self._MQConnectorProperties[ 'MQConnectorSystem' ],
                                               queueName = self._MQConnectorProperties[ 'Queue' ],
                                               receive = True,
                                               messageCallback = self.consume
                                              )

  def _loadMQConnector( self, moduleName = 'DIRAC.Core.Utilities.RabbitMQConnector', className = 'RabbitConnection' ):
    """Loads MQConnector module and class.
    Args:
      moduleName(str): full path to the MQConnector module.
      className(str): MQConnector class name.
    Returns:
      an instance of the specific MQConnector class.
    """
    myModule = __import__ (moduleName, fromlist = [moduleName])
    connectorClass = getattr(myModule, className)
    return connectorClass()

  def _setMQConnectorProperties( self, systemConsumerModuleName ):
    """Sets self._MQConnectorProperties from the CS. It is assumed that
       self._MQConnectorProperties dictionnary contains keys that
       correspond to strings which are options in the consumer section
       of CS e.g. 'Host, 'Port' etc.
    Args:
      systemConsumerModuleName(str): in form of [DIRAC System Name]/[DIRAC Consumer Name]
    """
    consumerSection = getConsumerSection(systemConsumerModuleName)
    for option in self._MQConnectorProperties:
      res = getConsumerOption( option, consumerSection)
      if res[ 'OK' ]:
        self._MQConnectorProperties[option] = res[ 'Value' ]
      else:
        self.log.error('Error: consumer option: %s was not found in section: %s' % ( option, consumerSection ))

  def consume( self, headers, message ):
    """ Function must be overriden in the implementation
    """
    raise NotImplementedError('That should be implemented')
