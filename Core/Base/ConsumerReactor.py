"""
  DIRAC class to execute Consumers

  All DIRAC Consumers must inherit from the basic class ConsumerModule

  In the most common case, DIRAC Consumerss are executed using the dirac-consumer command.
  dirac-consumer accepts a list positional arguments. These arguments have the form:
  [DIRAC System Name]/[DIRAC Consumer Name]
  dirac-consumer then:
  - produces a instance of ConsumerReactor
  - loads the required modules using the ConsumerReactor.loadConsumerModules method
  - starts the execution loop using the ConsumerReactor.go method

"""

class ConsumerReactor:
  """
    Main interface to DIRAC consumers.
  """

  def __init__( self ):
    pass

  def go( self ):
    """
      Main method to control the execution of all configured consumers
    """
    pass

  def loadConsumerModules( self ):
    pass
