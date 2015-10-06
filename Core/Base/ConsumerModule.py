"""
  DIRAC class which is the interface for the Consumers

  All DIRAC consumers must inherit from the basic class ConsumerModule
"""

class ConsumerModule:
  """ Base class for all consumer modules

      This class is used by the ConsumertReactor class to steer the execution of
      DIRAC consumers.
  """

  def __init__( self ):
    pass

  def execute( self ):
    pass
