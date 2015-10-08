"""
  DIRAC class which is the abstract class for the consumers

  All DIRAC consumers must inherit from the basic class ConsumerModule
  and override its methods
"""

class ConsumerModule(object):
  """ Base class for all consumer modules

      This class is used by the ConsumertReactor class to steer the execution of
      DIRAC consumers.
  """

  def __init__( self ):
    """  Abstract class
    """
    raise NotImplementedError('That should be implemented')

  def execute( self ):
    """ Function should be overriden in the implementation
    """
    raise NotImplementedError('That should be implemented')
