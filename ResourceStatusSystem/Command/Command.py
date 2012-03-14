# $HeadURL $
''' Command

  Base class for all commands.

'''

from DIRAC import gLogger

__RCSID__ = '$Id: $'

class Command( object ):
  """ 
    The Command class is a simple base class for all the commands
    for interacting with the clients
  """

  def __init__( self ):
    self.args = None
    self.APIs = {}

################################################################################

  def setArgs( self, argsIn ):
    """
    Set the command arguments, as a tuple. The tuple has to contain at least 2 values.

    :params:

      :attr:`args`: a tuple
        - `args[0]` should be a ValidRes

        - `args[1]` should be the name of the ValidRes
    """
    if type(argsIn) != tuple:
      raise TypeError("`Args` of commands should be in a tuple.")

    self.args = argsIn

################################################################################

  def setAPI( self, apiName, apiInstance ):
    self.APIs[ apiName ] = apiInstance

################################################################################

  #to be extended by real commands
  def doCommand( self ):
    """ Before use, call at least `setArgs`.
    """

    if self.args is None:
      gLogger.error( "Before, set `self.args` with `self.setArgs(a)` function." )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF