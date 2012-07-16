# $HeadURL $
''' Command

  Base class for all commands.

'''

__RCSID__ = '$Id: $'

class Command( object ):
  """ 
    The Command class is a simple base class for all the commands
    for interacting with the clients
  """

  def __init__( self, args = None, decissionParams = None, clients = None ):
    
    self.args            = ( 1 and args ) or {}
    self.decissionParams = ( 1 and decissionParams ) or {}   
    self.APIs            = ( 1 and clients ) or {}

  #to be extended by real commands
  def doCommand( self ):
    """ Before use, call at least `setArgs`.
    """
    
    return { 'Result' : self.args }
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF