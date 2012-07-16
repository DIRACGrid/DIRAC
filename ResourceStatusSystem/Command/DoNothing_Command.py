# $HeadURL $
''' DoNothing_Command

  Demo Command.

'''

from DIRAC.ResourceStatusSystem.Command.Command import Command

__RCSID__ = '$Id: $'

class DoNothing_Command( Command ):
  
  def __init__( self, *args, **kwargs ):
    super( DoNothing_Command, self ).__init__( *args, **kwargs )
  
  def doCommand( self ):
    ''' Do nothing.       
    '''
    #super( DoNothing_Command, self ).doCommand()

    return { 'Result' : None }
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF