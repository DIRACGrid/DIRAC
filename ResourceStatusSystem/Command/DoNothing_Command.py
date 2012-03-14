# $HeadURL $
''' DoNothing_Command

  Demo Command.

'''

from DIRAC.ResourceStatusSystem.Command.Command import *

__RCSID__ = '$Id: $'

class DoNothing_Command( Command ):
  
  def doCommand( self ):
    ''' Do nothing.       
    '''
    super( DoNothing_Command, self ).doCommand()

    return { 'Result' : None }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF