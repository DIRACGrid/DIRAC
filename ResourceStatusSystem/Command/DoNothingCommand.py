# $HeadURL:  $
''' DoNothingCommand

  Demo Command.

'''

from DIRAC                                      import S_OK
from DIRAC.ResourceStatusSystem.Command.Command import Command

__RCSID__ = '$Id:  $'

class DoNothingCommand( Command ):
  
#  def __init__( self, *args, **kwargs ):
#    super( DoNothing_Command, self ).__init__( *args, **kwargs )
  
  def doCommand( self ):
    ''' Do nothing.       
    '''
    #super( DoNothing_Command, self ).doCommand()

    return S_OK( None )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF