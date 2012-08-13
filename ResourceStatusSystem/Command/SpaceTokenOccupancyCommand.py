# $HeadURL:  $
''' SpaceTokenOccupancyCommand
  
  The Command gets information of the SpaceTokenOccupancy from the lcg_utils
  
'''

import lcg_util

from DIRAC                                      import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Command.Command import Command

__RCSID__ = '$Id:  $'

class SpaceTokenOccupancyCommand( Command ):
  '''
  Uses lcg_util to query status of endpoint for a given token.
  ''' 

  def doCommand( self ):
    '''
    Run the command.
    '''

    if not 'spaceTokenEndpoint' in self.args:
      return self.returnERROR( S_ERROR( '"spaceTokenEndpoint" not found in self.args' ) )
    if not 'spaceToken' in self.args:
      return self.returnERROR( S_ERROR( '"spaceToken" not found in self.args' ) )

    spaceTokenEndpoint = self.args[ 'spaceTokenEndpoint' ] 
    spaceToken         = self.args[ 'spaceToken' ]
         
    occupancy = lcg_util.lcg_stmd( spaceToken, spaceTokenEndpoint, True, 0 )
           
    if occupancy[ 0 ] != 0:
      return self.returnERROR( S_ERROR( occupancy ) )  
    
    output     = occupancy[ 1 ][ 0 ]
    total      = float( output.get( 'totalsize',      '0' ) ) / 1e12 # Bytes to Terabytes
    guaranteed = float( output.get( 'guaranteedsize', '0' ) ) / 1e12
    free       = float( output.get( 'unusedsize',     '0' ) ) / 1e12
      
    result = S_OK( 
                  { 
                   'total'      : total, 
                   'free'       : free, 
                   'guaranteed' : guaranteed 
                   } 
                  )

    return result
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF