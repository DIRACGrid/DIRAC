# $HeadURL:  $
''' SpaceTokenOccupancyCommand
  
  The Command gets information of the SpaceTokenOccupancy from the lcg_utils
  
'''

import lcg_util

from DIRAC                                                      import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

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

class SpaceTokenOccupancyCacheCommand( Command ):
  '''
    Cache command that reads from the StorageTokenOccupancyCache table
  ''' 

  def __init__( self, args = None, clients = None ):
    
    super( SpaceTokenOccupancyCacheCommand, self ).__init__( args, clients )

    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:  
      self.rmClient = ResourceManagementClient()

  def doCommand( self ):
    '''
    Run the command.
    '''

    if not 'elementType' in self.args:
      return self.returnERROR( S_ERROR( '"elementType" not found in self.args' ) )
    elementType = self.args[ 'elementType' ]
    
    if not elementType == 'StorageElement':
      return self.returnERROR( S_ERROR( 'Expecting StorageElement, not %s' % elementType ) )
    
    if not 'name' in self.args:
      return self.returnERROR( S_ERROR( '"name" not found in self.args' ) )
    name = self.args[ 'name' ]
 
    meta = { 'columns' : [ 'Total', 'Free', 'Guaranteed' ] }
 
    res = self.rmClient.selectSpaceTokenOccupancyCache( storageElement = name, 
                                                        meta = meta )
    
    if not res[ 'OK' ]:
      return self.returnERROR( res )
    res = res[ 'Value' ]
    
    zippedRes = dict( zip( res[ 'Columns' ], res[ 'Value' ] ) )
    
    result = { 
              'total'      : zippedRes[ 'Total' ], 
              'free'       : zippedRes[ 'Free' ], 
              'guaranteed' : zippedRes[ 'Guaranteed' ] 
              }   
    
    return S_OK( result )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF