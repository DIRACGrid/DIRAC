# $HeadURL:  $
''' SpaceTokenOccupancyCommand
  
  The Command gets information of the SpaceTokenOccupancy from the lcg_utils
  
'''


import lcg_util


from DIRAC                                                      import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess                            import pythonCall
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers


__RCSID__ = '$Id:  $'


class SpaceTokenOccupancyCommand( Command ):
  '''
  Uses lcg_util to query status of endpoint for a given token.
  ''' 


  def __init__( self, args = None, clients = None ):
    
    super( SpaceTokenOccupancyCommand, self ).__init__( args, clients )
    
    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient()


  def _storeCommand( self, results ):
    '''
      Stores the results of doNew method on the database.
    '''
    
    for result in results:
      
      resQuery = self.rmClient.addOrModifySpaceTokenOccupancyCache( result[ 'Endpoint' ], 
                                                                    result[ 'Token' ], 
                                                                    result[ 'Total' ], 
                                                                    result[ 'Guaranteed' ], 
                                                                    result[ 'Free' ] )
      if not resQuery[ 'OK' ]:
        return resQuery
    
    return S_OK()  


  def _prepareCommand( self ):
    '''
      SpaceTokenOccupancy requires one argument:
      - elementName : <str>
    
      Given a (storage)elementName, we calculate its endpoint and spaceToken,
      which are used to query the srm interface.
    '''

    if not 'name' in self.args:
      return S_ERROR( '"name" not found in self.args' )
    elementName = self.args[ 'name' ]
    
    endpoint = CSHelpers.getStorageElementEndpoint( elementName )
    if not endpoint[ 'OK' ]:
      return endpoint
    endpoint = endpoint[ 'Value' ]
    
    spaceToken = CSHelpers.getSEProtocolOption( elementName, 'SpaceToken' )
    if not spaceToken[ 'OK' ]:
      return spaceToken
    spaceToken = spaceToken[ 'Value']
  
    return S_OK( ( endpoint, spaceToken ) )


  def doNew( self, masterParams = None ):
    '''
      Gets the parameters to run, either from the master method or from its
      own arguments.
      
      It queries the srm interface, and hopefully it will not crash. Out of the
      results, we keep totalsize, guaranteedsuze, and unusedsize.
      
      Then, they are recorded and returned.
    '''   
     
    if masterParams is not None:
      spaceTokenEndpoint, spaceToken = masterParams
    else:
      params = self._prepareCommand()
      if not params[ 'OK' ]:
        return params
      spaceTokenEndpoint, spaceToken = params[ 'Value' ] 
      
    # 10 secs of timeout. If it works, the reply is immediate.  
    occupancy = pythonCall( 10, lcg_util.lcg_stmd, spaceToken, spaceTokenEndpoint, True, 0 )
    if not occupancy[ 'OK' ]:
      return occupancy
    occupancy = occupancy[ 'Value' ]
    
    #Timeout does not work here...
    #occupancy = lcg_util.lcg_stmd( spaceToken, spaceTokenEndpoint, True, 0 )
    
    if occupancy[ 0 ] != 0:
      return S_ERROR( occupancy )
    output = occupancy[ 1 ][ 0 ]

    sTokenDict = {} 
    sTokenDict[ 'Endpoint' ]   = spaceTokenEndpoint
    sTokenDict[ 'Token' ]      = spaceToken
    sTokenDict[ 'Total' ]      = float( output.get( 'totalsize', '0' ) ) / 1e12 # Bytes to Terabytes
    sTokenDict[ 'Guaranteed' ] = float( output.get( 'guaranteedsize', '0' ) ) / 1e12
    sTokenDict[ 'Free' ]       = float( output.get( 'unusedsize', '0' ) ) / 1e12                       
    
    storeRes = self._storeCommand( [ sTokenDict ] )
    if not storeRes[ 'OK' ]:
      return storeRes
           
    return S_OK( [ sTokenDict ] )                                 


  def doCache( self ):
    '''
      Method that reads the cache table and tries to read from it. It will 
      return a list of dictionaries if there are results.   
    '''
    
    params = self._prepareCommand()
    if not params[ 'OK' ]:
      return params
    spaceTokenEndpoint, spaceToken = params[ 'Value' ] 
      
    result = self.rmClient.selectSpaceTokenOccupancyCache( spaceTokenEndpoint, spaceToken )
    if result[ 'OK' ]:
      result = S_OK( [ dict( zip( result[ 'Columns' ], res ) ) for res in result[ 'Value' ] ] )
           
    return result    


  def doMaster( self ):
    '''
      Master method. Gets all endpoints from the storage elements and all 
      the spaceTokens. Could have taken from Shares/Disk as well. 
      It queries for all their possible combinations, unless there are records
      in the database for those combinations, which then are not queried. 
    '''
    
    storageElementNames = CSHelpers.getStorageElements()
    if not storageElementNames[ 'OK' ]:
      return storageElementNames
    storageElementNames = storageElementNames[ 'Value' ]
    
    endpointTokenSet = set()
    
    for storageElementName in storageElementNames:
    
      endpoint = CSHelpers.getStorageElementEndpoint( storageElementName )
      if not endpoint[ 'OK' ]:
        continue
      endpoint = endpoint[ 'Value' ]
    
      spaceToken = CSHelpers.getStorageElementSpaceToken( storageElementName )
      if not spaceToken[ 'OK' ]:
        continue
      spaceToken = spaceToken[ 'Value' ]
      
      endpointTokenSet.add( ( endpoint, spaceToken ) )
    
    gLogger.verbose( 'Processing %s' % endpointTokenSet )
    
    for elementToQuery in endpointTokenSet:

      result = self.doNew( elementToQuery  ) 
      if not result[ 'OK' ]:
        self.metrics[ 'failed' ].append( result )      
       
    return S_OK( self.metrics )


#...............................................................................
#EOF
