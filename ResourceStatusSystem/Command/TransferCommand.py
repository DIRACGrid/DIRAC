# $HeadURL:  $
''' TransferQualityCommand module
'''

from datetime                                                   import datetime, timedelta

from DIRAC                                                      import gLogger, S_OK, S_ERROR
from DIRAC.AccountingSystem.Client.ReportsClient                import ReportsClient
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers

__RCSID__ = '$Id:  $'  

class TransferChannelCommand( Command ):
  '''
    TransferChannel "master" Command
  '''
  
  def __init__( self, args = None, clients = None ):
    
    super( TransferChannelCommand, self ).__init__( args, clients )
    
    if 'ReportsClient' in self.apis:
      self.rClient = self.apis[ 'ReportsClient' ]
    else:
      self.rClient = ReportsClient() 

    if 'ReportGenerator' in self.apis:
      self.rgClient = self.apis[ 'ReportGenerator' ]
    else:
      self.rgClient = RPCClient( 'Accounting/ReportGenerator' )       
    
    self.rClient.rpcClient = self.rgClient  

    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient()
  
  def _storeCommand( self, results ):
    '''
      Stores the results of doNew method on the database.    
    '''

    for result in results:
      
      resQuery = self.rmClient.addOrModifyTransferCache( result[ 'SourceName' ], 
                                                         result[ 'DestinationName' ], 
                                                         result[ 'Metric' ], 
                                                         result[ 'Value' ] )
      if not resQuery[ 'OK' ]:
        return resQuery
    return S_OK()  
  
  def _prepareCommand( self ):
    '''
      TransferChannelCommand requires four arguments:
      - hours       : <int>
      - direction   : Source | Destination
      - elementName : <str>
      - metric      : Quality | FailedTransfers
      
      GGUSTickets are associated with gocDB names, so we have to transform the
      diracSiteName into a gocSiteName.    
    '''

    if not 'hours' in self.args:
      return S_ERROR( 'Number of hours not specified' )
    hours = self.args[ 'hours' ]

    if not 'direction' in self.args:
      return S_ERROR( 'element is missing' )
    direction = self.args[ 'direction' ]

    if direction not in [ 'Source', 'Destination' ]:
      return S_ERROR( 'direction is not Source nor Destination' )

    if not 'elementName' in self.args:
      return S_ERROR( '"elementName" is missing' )
    name = self.args[ 'elementName' ]

    if not 'metric' in self.args:
      return S_ERROR( 'metric is missing' )
    metric = self.args[ 'metric' ]
    
    if metric not in [ 'Quality', 'FailedTransfers' ]:
      return S_ERROR( 'metric is not Quality nor FailedTransfers' )

    return S_OK( ( hours, name, direction, metric ) )
  
  def doNew( self, masterParams = None ):
    '''
      Gets the parameters to run, either from the master method or from its
      own arguments.
      
      For every elementName ( cannot process bulk queries.. ) contacts the 
      accounting client. It reurns dictionaries like { 'X -> Y' : { id: 100%.. } }
      
      If there are ggus tickets, are recorded and then returned.        
    '''
    
    if masterParams is not None:
      hours, name, direction, metric = masterParams
      
    else:
      params = self._prepareCommand()
      if not params[ 'OK' ]:
        return params
      hours, name, direction, metric = params[ 'Value' ] 
    
    toD   = datetime.utcnow()    
    fromD = toD - timedelta( hours = hours )
        
    # dictionary with conditions for the accounting
    transferDict = { 
                     'OperationType' : 'putAndRegister',
                     direction       : name
                     }

    if metric == 'FailedTransfers':
      transferDict[ 'FinalStatus' ] = [ 'Failed' ]

    transferResults = self.rClient.getReport( 'DataOperation', metric, fromD, 
                                              toD, transferDict, 'Channel' )
    
    if not transferResults[ 'OK' ]:
      return transferResults
    transferResults = transferResults[ 'Value' ]
    
    if not 'data' in transferResults:
      return S_ERROR( 'Missing data key' )
    transferResults = transferResults[ 'data' ]

    uniformResult = []
           
    for channel, elementDict in transferResults.items():
      
      try:
        source, destination = channel.split( ' -> ' )
      except ValueError:
        continue  
      
      channelDict = {}
      channelDict[ 'SourceName' ]      = source
      channelDict[ 'DestinationName' ] = destination
      channelDict[ 'Metric' ]          = metric
      channelDict[ 'Value' ]           = sum( elementDict.values() ) / len( elementDict.values() )
      
      uniformResult.append( channelDict )
                 
    storeRes = self._storeCommand( uniformResult )
    if not storeRes[ 'OK' ]:
      return storeRes
           
    return S_OK( uniformResult )  
    
  def doCache( self ):
    '''
      Method that reads the cache table and tries to read from it. It will 
      return a list of dictionaries if there are results.
    '''
    
    params = self._prepareCommand()
    if not params[ 'OK' ]:
      return params
    _hours, direction, name, metric = params[ 'Value' ] 
      
    result = self.rmClient.selectTransferCache( name, direction, metric )  
    if result[ 'OK' ]:
      result = S_OK( [ dict( zip( result[ 'Columns' ], result[ 'Value' ] ) ) ] )
           
    return result      

  def doMaster( self ):
    '''
      Master method, which looks little bit spaguetti code, sorry !
      - It gets all Sites.
      - It gets all StorageElements
      
      As there is no bulk query, it compares with what we have on the database.
      It queries a portion of them.
    '''

    sites = CSHelpers.getSites()
    if not sites[ 'OK' ]:
      return sites
    sites = sites[ 'Value' ]
  
    ses = CSHelpers.getStorageElements()
    if not ses[ 'OK' ]:
      return ses
    ses = ses[ 'Value' ]
      
    elementNames = sites + ses   

#    sourceQuery = self.rmClient.selectTransferCache( meta = { 'columns' : [ 'SourceName' ] } )
#    if not sourceQuery[ 'OK' ]:
#      return sourceQuery
#    sourceQuery = [ element[0] for element in sourceQuery[ 'Value' ] ]
#    
#    sourceElementsToQuery = list( set( elementNames ).difference( set( sourceQuery ) ) )
    gLogger.info( 'Processing %s' % elementNames )
 
    for metric in [ 'Quality', 'FailedTransfers' ]:
      for direction in [ 'Source', 'Destination' ]: 
        # 2 hours of window
        result = self.doNew( ( 2, elementNames, direction, metric )  ) 
        if not result[ 'OK' ]:
          self.metrics[ 'failed' ].append( result )
       
    return S_OK( self.metrics )
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF