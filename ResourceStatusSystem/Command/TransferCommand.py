# $HeadURL:  $
''' TransferQualityCommand module
'''

from datetime                                                   import datetime, timedelta

from DIRAC                                                      import S_OK, S_ERROR
from DIRAC.AccountingSystem.Client.ReportsClient                import ReportsClient
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers

__RCSID__ = '$Id:  $'  

class TransferChannelCommand( Command ):
  
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

    for result in results:
      
      resQuery = self.rmClient.addOrModifyTransferCache( result[ 'SourceName' ], 
                                                         result[ 'DestinationName' ], 
                                                         result[ 'Metric' ], 
                                                         result[ 'Value' ] )
      if not resQuery[ 'OK' ]:
        return resQuery
    
    return S_OK()  
  
  def _prepareCommand( self ):

    if not 'hours' in self.args:
      return S_ERROR( 'Number of hours not specified' )
    hours = self.args[ 'hours' ]

    if not 'direction' in self.args:
      return S_ERROR( 'element is missing' )
    direction = self.args[ 'direction' ]

    if direction not in [ 'Source', 'Destination' ]:
      return S_ERROR( 'direction is not Source nor Destination' )

    if not 'name' in self.args:
      return S_ERROR( 'name is missing' )
    name = self.args[ 'name' ]

    if not 'metric' in self.args:
      return S_ERROR( 'metric is missing' )
    metric = self.args[ 'metric' ]
    
    if metric not in [ 'Quality', 'FailedTransfers' ]:
      return S_ERROR( 'metric is not Quality nor FailedTransfers' )

    return S_OK( ( hours, name, direction, metric ) )
  
  def doNew( self, masterParams = None ):
    
    if masterParams is not None:
      hours, name, direction, metric = masterParams
      
    else:
      params = self._prepareCommand()
      if not params[ 'OK' ]:
        return params
      hours, name, direction, metric = params[ 'Value' ] 
      
    fromD = datetime.utcnow() - timedelta( hours = hours )
    toD   = datetime.utcnow()  
    
    transferDict = { 
                     'OperationType' : 'putAndRegister',
                     direction   : name
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
    
    params = self._prepareCommand()
    if not params[ 'OK' ]:
      return params
    _hours, direction, name, metric = params[ 'Value' ] 
      
    result = self.rmClient.selectTransferCache( name, direction, metric )  
    if result[ 'OK' ]:
      result = S_OK( [ dict( zip( result[ 'Columns' ], result[ 'Value' ] ) ) ] )
           
    return result      

  def doMaster( self ):

    sites = CSHelpers.getSites()
    if not sites[ 'OK' ]:
      return sites
    sites = sites[ 'Value' ]
  
    ses = CSHelpers.getStorageElements()
    if not ses[ 'OK' ]:
      return ses
    ses = ses[ 'Value' ]
      
    elementNames = sites + ses   

    sourceQuery = self.rmClient.selectTransferCache( meta = { 'columns' : [ 'SourceName' ] } )
    if not sourceQuery[ 'OK' ]:
      return sourceQuery
    sourceQuery = sourceQuery[ 'Value' ]
    
    sourceElementsToQuery = list( set( elementNames ).difference( set( sourceQuery ) ) )
 
    for metric in [ 'Quality', 'FailedTransfers' ]:
      for direction in [ 'Source', 'Destination' ]: 
        result = self.doNew( ( 2, sourceElementsToQuery, direction, metric )  ) 
        if not result[ 'OK' ]:
          self.metrics[ 'failed' ].append( result )
       
    return S_OK( self.metrics )           

################################################################################
################################################################################
################################################################################
################################################################################
################################################################################

#class TransferQualityCommand( Command ):
#
#  def __init__( self, args = None, clients = None ):
#    
#    super( TransferQualityCommand, self ).__init__( args, clients )
#    
#    if 'ReportsClient' in self.apis:
#      self.rClient = self.apis[ 'ReportsClient' ]
#    else:
#      self.rClient = ReportsClient() 
#
#    if 'ReportGenerator' in self.apis:
#      self.rgClient = self.apis[ 'ReportGenerator' ]
#    else:
#      self.rgClient = RPCClient( 'Accounting/ReportGenerator' )       
#    
#    self.rClient.rpcClient = self.rgClient  
#
#  def doCommand( self ):
#
#    if not 'hours' in self.args:
#      return self.returnERROR( S_ERROR( 'Number of hours not specified' ) )
#    hours = self.args[ 'hours' ]
#
#    if not 'direction' in self.args:
#      return self.returnERROR( S_ERROR( 'element is missing' ) )
#    direction = self.args[ 'direction' ]
#
#    if direction not in [ 'Source', 'Destination' ]:
#      return self.returnERROR( S_ERROR( 'direction is not Source nor Destination' ) )
#
#    if not 'name' in self.args:
#      return self.returnERROR( S_ERROR( 'name is missing' ) )
#    name = self.args[ 'name' ]
#    
#    # If name is None, we take all Sites or StorageElements
#    if name is None:
#      
#      sites = CSHelpers.getSites()
#      if not sites[ 'OK' ]:
#        return self.returnERROR( sites )
#      sites = sites[ 'Value' ]
#  
#      ses = CSHelpers.getStorageElements()
#      if not ses[ 'OK' ]:
#        return self.returnERROR( ses )
#      ses = ses[ 'Value' ]
#      
#      name = sites + ses        
#
#    if not isinstance( name, list ):
#      name = list( name )
#
#    fromD = datetime.utcnow() - timedelta( hours = hours )
#    toD   = datetime.utcnow()
#
#    qualityDict = { 'OperationType' : 'putAndRegister' }
#    if direction == 'Source':
#      qualityDict[ 'Destination' ] = name      
#    else:
#      qualityDict[ 'Source' ] = name
# 
#    qualityResults = self.rClient.getReport( 'DataOperation', 'Quality', fromD, toD,
#                                             qualityDict, direction )
#    if not qualityResults[ 'OK' ]:
#      return self.returnERROR( qualityResults )
#    qualityResults = qualityResults[ 'Value' ]
#    
#    if not 'data' in qualityResults:
#      return self.returnERROR( S_ERROR( 'Missing data key' ) )
#    qualityResults = qualityResults[ 'data' ]
#
#    qualityMean = {}
#           
#    for element, elementDict in qualityResults.items():
#      
#      if element in ( 'Suceeded', 'Total' ):
#        # Sometimes it returns this element, which we do not want
#        continue
#      
#      values = elementDict.values()
#      
#      #FIXME: will we get empty dictionaries ?
#      if values:
#        qualityMean[ element ] = sum( values ) / len( values )        
##      else:     
##        qualityMean[ element ] = 0   
#           
#    return S_OK( qualityMean )  
#
#################################################################################
#
#class TransferFailedCommand( Command ):
#
#  def __init__( self, args = None, clients = None ):
#    
#    super( TransferFailedCommand, self ).__init__( args, clients )
#    
#    if 'ReportsClient' in self.apis:
#      self.rClient = self.apis[ 'ReportsClient' ]
#    else:
#      self.rClient = ReportsClient() 
#
#    if 'ReportGenerator' in self.apis:
#      self.rgClient = self.apis[ 'ReportGenerator' ]
#    else:
#      self.rgClient = RPCClient( 'Accounting/ReportGenerator' )       
#    
#    self.rClient.rpcClient = self.rgClient  
#
#  def doCommand( self ):
#
#    if not 'hours' in self.args:
#      return self.returnERROR( S_ERROR( 'Number of hours not specified' ) )
#    hours = self.args[ 'hours' ]
#
#    if not 'direction' in self.args:
#      return self.returnERROR( S_ERROR( 'element is missing' ) )
#    direction = self.args[ 'direction' ]
#
#    if direction not in [ 'Source', 'Destination' ]:
#      return self.returnERROR( S_ERROR( 'direction is not Source nor Destination' ) )
#
#    if not 'name' in self.args:
#      return self.returnERROR( S_ERROR( 'name is missing' ) )
#    name = self.args[ 'name' ]
#    
#    # If name is None, we take all Sites or StorageElements
#    if name is None:
#      
#      sites = CSHelpers.getSites()
#      if not sites[ 'OK' ]:
#        return self.returnERROR( sites )
#      sites = sites[ 'Value' ]
#  
#      ses = CSHelpers.getStorageElements()
#      if not ses[ 'OK' ]:
#        return self.returnERROR( ses )
#      ses = ses[ 'Value' ]
#      
#      name = sites + ses     
#
#    if not isinstance( name, list ):
#      name = list( name )
#
#    fromD = datetime.utcnow() - timedelta( hours = hours )
#    toD   = datetime.utcnow()
#
#    qualityDict = { 'OperationType' : 'putAndRegister', 'FinalStatus' : [ 'Failed' ] }
#    if direction == 'Source':
#      qualityDict[ 'Destination' ] = name      
#    else:
#      qualityDict[ 'Source' ] = name
# 
#    qualityResults = self.rClient.getReport( 'DataOperation', 'FailedTransfers', fromD, toD,
#                                             qualityDict, direction )
#    if not qualityResults[ 'OK' ]:
#      return self.returnERROR( qualityResults )
#    qualityResults = qualityResults[ 'Value' ]
#    
#    if not 'data' in qualityResults:
#      return self.returnERROR( S_ERROR( 'Missing data key' ) )
#    qualityResults = qualityResults[ 'data' ]
#
#    qualityMean = {}
#           
#    for element, elementDict in qualityResults.items():
#      
#      if element in ( 'Suceeded', 'Total' ):
#        # Sometimes it returns this element, which we do not want
#        continue
#       
#      values = elementDict.values()
#      
#      if values:
#        qualityMean[ element ] = sum( values ) / len( values )        
##      else:     
##        qualityMean[ element ] = 0   
#           
#    return S_OK( qualityMean )  
#
#################################################################################
#################################################################################
#
##FIXME: the same data can be obtained with TransferQualityCommand and direction = Destination
#class TransferQualityChannelCommand( Command ):
#
#  def __init__( self, args = None, clients = None ):
#    
#    super( TransferQualityChannelCommand, self ).__init__( args, clients )
#    
#    if 'ReportsClient' in self.apis:
#      self.rClient = self.apis[ 'ReportsClient' ]
#    else:
#      self.rClient = ReportsClient() 
#
#    if 'ReportGenerator' in self.apis:
#      self.rgClient = self.apis[ 'ReportGenerator' ]
#    else:
#      self.rgClient = RPCClient( 'Accounting/ReportGenerator' )       
#    
#    self.rClient.rpcClient = self.rgClient  
#
#  def doCommand( self ):
#
#    if not 'hours' in self.args:
#      return self.returnERROR( S_ERROR( 'Number of hours not specified' ) )
#    hours = self.args[ 'hours' ]
#
#    if not 'element' in self.args:
#      return self.returnERROR( S_ERROR( 'element is missing' ) )
#    element = self.args[ 'element' ]
#
#    if element not in [ 'Site', 'Resource' ]:
#      return self.returnERROR( S_ERROR( 'element is not Site nor Resource' ) )
#
#    if not 'name' in self.args:
#      return self.returnERROR( S_ERROR( 'name is missing' ) )
#    name = self.args[ 'name' ]
#    
#    # If name is None, we take all Sites or StorageElements
#    if name is None:
#      
#      if element == 'Site':
#        name = CSHelpers.getSites()
#      else:  
#        name = CSHelpers.getStorageElements()
#              
#      if not name[ 'OK' ]:
#        return name
#      name = name[ 'Value' ]    
#
#    if not isinstance( name, list ):
#      name = list( name )
#
#    fromD = datetime.utcnow() - timedelta( hours = hours )
#    toD   = datetime.utcnow()
#
#    qualityResults = self.rClient.getReport( 'DataOperation', 'Quality', fromD, toD,
#                                             { 'OperationType' : 'putAndRegister',
#                                               'Destination'   : name }, 
#                                             'Channel' )
#    if not qualityResults[ 'OK' ]:
#      return self.returnERROR( qualityResults )
#    qualityResults = qualityResults[ 'Value' ]
#    
#    if not 'data' in qualityResults:
#      return self.returnERROR( S_ERROR( 'Missing data key' ) )
#    qualityResults = qualityResults[ 'data' ]
#
#    qualityMean = {}
#
#    for channel, quality in qualityResults.values():
#
#      _source, destination = channel.split( ' -> ' )
#      if not destination in name:
#        # If we get a destination we do not have as input, we ignore it
#        continue      
#      if not destination in qualityMean:
#        qualityMean[ destination ] = []
#       
#      qualityMean[ destination ].extend( quality.values() )
#           
#    for destination, values in qualityMean.items():
#      
#      if values:
#        qualityMean[ destination ] = sum( values ) / len( values )        
##      else:     
##        qualityMean[ destination ] = 0   
#           
#    return S_OK( qualityMean )   
#
#################################################################################
#
#class TransferCacheCommand( Command ):
#  
#  def __init__( self, args = None, clients = None ):
#    
#    super( TransferCacheCommand, self ).__init__( args, clients )
#    
#    if 'ResourceManagementClient' in self.apis:
#      self.rmClient = self.apis[ 'ResourceManagementClient' ]
#    else:
#      self.rmClient = ResourceManagementClient()   
#
#  def doCommand( self ):
#
#    if not 'direction' in self.args:
#      return self.returnERROR( S_ERROR( 'element is missing' ) )
#    direction = self.args[ 'direction' ]
#
#    if direction not in [ 'Source', 'Destination' ]:
#      return self.returnERROR( S_ERROR( 'direction is not Source nor Destination' ) )
#
#    if not 'name' in self.args:
#      return self.returnERROR( S_ERROR( 'name is missing' ) )
#    name = self.args[ 'name' ]
#
#    if not 'metric' in self.args:
#      return self.returnERROR( S_ERROR( 'metric is missing' ) )
#    metric = self.args[ 'metric' ]
#
#    meta = { 'columns' : [ 'Value' ] }
#    resQuery = self.rmClient.selectTransferCache( elementName = name, 
#                                                  direction = direction,
#                                                  metric = metric, meta = meta )
#    
#    if not resQuery[ 'OK' ]:
#      return self.returnERROR( resQuery )
#    
#    return resQuery
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF