# $HeadURL:  $
''' TransferQualityCommand module
'''

from datetime                                     import datetime, timedelta

from DIRAC                                        import S_OK, S_ERROR
from DIRAC.AccountingSystem.Client.ReportsClient  import ReportsClient
from DIRAC.Core.DISET.RPCClient                   import RPCClient
from DIRAC.ResourceStatusSystem.Command.Command   import Command
from DIRAC.ResourceStatusSystem.Utilities         import CSHelpers

__RCSID__ = '$Id:  $'  

class TransferQualitySEsCommand( Command ):

  def __init__( self, args = None, clients = None ):
    
    super( TransferQualitySEsCommand, self ).__init__( args, clients )
    
    if 'ReportsClient' in self.apis:
      self.rClient = self.apis[ 'ReportsClient' ]
    else:
      self.rClient = ReportsClient() 

    if 'ReportGenerator' in self.apis:
      self.rgClient = self.apis[ 'ReportGenerator' ]
    else:
      self.rgClient = RPCClient( 'Accounting/ReportGenerator' )       
    
    self.rClient.rpcClient = self.rgClient  

  def doCommand( self ):
    """ 
#    Returns transfer quality using the DIRAC accounting system for every SE 
#        
#    :params:
#      :attr:`SEs`: list of storage elements (when not given, take every SE)
#    
#    :returns:
#      {'SiteName': {TQ : 'Good'|'Fair'|'Poor'|'Idle'|'Bad'} ...}
    """

    if not 'hours' in self.args:
      return S_ERROR( 'Number of hours not specified' )
    hours = self.args[ 'hours' ]

    if not 'seName' in self.args:
      return S_ERROR( 'seName is missing' )
    seName = self.args[ 'seName' ]
    
    # If seName is None, we take all StorageElements
    if seName is None:
      seName = CSHelpers.getStorageElements()      
      if not seName[ 'OK' ]:
        return seName
      seName = seName[ 'Value' ]    

    if not isinstance( seName, list ):
      seName = list( seName )

    fromD = datetime.utcnow() - timedelta( hours = hours )
    toD   = datetime.utcnow()

    qualityResults = self.rClient.getReport( 'DataOperation', 'Quality', fromD, toD,
                                             { 'OperationType' : 'putAndRegister',
                                               'Destination'   : seName }, 
                                             'Channel' )
    if not qualityResults[ 'OK' ]:
      return qualityResults
    qualityResults = qualityResults[ 'Value' ]
    
    if not 'data' in qualityResults:
      return S_ERROR( 'Missing data key' )
    qualityResults = qualityResults[ 'data' ]

    qualityMean = {}

    for channel, quality in qualityResults.values():

      _source, destination = channel.split( ' -> ' )
      if not destination in seName:
        # If we get a destination we do not have as input, we ignore it
        continue      
      if not destination in qualityMean:
        qualityMean[ destination ] = []
       
      qualityMean[ destination ].extend( quality.values() )
           
    for destination, values in qualityMean.items():
      
      if values:
        qualityMean[ destination ] = sum( values ) / len( values )        
      else:     
        qualityMean[ destination ] = 0   
           
    return S_OK( qualityMean )  
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF