# $HeadURL:  $
''' JobsClient module
'''

from DIRAC                      import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient

__RCSID__  = '$Id:  $'

class JobsClient( object ):
  ''' 
    JobResultsClient class is a client to get jobs' stats.
  '''
  
  def __init__( self ):
    
    self.gate = RPCClient( 'WorkloadManagement/WMSAdministrator' )

  def getJobsSimpleEff( self, name ):
    '''  
    Return simple jobs efficiency
    
    :Parameters:
      **name** - `string||list`
        site name(s)
    
      **RPCWMSAdmin** - `[,RPCClient]`
        RPCClient to RPCWMSAdmin

    :return: { SiteName : Good | Fair | Poor | Idle | Bad }
    '''
    
    results = self.gate.getSiteSummaryWeb( { 'Site' : name }, [] , 0, 500 )
    
    if not results[ 'OK' ]:
      return results
    
    if not 'Records' in results[ 'Value' ]:
      return S_ERROR( 'No Records key on result' )  
    results = results[ 'Value' ][ 'Records' ]
    
    if len( results ) == 0:
      return S_ERROR( 'No records found' ) 
    
    effRes = {}
    
    for result in results:
      
      name = result[ 0 ]
      
      try:
        #FIXME: WTF is this returning ?
        eff = result[ 16 ]
      except IndexError:
        eff = 'Idle'
        
      effRes[ name ] = eff 
    
    return S_OK( effRes )

#  def getJobsStats(self, name, periods = None):
#    pass

#  def getJobsEff( self, granularity, name, periods ):
#    pass

#  def getSystemCharge(self):
#    pass
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF