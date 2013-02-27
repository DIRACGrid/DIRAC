# $HeadURL:  $
''' LogPolicyResultAction

'''

from DIRAC                                                      import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction

__RCSID__ = '$Id:  $'

class LogPolicyResultAction( BaseAction ):
  '''
    Action that registers on the database a new entry per policy result in the
    list singlePolicyResults.
  '''

  def __init__( self, name, decissionParams, enforcementResult, singlePolicyResults, 
                clients = None ):
    
    super( LogPolicyResultAction, self ).__init__( name, decissionParams, enforcementResult, 
                                                   singlePolicyResults, clients )

    if clients is not None and 'ResourceManagementClient' in clients:
      self.rmClient = clients[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient()

  def run( self ):
    '''
      Checks it has the parameters it needs and tries to addOrModify in the 
      database.
    '''
  
    element = self.decissionParams[ 'element' ]
    if element is None:
      return S_ERROR( 'element should not be None' )
    
    name = self.decissionParams[ 'name' ] 
    if name is None:
      return S_ERROR( 'name should not be None' )
    
    statusType = self.decissionParams[ 'statusType' ]
    if statusType is None:
      return S_ERROR( 'statusType should not be None' )
    
    for singlePolicyResult in self.singlePolicyResults:
      
      status = singlePolicyResult[ 'Status' ]
      if status is None:
        return S_ERROR( 'status should not be None' )
      
      reason = singlePolicyResult[ 'Reason' ]
      if reason is None:
        return S_ERROR( 'reason should not be None' )
    
      policyName = singlePolicyResult[ 'Policy' ][ 'name' ]
      if policyName is None:
        return S_ERROR( 'policyName should not be None' )
      
      #Truncate reason to fit in database column
      reason = ( reason[ :508 ] + '..') if len( reason ) > 508 else reason
    
      polUpdateRes = self.rmClient.addOrModifyPolicyResult( element = element, 
                                                            name = name,
                                                            policyName = policyName, 
                                                            statusType = statusType, 
                                                            status = status, 
                                                            reason = reason )
      
      if not polUpdateRes[ 'OK' ]:
        return polUpdateRes
    
    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF