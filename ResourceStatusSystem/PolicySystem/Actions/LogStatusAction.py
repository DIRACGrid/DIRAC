# $HeadURL:  $
''' LogStatusAction

'''

from DIRAC                                                      import S_ERROR
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction

__RCSID__ = '$Id:  $'

class LogStatusAction( BaseAction ):

  def __init__( self, decissionParams, enforcementResult, singlePolicyResults ):
    
    super( LogStatusAction, self ).__init__( decissionParams, enforcementResult, singlePolicyResults )
    self.actionName = 'LogStatusAction'
    
    self.rsClient   = ResourceStatusClient()

  def run( self ):
    
    # Minor security checks
    
    element = self.decissionParams[ 'element' ]
    if element is None:
      return S_ERROR( 'element should not be None' )
    
    name = self.decissionParams[ 'name' ] 
    if name is None:
      return S_ERROR( 'name should not be None' )
    
    statusType = self.decissionParams[ 'statusType' ]
    if statusType is None:
      return S_ERROR( 'statusType should not be None' )
    
    status = self.decissionParams[ 'status' ]    
    if status is None:
      return S_ERROR( 'status should not be None' )
    
    elementType = self.decissionParams[ 'elementType' ]
    if elementType is None:
      return S_ERROR( 'elementType should not be None' )
    
    reason = self.decissionParams[ 'reason' ]
    if reason is None:
      return S_ERROR( 'reason should not be None' )

#    newStatus = self.enforcementResult[ 'Status' ]
#    if newStatus == status:
    
    resLogUpdate = self.rsClient.addOrModifyStatusElement( element, 'Status',
                                                           name = name, statusType = statusType,
                                                           status = status, elementType = elementType,
                                                           reason = reason     
                                                           )
    
    return resLogUpdate   

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF