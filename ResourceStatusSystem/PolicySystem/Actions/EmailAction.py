# $HeadURL:  $
''' EmailAction

'''

from DIRAC                                                      import S_ERROR, S_OK
#from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction

__RCSID__ = '$Id:  $'

class EmailAction( BaseAction ):

  def __init__( self, decissionParams, enforcementResult, singlePolicyResults ):
    
    super( EmailAction, self ).__init__( decissionParams, enforcementResult, singlePolicyResults )
    self.actionName = 'EmailAction'
    
#    self.rsClient   = ResourceStatusClient()

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
    
    status = self.enforcementResult[ 'Status' ]    
    if status is None:
      return S_ERROR( 'status should not be None' )

    reason = self.enforcementResult[ 'Reason' ]
    if reason is None:
      return S_ERROR( 'reason should not be None' )
    
    if self.decissionParams[ 'status' ] == status:
      # If status has not changed, we skip
      return S_OK()

    if self.decissionParams[ 'reason' ] == reason:
      # If reason has not changed, we skip
      return S_OK()
      
    subject = '%s %s %s is on status %s' % ( element, name, statusType, status )
    
    body = 'Enforcement result/n'
    body += '/n'.join( [ '%s : %s' % ( key, value ) for key, value in self.enforcementResult ] )
    body += '/n'
    body += '*' * 50
    body += '/nOriginal parameters/n'
    body += '/n'.join( [ '%s : %s' % ( key, value ) for key, value in self.decissionParams ] )
    body += '/n'
    body += '*' * 50
    body += '/nPolicies run/n'
    
    for policy in self.singlePolicyResults:
      
      body += '/n'.join( [ '%s : %s' % ( key, value ) for key, value in policy if not key == 'policy' ] )
      body += '/n'.join( [ '%s : %s' % ( key, value ) for key, value in policy[ 'policy' ] ] )
    
    print subject
    print body
    
    return S_OK()    

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF