# $HeadURL:  $
''' SMSAction

'''

from DIRAC                                                      import S_ERROR, S_OK
#from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction

__RCSID__ = '$Id:  $'

class SMSAction( BaseAction ):

  def __init__( self, decissionParams, enforcementResult, singlePolicyResults, clients = None ):
    
    super( SMSAction, self ).__init__( decissionParams, enforcementResult, 
                                       singlePolicyResults, clients )
    self.actionName = 'SMSAction'
    
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
      
    text = '%s %s is %s ( %s )' % ( name, statusType, status, reason )   
    print text
    
    address = ''
    return self._sendSMS( address, '' )    

  @staticmethod
  def _sendSMS( address, text ):
    
    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
    diracAdmin = DiracAdmin()
    
    return diracAdmin.sendSMS( address, text )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    