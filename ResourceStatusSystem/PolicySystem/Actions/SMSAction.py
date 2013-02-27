# $HeadURL:  $
''' SMSAction

'''

from DIRAC                                                      import S_ERROR, S_OK
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction
#from DIRAC.ResourceStatusSystem.Utilities.InfoGetter            import InfoGetter

__RCSID__ = '$Id:  $'

class SMSAction( BaseAction ):
  '''
    Action that sends a brief SMS to the user with a few keywords that will make
    him run to his or her office.
  '''
  
  def __init__( self, name, decissionParams, enforcementResult, singlePolicyResults, 
                clients = None ):
    
    super( SMSAction, self ).__init__( name, decissionParams, enforcementResult, 
                                       singlePolicyResults, clients )
  def run( self ):
    '''
      Checks it has the parameters it needs and tries to send an sms to the users
      that apply.
    '''    
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

    return self._sendSMS( text )    

  def _sendSMS( self, text ):
    
    #FIXME: implement it !
    return S_ERROR( 'Not implemented yet' ) 
    
#    from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
#    diracAdmin = DiracAdmin()
#    
#    address = InfoGetter().getNotificationsThatApply( self.decissionParams, self.actionName )
#    if not address[ 'OK' ]:
#      return address 
#    address = address[ 'Value' ]
#    
#    for addressDict in address:
#      if not 'name' in addressDict:
#        return S_ERROR( 'Malformed address dict %s' % addressDict ) 
#      if not 'users' in addressDict:
#        return S_ERROR( 'Malformed address dict %s' % addressDict )     
#      
#      for user in addressDict[ 'users' ]:
#      
#        # Where are the SMS numbers defined ?
#      
#        resSMS = diracAdmin.sendSMS( user, text )
#        if not resSMS[ 'OK' ]:
#          return S_ERROR( 'Cannot send SMS to user "%s"' % user )    
#      
#    return resSMS 

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    