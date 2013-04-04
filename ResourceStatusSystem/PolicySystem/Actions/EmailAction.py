# $HeadURL:  $
''' EmailAction

'''

from DIRAC                                                      import gConfig, gLogger, S_ERROR, S_OK
from DIRAC.Interfaces.API.DiracAdmin                            import DiracAdmin
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.BaseAction import BaseAction
from DIRAC.ResourceStatusSystem.Utilities                       import RssConfiguration
#from DIRAC.ResourceStatusSystem.Utilities.InfoGetter            import InfoGetter

__RCSID__ = '$Id:  $'

class EmailAction( BaseAction ):
  '''
    Action that sends an email with the information concerning the status and 
    the policies run.
  '''
  
  def __init__( self, name, decissionParams, enforcementResult, singlePolicyResults, 
                clients = None ):
    
    super( EmailAction, self ).__init__( name, decissionParams, enforcementResult, 
                                         singlePolicyResults, clients )
    self.diracAdmin = DiracAdmin()

  def run( self ):
    '''
      Checks it has the parameters it needs and tries to send an email to the users
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
    
#    if self.decissionParams[ 'status' ] == status:
#      # If status has not changed, we skip
#      return S_OK()

#    if self.decissionParams[ 'reason' ] == reason:
#      # If reason has not changed, we skip
#      return S_OK()

#    if not set( ( 'Banned', 'Error', 'Unknown' ) ) & set( ( status, self.decissionParams[ 'status' ] ) ):
#      # if not 'Banned', 'Error', 'Unknown' in ( status, self.decissionParams[ 'status' ] ):
#      # not really interesting to send an email
#      return S_OK()
      
    setup = gConfig.getValue( 'DIRAC/Setup' )  
      
    #subject2 = '[%s]%s %s %s is on status %s' % ( setup, element, name, statusType, status )
    subject = '[RSS](%s) %s (%s) %s' % ( setup, name, statusType, self.actionName )
    
    body = 'ENFORCEMENT RESULT\n\n'
    body += '\n'.join( [ '%s : "%s"' % ( key, value ) for key, value in self.enforcementResult.items() ] )
    body += '\n\n'
    body += '*' * 80
    body += '\nORGINAL PARAMETERS\n\n'
    body += '\n'.join( [ '%s : "%s"' % ( key, value ) for key, value in self.decissionParams.items() ] )
    body += '\n\n'
    body += '*' * 80
    body += '\nPOLICIES RUN\n\n'
    
    for policy in self.singlePolicyResults:
      
      body += '\n'.join( [ '%s : "%s"' % ( key, value ) for key, value in policy.items() if not key == 'Policy' ] )
      body += '\n'
      body += '\n'.join( [ '%s : "%s"' % ( key, value ) for key, value in policy[ 'Policy' ].items() ] )
      body += '\n\n'
        
    return self._sendMail( subject, body )

  def _getUserEmails( self ):

    policyActions = RssConfiguration.getPolicyActions()
    if not policyActions[ 'OK' ]:
      return policyActions
    try:
      notificationGroups = policyActions[ 'Value' ][ self.actionName ][ 'notificationGroups' ]
    except KeyError:
      return S_ERROR( '%s/notificationGroups not found' % self.actionName )  
    
    notifications = RssConfiguration.getNotifications()
    if not notifications[ 'OK' ]:
      return notifications
    notifications = notifications[ 'Value' ]  

    userEmails = []
    
    for notificationGroupName in notificationGroups:
      try:
        userEmails.extend( notifications[ notificationGroupName ][ 'users' ] ) 
      except KeyError:
        gLogger.error( '%s not present' % notificationGroupName )

    return S_OK( userEmails )

  def _sendMail( self, subject, body ):
    
    userEmails = self._getUserEmails()
    if not userEmails[ 'OK' ]:
      return userEmails
    
    # User email address used to send the emails from.
    fromAddress = RssConfiguration.RssConfiguration().getConfigFromAddress()
    
    for user in userEmails[ 'Value' ]:
      
      #FIXME: should not I get the info from the RSS User cache ?
      
      resEmail = self.diracAdmin.sendMail( user, subject, body, fromAddress = fromAddress )
      if not resEmail[ 'OK' ]:
        return S_ERROR( 'Cannot send email to user "%s"' % user )    
      
    return S_OK() 
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF