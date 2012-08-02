# $HeadURL:  $
''' TokenAgent

  This agent inspect all elements, and resets their tokens if necessary.

'''

from datetime import datetime, timedelta

from DIRAC                                                      import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.FrameworkSystem.Client.NotificationClient            import NotificationClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
#from DIRAC.ResourceStatusSystem.PolicySystem.PDP                import PDP
#from DIRAC.ResourceStatusSystem.Utilities                       import RssConfiguration 

from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

__RCSID__  = '$Id: $'
AGENT_NAME = 'ResourceStatus/TokenAgent'

class TokenAgent( AgentModule ):
  '''
    TokenAgent is in charge of checking tokens assigned on resources.
    Notifications are sent to those users owning expiring tokens.
  '''

  # Hours to notify a user
  __notifyHours = 12

  # Rss token
  __rssToken = 'rs_svc'
  
  # Admin mail
  __adminMail = None

  def __init__( self, agentName, baseAgentName = False, properties = dict() ):
    
    AgentModule.__init__( self, agentName, baseAgentName, properties ) 
    
    self.notifyHours = self.__notifyHours
    self.adminMail   = self.__adminMail
    
    self.rsClient = None  
    self.rmClient = None
    self.noClient = None
    
    self.tokenDict  = None
    self.diracAdmin = None

  def initialize( self ):
    '''
    TokenAgent initialization
    '''

    self.notifyHours = self.am_getOption( 'notifyHours', self.notifyHours )

    self.rsClient = ResourceStatusClient()
    self.rmClient = ResourceManagementClient()
    self.noClient = NotificationClient()

    self.diracAdmin = DiracAdmin()

    return S_OK()

  def execute( self ):
    '''
      Looks for user tokens. If they are expired, or expiring, it notifies users.
    '''

    # Initialized here, as it is needed empty at the beginning of the execution
    self.tokenDict  = {}
    
    #FIXME: probably this can be obtained from RssConfiguration instead
    elements = ( 'Site', 'Resource', 'Node' )

    for element in elements:

      self.log.info( 'Processing %s' % element )

      interestingTokens = self._getInterestingTokens( element )
      if not interestingTokens[ 'OK' ]:
        self.log.error( interestingTokens[ 'Message' ] )
        continue
      interestingTokens = interestingTokens[ 'Value' ]
      
      processTokens = self._processTokens( element, interestingTokens )
      if not processTokens[ 'OK' ]:
        self.log.error( processTokens[ 'Message' ] )
        continue
    
    notificationResult = self._notifyOfTokens()
    if not notificationResult[ 'OK' ]:
      self.log.error( notificationResult[ 'Message' ] )  
      
    return S_OK()

  ## Protected methods #########################################################

  def _getInterestingTokens( self, element ):
    '''
      Given an element, picks all the entries with TokenExpiration < now + X<hours>
      If the TokenOwner is not the rssToken ( rs_svc ), it is selected.
    '''
    
    tokenExpLimit = datetime.utcnow() + timedelta( hours = self.notifyHours )
    
    tokenElements = self.rsClient.selectStatusElement( element, 'Status',
                                                       meta = { 'older' : ( 'TokenExpiration', tokenExpLimit ) } )
    
    if not tokenElements[ 'OK' ]:
      return tokenElements
    
    tokenColumns  = tokenElements[ 'Columns' ]
    tokenElements = tokenElements[ 'Value' ]
    
    interestingTokens = []
    
    for tokenElement in tokenElements:
      
      tokenElement = dict( zip( tokenColumns, tokenElement ) )
      
      if tokenElement[ 'TokenOwner' ] != self.__rssToken:
        interestingTokens.append( tokenElement )
          
    return S_OK( interestingTokens )

  def _processTokens( self, element, tokenElements ):
    '''
      Given an element and a list of interesting token elements, updates the
      database if the token is expired, logs a message and adds
    '''
    
    never = datetime.max
    
    for tokenElement in tokenElements:
      
      try:
        name            = tokenElement[ 'Name' ]
        statusType      = tokenElement[ 'StatusType' ]
        status          = tokenElement[ 'Status' ]        
        tokenOwner      = tokenElement[ 'TokenOwner' ]
        tokenExpiration = tokenElement[ 'TokenExpiration' ]
      except KeyError, e:
        return S_ERROR( e )  
        
      # If token has already expired  
      if tokenExpiration < datetime.utcnow():    
        _msg = '%s with statusType "%s" and owner %s EXPIRED'
        self.log.info( _msg % ( name, statusType, tokenOwner ) )
        
        result = self.rsClient.addOrModifyStatusElement( element, 'Status', name = name, 
                                                         statusType = statusType, 
                                                         tokenOwner = self.__rssToken, 
                                                         tokenExpiration = never )
        if not result[ 'OK' ]:
          return result
    
      else:  
        _msg = '%s with statusType "%s" and owner %s -> %s'     
        self.log.info( _msg % ( name, statusType, tokenOwner, tokenExpiration ) )
 
      if not tokenOwner in self.tokenDict:
        self.tokenDict[ tokenOwner ] = []
        
      self.tokenDict[ tokenOwner ].append( [ tokenOwner, element, name, statusType, status, tokenExpiration ] )  
                  
    return S_OK()

  def _notifyOfTokens( self ):
    '''
      Splits interesing tokens between expired and expiring. Also splits them
      among users. It ends sending notifications to the users.
    '''
       
    now = datetime.utcnow()   
    
    adminExpired  = []   
    adminExpiring = []   
       
    for tokenOwner, tokenLists in self.tokenDict.items():
      
      expired  = []
      expiring = []     
      
      for tokenList in tokenLists:
       
        if tokenList[ 4 ] < now:
          expired.append( tokenList )
          adminExpired.append( tokenList )
        else:
          expiring.append( tokenList )     
          adminExpiring.append( tokenList )

      resNotify = self._notify( tokenOwner, expired, expiring )
      if not resNotify[ 'OK' ]:
        self.log.error( resNotify[ 'Message' ] )
    
    if adminExpired or adminExpiring:
      return self._notify( self.__adminMail, adminExpired, adminExpiring )
    
    return S_OK()                     
      
  def _notify( self, tokenOwner, expired, expiring ):
    '''
      Given a token owner and a list of expired and expiring tokens, sends an
      email to the user.
    '''
    
    subject = 'RSS token summary for tokenOwner %s' % tokenOwner
    
    mail = '\nEXPIRED tokens ( RSS has taken control of them )\n'
    for tokenList in expired:
      
      mail += ' '.join( tokenList )
      mail += '\n'
            
    mail = '\nEXPIRING tokens ( RSS will taken control of them )\n'
    for tokenList in expiring:
      
      mail += ' '.join( tokenList )
      mail += '\n'

    #FIXME: you can re-take control of them using this or that...
    
    resEmail = self.diracAdmin.sendMail( tokenOwner, subject, mail )
    if not resEmail[ 'OK' ]:
      return S_ERROR( 'Cannot send email to user "%s"' % tokenOwner )  

    return resEmail
  
#  def execute2( self ):
#    '''
#      The main TokenAgent execution method.
#      Checks for tokens owned by users that are expiring, and notifies those users.
#      Calls rsClient.setToken() to set 'RS_SVC' as owner for those tokens that expired.
#    '''
#
#    adminMail = ''
#
#    try:
#
#      reason = 'Out of date token'
#
#      #reAssign the token to RS_SVC
#      #for g in self.ELEMENTS:
#
#      validElements = RssConfiguration.getValidElements()
#
#      for granularity in validElements:
#        tokensExpired = self.rsClient.getTokens( granularity, 
#                                                 tokenExpiration = datetime.datetime.utcnow() )
#
#        if tokensExpired[ 'Value' ]:
#          adminMail += '\nLIST OF EXPIRED %s TOKENS\n' % granularity
#          adminMail += '%s|%s|%s\n' % ( 'user'.ljust(20), 'name'.ljust(15), 'status type')
#
#        for token in tokensExpired[ 'Value' ]:
#
#          name  = token[ 1 ]
#          stype = token[ 2 ]
#          user  = token[ 9 ]
#
#          self.rsClient.setToken( granularity, name, stype, reason, 'RS_SVC', 
#                                  datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )
#          adminMail += ' %s %s %s\n' %( user.ljust(20), name.ljust(15), stype )
#
#      #notify token owners
#      inNHours = datetime.datetime.utcnow() + datetime.timedelta( hours = self.notifyHours )
#      #for g in self.ELEMENTS:
#      for granularity in validElements:
#
#        tokensExpiring = self.rsClient.getTokens( granularity, tokenExpiration = inNHours )
#
#        if tokensExpiring[ 'Value' ]:
#          adminMail += '\nLIST OF EXPIRING %s TOKENS\n' % granularity
#          adminMail += '%s|%s|%s\n' % ( 'user'.ljust(20),'name'.ljust(15),'status type')
#
#        for token in tokensExpiring[ 'Value' ]:
#
#          name  = token[ 1 ]
#          stype = token[ 2 ]
#          user  = token[ 9 ]
#
#          adminMail += '\n %s %s %s\n' %( user.ljust(20), name.ljust(15), stype )
#
#          #If user is RS_SVC, we ignore this, whenever the token is out, this
#          #agent will set again the token to RS_SVC
#          if user == 'RS_SVC':
#            continue
#
#          pdp = PDP( granularity = granularity, name = name, statusType = stype )
#
#          decision = pdp.takeDecision()
#          pcresult = decision[ 'PolicyCombinedResult' ]
#          spresult = decision[ 'SinglePolicyResults' ]
#
#          expiration = token[ 10 ]
#
#          mailMessage = "The token for %s %s ( %s )" % ( granularity, name, stype )
#          mailMessage = mailMessage + " will expire on %s\n\n" % expiration
#          mailMessage = mailMessage + "You can renew it with command 'dirac-rss-renew-token'.\n"
#          mailMessage = mailMessage + "If you don't take any action, RSS will take control of the resource.\n\n"
#
#          policyMessage = ''
#
#          if pcresult[ 'Action' ]:
#
#            policyMessage += "  Policies applied will set status to %s.\n" % pcresult[ 'Status' ]
#
#            for spr in spresult:
#              policyMessage += "    %s Status->%s\n" % ( spr[ 'PolicyName' ].ljust(25), spr[ 'Status' ] )
#
#          mailMessage += policyMessage
#          adminMail   += policyMessage
#
#          self.noClient.sendMail( self.rmClient.getUserRegistryCache( user )[ 'Value' ][ 0 ][ 2 ],
#                            'Token for %s is expiring' % name, mailMessage )
#      if adminMail != '':
#        #FIXME: 'ubeda' is not generic ;p
#        self.noClient.sendMail( self.rmClient.getUserRegistryCache( 'ubeda' )[ 'Value' ][ 0 ][ 2 ],
#                            "Token's summary", adminMail )
#
#      return S_OK()
#
#    except Exception:
#      errorStr = "TokenAgent execution"
#      self.log.exception( errorStr )
#      return S_ERROR( errorStr )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF