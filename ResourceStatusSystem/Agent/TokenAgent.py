# $HeadURL:  $
''' TokenAgent

  This agent inspect all elements, and resets their tokens if necessary.

'''

import datetime

from DIRAC                                                      import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.FrameworkSystem.Client.NotificationClient            import NotificationClient
from DIRAC.ResourceStatusSystem                                 import ValidRes
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.PolicySystem.PDP                import PDP

__RCSID__  = '$Id: $'
AGENT_NAME = 'ResourceStatus/TokenAgent'

class TokenAgent( AgentModule ):
  '''
    TokenAgent is in charge of checking tokens assigned on resources.
    Notifications are sent to those users owning expiring tokens.
  '''

  # Too many public methods
  # pylint: disable-msg=R0904

  def initialize( self ):
    '''
    TokenAgent initialization
    '''
    
    # Attribute defined outside __init__
    # pylint: disable-msg=W0201

    self.notifyHours = self.am_getOption( 'notifyHours', 10 )

    try:
      self.rsClient = ResourceStatusClient()
      self.rmClient = ResourceManagementClient()
      self.noClient = NotificationClient()

      return S_OK()
    except Exception:
      errorStr = "TokenAgent initialization"
      self.log.exception( errorStr )
      return S_ERROR( errorStr )

  def execute( self ):
    '''
      The main TokenAgent execution method.
      Checks for tokens owned by users that are expiring, and notifies those users.
      Calls rsClient.setToken() to set 'RS_SVC' as owner for those tokens that expired.
    '''

    adminMail = ''

    try:

      reason = 'Out of date token'

      #reAssign the token to RS_SVC
      #for g in self.ELEMENTS:

      for granularity in ValidRes:
        tokensExpired = self.rsClient.getTokens( granularity, 
                                                 tokenExpiration = datetime.datetime.utcnow() )

        if tokensExpired[ 'Value' ]:
          adminMail += '\nLIST OF EXPIRED %s TOKENS\n' % granularity
          adminMail += '%s|%s|%s\n' % ( 'user'.ljust(20), 'name'.ljust(15), 'status type')

        for token in tokensExpired[ 'Value' ]:

          name  = token[ 1 ]
          stype = token[ 2 ]
          user  = token[ 9 ]

          self.rsClient.setToken( granularity, name, stype, reason, 'RS_SVC', 
                                  datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )
          adminMail += ' %s %s %s\n' %( user.ljust(20), name.ljust(15), stype )

      #notify token owners
      inNHours = datetime.datetime.utcnow() + datetime.timedelta( hours = self.notifyHours )
      #for g in self.ELEMENTS:
      for granularity in ValidRes:

        tokensExpiring = self.rsClient.getTokens( granularity, tokenExpiration = inNHours )

        if tokensExpiring[ 'Value' ]:
          adminMail += '\nLIST OF EXPIRING %s TOKENS\n' % granularity
          adminMail += '%s|%s|%s\n' % ( 'user'.ljust(20),'name'.ljust(15),'status type')

        for token in tokensExpiring[ 'Value' ]:

          name  = token[ 1 ]
          stype = token[ 2 ]
          user  = token[ 9 ]

          adminMail += '\n %s %s %s\n' %( user.ljust(20), name.ljust(15), stype )

          #If user is RS_SVC, we ignore this, whenever the token is out, this
          #agent will set again the token to RS_SVC
          if user == 'RS_SVC':
            continue

          pdp = PDP( granularity = granularity, name = name, statusType = stype )

          decision = pdp.takeDecision()
          pcresult = decision[ 'PolicyCombinedResult' ]
          spresult = decision[ 'SinglePolicyResults' ]

          expiration = token[ 10 ]

          mailMessage = "The token for %s %s ( %s )" % ( granularity, name, stype )
          mailMessage = mailMessage + " will expire on %s\n\n" % expiration
          mailMessage = mailMessage + "You can renew it with command 'dirac-rss-renew-token'.\n"
          mailMessage = mailMessage + "If you don't take any action, RSS will take control of the resource.\n\n"

          policyMessage = ''

          if pcresult[ 'Action' ]:

            policyMessage += "  Policies applied will set status to %s.\n" % pcresult[ 'Status' ]

            for spr in spresult:
              policyMessage += "    %s Status->%s\n" % ( spr[ 'PolicyName' ].ljust(25), spr[ 'Status' ] )

          mailMessage += policyMessage
          adminMail   += policyMessage

          self.noClient.sendMail( self.rmClient.getUserRegistryCache( user )[ 'Value' ][ 0 ][ 2 ],
                            'Token for %s is expiring' % name, mailMessage )
      if adminMail != '':
        #FIXME: 'ubeda' is not generic ;p
        self.noClient.sendMail( self.rmClient.getUserRegistryCache( 'ubeda' )[ 'Value' ][ 0 ][ 2 ],
                            "Token's summary", adminMail )

      return S_OK()

    except Exception:
      errorStr = "TokenAgent execution"
      self.log.exception( errorStr )
      return S_ERROR( errorStr )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF