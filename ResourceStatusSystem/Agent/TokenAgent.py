########################################################################
# $HeadURL:  $
########################################################################
""" 
TokenAgent is in charge of checking tokens assigned on resources.
Notifications are sent to those users owning expiring tokens. 
"""

import datetime

from DIRAC                                           import S_OK, S_ERROR
from DIRAC                                           import gLogger
from DIRAC.Core.Base.AgentModule                     import AgentModule
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB  import ResourceStatusDB
from DIRAC.ResourceStatusSystem.Utilities.CS         import getMailForUser
from DIRAC.ResourceStatusSystem.PolicySystem.PDP     import PDP 
from DIRAC.ResourceStatusSystem.Utilities.CS         import getExt
      
__RCSID__ = "$Id:  $"

AGENT_NAME = 'ResourceStatus/TokenAgent'


class TokenAgent( AgentModule ):

#############################################################################

  def initialize( self ):
    """ 
    TokenAgent initialization
    """
    
    self.ELEMENTS    = [ 'Site', 'StorageElementRead', 'StorageElementWrite' ]
    self.notifyHours = self.am_getOption( 'notifyHours', 10 )
    
    try:
      self.rsDB  = ResourceStatusDB()
      self.nc    = NotificationClient()
      self.VOExt = getExt()
      
      return S_OK()
    except Exception:
      errorStr = "TokenAgent initialization"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )


#############################################################################

  def execute( self ):
    """ 
    The main TokenAgent execution method.
    Checks for tokens owned by users that are expiring, and notifies those users.
    Calls rsDB.setToken() to set 'RS_SVC' as owner for those tokens that expired.
    """
    
    adminMail = ''
    
    try:
      
      #reAssign the token to RS_SVC
      for g in self.ELEMENTS:
        tokensExpired = self.rsDB.getTokens( g, None, datetime.datetime.utcnow() )
        
        if tokensExpired:
          adminMail += '\nLIST OF EXPIRED TOKENS\n'
                
        for token in tokensExpired:
          
          name = token[ 0 ]
          user = token[ 1 ]
          
          self.rsDB.setToken( g, name, 'RS_SVC', datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )
          adminMail += ' %s %s\n' %( user.ljust(20), name )

      #notify token owners
      inNHours = datetime.datetime.utcnow() + datetime.timedelta( hours = self.notifyHours )
      for g in self.ELEMENTS:
          
        tokensExpiring = self.rsDB.getTokens( g, None, inNHours )
        
        if tokensExpiring:
          adminMail += '\nLIST OF EXPIRING TOKENS\n'
                  
        for token in tokensExpiring:
          
          name = token[ 0 ]
          user = token[ 1 ]
          
          adminMail += '\n %s %s\n' %( user.ljust(20), name )
          
          if user == 'RS_SVC':
            continue
          
          pdp = PDP( self.VOExt, granularity = g, name = name )
          
          decision = pdp.takeDecision()
          pcresult = decision[ 'PolicyCombinedResult' ]
          spresult = decision[ 'SinglePolicyResults' ]
       
          expiration = token[ 2 ]
          
          mailMessage = "The token for %s %s " % ( g, name )
          mailMessage = mailMessage + "will expire on %s\n\n" % expiration
          mailMessage = mailMessage + "You can renew it with command 'dirac-rss-renew-token'.\n"
          mailMessage = mailMessage + "If you don't take any action, RSS will take control of the resource.\n\n"
       
          policyMessage = ''
       
          if pcresult:
       
            policyMessage += "  Policies applied will set status to %s.\n" % pcresult[ 'Status' ]
       
            for spr in spresult:
              policyMessage += "    %s Status->%s\n" % ( spr[ 'PolicyName' ].ljust(25), spr[ 'Status' ] )
       
          mailMessage += policyMessage
          adminMail   += policyMessage
          
          self.nc.sendMail( getMailForUser( user )[ 'Value' ][ 0 ], 
                            'Token for %s is expiring' % name, mailMessage )

      if adminMail != '':
        self.nc.sendMail( getMailForUser( 'ubeda' )[ 'Value' ][ 0 ], 
                            "Token's summary", adminMail )

      return S_OK()
    
    except Exception:
      errorStr = "TokenAgent execution"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

#############################################################################