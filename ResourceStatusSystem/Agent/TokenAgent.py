########################################################################
# $HeadURL:  $
########################################################################
""" 
TokenAgent is in charge of checking tokens assigned on resources.
Notifications are sent to those users owning expiring tokens. 
"""

import datetime

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
from DIRAC.ResourceStatusSystem.Utilities.CS import getMailForUser
      
__RCSID__ = "$Id:  $"

AGENT_NAME = 'ResourceStatus/TokenAgent'


class TokenAgent( AgentModule ):

#############################################################################

  def initialize( self ):
    """ 
    TokenAgent initialization
    """
    
    self.ELEMENTS = [ 'Site', 'StorageElementRead', 'StorageElementWrite' ]
    
    try:
      self.rsDB = ResourceStatusDB()
      self.nc = NotificationClient()
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
    
    try:
      
      #reAssign the token to RS_SVC
      for g in self.ELEMENTS:
        tokensExpired = self.rsDB.getTokens( g, None, datetime.datetime.utcnow() )
        for token in tokensExpired:
          self.rsDB.setToken( g, token[ 0 ], 'RS_SVC', datetime.datetime( 9999, 12, 31, 23, 59, 59 ) )

      #notify token owners
      in2Hours = datetime.datetime.utcnow() + datetime.timedelta( hours = 2 )
      for g in self.ELEMENTS:
          
        tokensExpiring = self.rsDB.getTokens( g, None, in2Hours )
        for token in tokensExpiring:
          name = token[ 0 ]
          user = token[ 1 ]
          if user == 'RS_SVC':
            continue
          expiration = token[ 2 ]
          
          mailMessage = "The token for %s %s " % ( g, name )
          mailMessage = mailMessage + "will expire on %s\n\n" % expiration
          mailMessage = mailMessage + "You can renew it with command 'dirac-rss-renew-token'.\n"
          mailMessage = mailMessage + "If you don't take any action, RSS will take control of the resource."
          
          self.nc.sendMail( getMailForUser( user )[ 'Value' ][ 0 ], 
                            'Token for %s is expiring' % name, mailMessage )

      return S_OK()
    
    except Exception:
      errorStr = "TokenAgent execution"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

#############################################################################