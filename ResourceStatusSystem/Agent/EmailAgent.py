''' EmailAgent

  This agent reads a cache file ( cache.json ) which contains the aggregated information
  of what happened to the elements of each site. After reading the cache file
  ( by default every 30 minutes ) it sends an email for every site and then clears it.

'''


import os
import sqlite3
from DIRAC                                                       import gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                                 import AgentModule
from DIRAC.ResourceStatusSystem.Utilities                        import RssConfiguration
from DIRAC.Interfaces.API.DiracAdmin                             import DiracAdmin

__RCSID__ = '$Id: $'

AGENT_NAME = 'ResourceStatus/EmailAgent'

class EmailAgent( AgentModule ):

  def __init__( self, *args, **kwargs ):

    AgentModule.__init__( self, *args, **kwargs )
    self.diracAdmin = None
    self.default_value = None

    if 'DIRAC' in os.environ:
      self.cacheFile = os.path.join( os.getenv('DIRAC'), 'work/ResourceStatus/cache.db' )
    else:
      self.cacheFile = os.path.realpath('cache.db')

  def initialize( self ):
    ''' EmailAgent initialization
    '''

    self.diracAdmin = DiracAdmin()

    return S_OK()

  def execute( self ):

    if os.path.isfile(self.cacheFile):
      with sqlite3.connect(self.cacheFile) as conn:

        result = conn.execute("SELECT DISTINCT SiteName from ResourceStatusCache;")
        for site in result:
          cursor = conn.execute("SELECT StatusType, ResourceName, Status, Time, PreviousStatus from ResourceStatusCache WHERE SiteName='"+ site[0] +"';")

          email_body = "(" + gConfig.getValue('/DIRAC/Setup') + ")\n\n"
          for StatusType, ResourceName, Status, Time, PreviousStatus in cursor:
            email_body += StatusType + " of " + ResourceName + " has been " + Status + " since " + Time + " (Previous status: " + PreviousStatus + ")\n"

          subject = "RSS actions taken for " + site[0] + "\n"
          self._sendMail(subject, email_body)

        conn.execute("DELETE FROM ResourceStatusCache;")
        conn.execute("VACUUM;")

    return S_OK()

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

  def _getUserEmails( self ):

    configResult = RssConfiguration.getnotificationGroups()
    if not configResult[ 'OK' ]:
      return configResult
    try:
      notificationGroups = configResult[ 'Value' ][ 'notificationGroups' ]
    except KeyError:
      return S_ERROR( '%s/notificationGroups not found' )

    notifications = RssConfiguration.getNotifications()
    if not notifications[ 'OK' ]:
      return notifications
    notifications = notifications[ 'Value' ]

    userEmails = []

    for notificationGroupName in notificationGroups:
      try:
        userEmails.extend( notifications[ notificationGroupName ][ 'users' ] )
      except KeyError:
        self.log.error( '%s not present' % notificationGroupName )

    return S_OK( userEmails )

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
