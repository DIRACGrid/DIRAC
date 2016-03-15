''' EmailAgent

  This agent reads a cache file ( cache.json ) which contains the aggregated information
  of what happened to the elements of each site. After reading the cache file
  ( by default every 30 minutes ) it sends an email for every site and then clears it.

'''


import os
import json
from DIRAC                                                       import S_OK, S_ERROR
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
    self.cacheFile = None
    self.cacheFile = os.getenv('DIRAC') + 'work/ResourceStatus/cache.json'

  def initialize( self ):
    ''' EmailAgent initialization
    '''

    self.diracAdmin = DiracAdmin()

    return S_OK()

  def execute( self ):

    #if the file exists and it is not empty
    if os.path.isfile(self.cacheFile) and os.stat(self.cacheFile).st_size:

      #rename the file and work with it in order to avoid race condition
      os.rename(self.cacheFile, self.cacheFile + ".send")
      self.cacheFile += ".send"

      #load the file
      with open(self.cacheFile, 'r') as f:
        new_dict = json.load(f)

      #read all the name elements of a site
      for site in new_dict:
        subject = "RSS actions taken for " + site
        body = self._emailBodyGenerator(new_dict, site)
        self._sendMail(subject, body)
        self._deleteCacheFile()

    return S_OK()


  def _emailBodyGenerator(self, site_dict, siteName):
    ''' Returns a string with all the elements that have been banned from a given site.
    '''

    if site_dict:

      #if the site's name is in the file
      if siteName in site_dict:
        #read all the name elements of a site
        email_body = ""
        for data in site_dict[siteName]:
          email_body += data['statusType'] + " of " + data['name'] + " has been " + \
                        data['status'] + " since " + data['time'] + \
                        " (Previous status: " + data['previousStatus'] + ")\n"

        return email_body

    else:
      return S_ERROR("Site dictionary is empty")

  def _deleteCacheFile(self):
    ''' Deletes the cache file
    '''
    try:
      os.remove(self.cacheFile)
      return S_OK()
    except OSError as e:
      return S_ERROR("Error %s" % repr(e))

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
