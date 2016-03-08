''' EmailAgent

  This agent sends an email every 30 minutes with aggregated actions taken for each site

'''


import os
import json
from datetime import datetime
from DIRAC                                                       import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                                 import AgentModule
from DIRAC.ResourceStatusSystem.Utilities                        import RssConfiguration
from DIRAC.Interfaces.API.DiracAdmin                             import DiracAdmin

__RCSID__ = '$Id: $'

AGENT_NAME = 'ResourceStatus/EmailAgent'

class EmailAgent( AgentModule ):

  def __init__( self, *args, **kwargs ):

    AgentModule.__init__( self, *args, **kwargs )
    self.diracAdmin = DiracAdmin()

    self.default_value = None
    self.dirac_path = None
    self.dirac_path = None
    self.cacheFile = None

  def initialize( self ):
    ''' EmailAgent initialization
    '''

    self.default_value = '/opt/dirac/pro/work/ResourceStatus/'
    self.dirac_path = os.getenv('DIRAC', self.default_value)
    self.cacheFile = self.dirac_path + 'cache.json'

    return S_OK()

  def execute( self ):

    #if the file exists and it is not empty
    if (os.path.isfile(self.cacheFile)) and (os.stat(self.cacheFile).st_size > 0):

      time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

      #load the file
      with open(self.cacheFile) as f:
        new_dict = json.load(f)

      #read all the name elements of a site
      for site in new_dict:
        subject = "RSS actions taken for " + site
        body = self._emailBodyGenerator(self.cacheFile, site)
        self._sendMail(subject, body)
        self._removefromJSON(self.cacheFile, site)

    return S_OK()


  def _emailBodyGenerator(self, cache_file, siteName):
    ''' Returns a string with all the elements that have been banned from a given site.
    '''

    if (os.path.isfile(cache_file)) and (os.stat(cache_file).st_size > 0):
      with open(cache_file) as f:
        new_dict = json.load(f)

      #if the site's name is in the file
      if siteName in new_dict:
        #read all the name elements of a site
        email_body = ""
        for data in new_dict[siteName]:
          email_body += data['statusType'] + " of " + data['name'] + " has been " + data['status'] + " since " + data['time'] + " (Previous status: " + data['previousStatus'] + ")\n"

        return email_body

  def _removefromJSON(self, cache_file, siteName):
    ''' Removes a whole site along with its records.
    '''

    try:

      if (os.path.isfile(cache_file)) and (os.stat(cache_file).st_size > 0):
        with open(cache_file) as f:
          new_dict = json.load(f)

        #if the site's name is in the file delete it
        if siteName in new_dict:
          del new_dict[siteName]

          #write the file again with the modified contents
          with open(cache_file, 'w') as f:
            json.dump(new_dict, f)

      return S_OK()

    except:
      return S_ERROR("Could not remove site from cache file")

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
