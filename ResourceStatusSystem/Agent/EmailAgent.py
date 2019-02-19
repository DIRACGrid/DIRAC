''' EmailAgent
  This agent reads a cache file ( cache.db ) which contains the aggregated information
  of what happened to the elements of each site. After reading the cache file
  ( by default every 30 minutes ) it sends an email for every site and then clears it.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN EmailAgent
  :end-before: ##END
  :dedent: 2
  :caption: EmailAgent options
'''

import os
import sqlite3
from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ResourceStatusSystem.Utilities import RssConfiguration
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

__RCSID__ = '$Id$'

AGENT_NAME = 'ResourceStatus/EmailAgent'

class EmailAgent(AgentModule):

  def __init__(self, *args, **kwargs):

    AgentModule.__init__(self, *args, **kwargs)
    self.diracAdmin = None
    self.default_value = None

    if 'DIRAC' in os.environ:
      self.cacheFile = os.path.join(os.getenv('DIRAC'), 'work/ResourceStatus/cache.db')
    else:
      self.cacheFile = os.path.realpath('cache.db')

  def initialize(self):
    ''' EmailAgent initialization
    '''

    self.diracAdmin = DiracAdmin()

    return S_OK()

  def execute(self):

    if os.path.isfile(self.cacheFile):
      with sqlite3.connect(self.cacheFile) as conn:

        result = conn.execute("SELECT DISTINCT SiteName from ResourceStatusCache;")
        for site in result:
          query = "SELECT StatusType, ResourceName, Status, Time, PreviousStatus from ResourceStatusCache "
          query += "WHERE SiteName='%s';" % site[0]
          cursor = conn.execute(query)

          email = ""
          html_body = ""
          html_elements = ""

          if gConfig.getValue('/DIRAC/Setup'):
            setup = "(" + gConfig.getValue('/DIRAC/Setup') + ")\n\n"
          else:
            setup = ""

          html_header = """\
          <!DOCTYPE html>
          <html>
          <head>
          <meta charset='UTF-8'>
            <style>
              table{{color:#333;font-family:Helvetica,Arial,sans-serif;min-width:700px;border-collapse:collapse;border-spacing:0}}
              td,th{{border:1px solid transparent;height:30px;transition:all .3s}}th{{background:#DFDFDF;font-weight:700}}
              td{{background:#FAFAFA;text-align:center}}.setup{{font-size:150%;color:grey}}.Banned{{color:red}}.Error{{color:#8b0000}}
              .Degraded{{color:gray}}.Probing{{color:#00f}}.Active{{color:green}}tr:nth-child(even) td{{background:#F1F1F1}}tr:nth-child(odd)
              td{{background:#FEFEFE}}tr td:hover{{background:#666;color:#FFF}}
            </style>
          </head>
          <body>
            <p class="setup">{setup}</p>
          """.format(setup=setup)

          for StatusType, ResourceName, Status, Time, PreviousStatus in cursor:
            html_elements += "<tr>" + \
                             "<td>" + StatusType + "</td>" + \
                             "<td>" + ResourceName + "</td>" + \
                             "<td class='" + Status + "'>" + Status + "</td>" + \
                             "<td>" + Time + "</td>" + \
                             "<td class='" + PreviousStatus + "'>" + PreviousStatus + "</td>" + \
                             "</tr>"

          html_body = """\
            <table>
              <tr>
                  <th>Status Type</th>
                  <th>Resource Name</th>
                  <th>Status</th>
                  <th>Time</th>
                  <th>Previous Status</th>
              </tr>
              {html_elements}
            </table>
          </body>
          </html>
          """.format(html_elements=html_elements)

          email = html_header + html_body

          subject = "RSS actions taken for " + site[0] + "\n"
          self._sendMail(subject, email, html=True)

        conn.execute("DELETE FROM ResourceStatusCache;")
        conn.execute("VACUUM;")

    return S_OK()

  def _sendMail(self, subject, body, html=False):

    userEmails = self._getUserEmails()
    if not userEmails['OK']:
      return userEmails

    # User email address used to send the emails from.
    fromAddress = RssConfiguration.RssConfiguration().getConfigFromAddress()

    for user in userEmails['Value']:

      # FIXME: should not I get the info from the RSS User cache ?

      resEmail = self.diracAdmin.sendMail(user, subject, body, fromAddress=fromAddress, html=html)
      if not resEmail['OK']:
        return S_ERROR('Cannot send email to user "%s"' % user)

    return S_OK()

  def _getUserEmails(self):

    configResult = RssConfiguration.getnotificationGroups()
    if not configResult['OK']:
      return configResult
    try:
      notificationGroups = configResult['Value']['notificationGroups']
    except KeyError:
      return S_ERROR('%s/notificationGroups not found')

    notifications = RssConfiguration.getNotifications()
    if not notifications['OK']:
      return notifications
    notifications = notifications['Value']

    userEmails = []

    for notificationGroupName in notificationGroups:
      try:
        userEmails.extend(notifications[notificationGroupName]['users'])
      except KeyError:
        self.log.error('%s not present' % notificationGroupName)

    return S_OK(userEmails)
