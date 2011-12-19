################################################################################
# $HeadURL $
################################################################################
"""
  AlarmPolType Actions
"""

__RCSID__  = "$Id$"

from DIRAC                                                      import gLogger
from DIRAC.ResourceStatusSystem.Utilities                       import CS
from DIRAC.ResourceStatusSystem.Utilities                       import Utils
from DIRAC.FrameworkSystem.Client.NotificationClient            import NotificationClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

class AlarmPolType(object):
  def __init__(self, name, res, statusType, clients, **kwargs):
    self.name       = name
    self.res        = res
    self.statusType = statusType
    self.nc         = NotificationClient()

    try:    self.rsClient = clients[ 'ResourceStatusClient' ]
    except: self.rsClient = ResourceStatusClient()
    try:    self.rmClient = clients[ 'ResourceManagementClient' ]
    except: self.rmClient = ResourceManagementClient()

    self.kwargs     = kwargs
    self.setup      = CS.getSetup()
    self.run()

  def _getUsersToNotify(self):
    groups = CS.getTypedDictRootedAt("AssigneeGroups/" + self.setup).values()
    concerned_groups = [g for g in groups if Utils.dictMatch(self.kwargs, g)]
    return [{'Users':g['Users'],
             'Notifications':g['Notifications']} for g in concerned_groups]

  def run(self):
    """ Do actions required to notify users.
    Mandatory keyword arguments:
    - Granularity
    Optional keyword arguments:
    - SiteType
    - ServiceType
    - ResourceType
    """
    # raise alarms, right now makes a simple notification

    if 'Granularity' not in self.kwargs.keys():
      raise ValueError, "You have to provide a argument Granularity = <desired_granularity>"

    granularity = self.kwargs['Granularity']

    if self.res['Action']:

      notif = "%s %s is perceived as" % (granularity, self.name)
      notif = notif + " %s. Reason: %s." % (self.res['Status'], self.res['Reason'])

      NOTIF_D = self._getUsersToNotify()

      for notification in NOTIF_D:
        for user in notification['Users']:
          if 'Web' in notification['Notifications']:
            gLogger.info("Sending web notification to user %s" % user)
            self.nc.addNotificationForUser(user, notif)
          if 'Mail' in notification['Notifications']:
            gLogger.info("Sending mail notification to user %s" % user)
            was = Utils.unpack(self.rsClient.getElementHistory(
                granularity, elementName=self.name,
                statusType=self.statusType,
                meta = {"order": "DESC", 'limit' : 1,
                        "columns":  ['Status', 'Reason', 'DateEffective']}))[0]

            mailMessage = """RSS changed the status of the following resource:

Granularity:\t%s
Name:\t\t%s
New status:\t%s
Reason:\t\t%s
Was:\t\t%s (%s) since %s
Setup:\t\t%s

If you think RSS took the wrong decision, please set the status manually:

Use: dirac-rss-set-status -g <granularity> -n <element_name> -s <desired_status> [-t status_type]
(if you omit the optional last part of the command, all status types are matched.)

This notification has been sent according to those parameters:
%s
""" % (granularity, self.name, self.res['Status'],
       self.res['Reason'], was[0], was[1], was[2], self.setup, str(NOTIF_D))

            # Actually send the mail!
            self.nc.sendMail(Utils.unpack(self.rmClient.getUserRegistryCache(user))[0][2],
                             '[RSS][%s][%s] %s -> %s'
                             % (granularity, self.name, self.res['Status'], was[0]), mailMessage)


################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
