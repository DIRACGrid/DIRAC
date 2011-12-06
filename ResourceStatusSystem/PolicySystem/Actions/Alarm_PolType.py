################################################################################
# $HeadURL $
################################################################################
"""
  AlarmPolType Actions
"""

__RCSID__  = "$Id$"

from DIRAC.ResourceStatusSystem.Utilities            import CS
from DIRAC.ResourceStatusSystem.Utilities            import Utils
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

class AlarmPolType(object):
  def __init__(self, name, res, statusType, clients, **kwargs):
    self.name       = name
    self.res        = res
    self.statusType = statusType
    self.nc         = NotificationClient()

    try:
      self.rsAPI = clients[ 'ResourceStatusClient' ]
    except ValueError:
      self.rsAPI = ResourceStatusClient()
    try:
      self.rmAPI = clients[ 'ResourceManagementClient' ]
    except ValueError:
      self.rmAPI = ResourceManagementClient()

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
            self.nc.addNotificationForUser(user, notif)
          if 'Mail' in notification['Notifications']:

            kwargs = { '%sName'     : self.name,
                       'statusType' : self.statusType,
                       'columns'    : ['Status', 'Reason', 'DateEffective'],
                       'order'      : 'DESC',
                       'limit'      : 1 }

            was = Utils.unpack(self.rsAPI.getElementHistory( granularity, **kwargs )[0])

            mailMessage = """Granularity = %s
Name = %s
New perceived status = %s
Reason for status change = %s
Was in status "%s", with reason "%s", since %s
Setup = %s
""" % (granularity, self.name, self.res['Status'],
       self.res['Reason'], was[0], was[1], was[2], self.setup)

            # Actually send the mail!
            self.nc.sendMail(Utils.unpack(self.rmAPI.getUserRegistryCache(user)[0][2]),
                             '[RSS] Status change for site %s: %s -> %s'
                             % (self.name,  self.res['Status'], was[0]), mailMessage)


################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
