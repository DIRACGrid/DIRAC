################################################################################
# $HeadURL $
################################################################################
"""
  AlarmPolType Actions
"""

__RCSID__  = "$Id$"

from DIRAC.ResourceStatusSystem.Utilities                       import CS
from DIRAC.ResourceStatusSystem.Utilities                       import Utils
from DIRAC.FrameworkSystem.Client.NotificationClient            import NotificationClient
from DIRAC.ResourceStatusSystem.PolicySystem.Actions.ActionBase import ActionBase
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

class AlarmAction(ActionBase):
  def __init__(self, granularity, name, status_type, pdp_decision, **kw):
    ActionBase.__init__(self)

    try:
      self.rsAPI = self.kw["Clients"][ 'ResourceStatusClient' ]
    except ValueError:
      self.rsAPI = ResourceStatusClient()
    try:
      self.rmAPI = self.kw["Clients"][ 'ResourceManagementClient' ]
    except ValueError:
      self.rmAPI = ResourceManagementClient()


  def _getUsersToNotify(self):
    groups = CS.getTypedDictRootedAt("AssigneeGroups/" + CS.getSetup()).values()
    concerned_groups = [g for g in groups if Utils.dictMatch(self.kw["Params"], g)]
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

    # Initializing variables
    nc = NotificationClient()

    # raise alarms, right now makes a simple notification

    if 'Granularity' not in self.kw.keys():
      raise ValueError, "You have to provide a argument Granularity = <desired_granularity>"

    if self.new_status['Action']:

      notif = "%s %s is perceived as" % (self.granularity, self.name)
      notif = notif + " %s. Reason: %s." % (self.new_status['Status'], self.new_status['Reason'])

      users_to_notify = self._getUsersToNotify()

      for notif in users_to_notify:
        for user in notif['Users']:
          if 'Web' in notif['Notifications']:
            nc.addNotificationForUser(user, notif)
          if 'Mail' in notif['Notifications']:

            kwargs = { self.granularity+'Name' : self.name,
                       'statusType'            : self.status_type,
                       'columns'               : ['Status', 'Reason', 'DateEffective'],
                       'order'                 : 'DESC',
                       'limit'                 : 1 }

            was = Utils.unpack(self.rsAPI.getElementHistory( self.granularity, **kwargs )[0])

            mailMessage = """Granularity = %s
Name = %s
New perceived status = %s
Reason for status change = %s
Was in status "%s", with reason "%s", since %s
Setup = %s
""" % (self.granularity, self.name, self.new_status['Status'],
       self.new_status['Reason'], was[0], was[1], was[2], CS.getSetup())

            # Actually send the mail!
            nc.sendMail(Utils.unpack(rmAPI.getUserRegistryCache(user)[0][2]),
                        '[RSS] Status change for site %s: %s -> %s'
                        % (self.name,  self.new_status['Status'], was[0]), mailMessage)


################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
