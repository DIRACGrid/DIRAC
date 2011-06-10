"""
AlarmPolType Actions
"""

from DIRAC.ResourceStatusSystem.Utilities import CS
from DIRAC.ResourceStatusSystem.Utilities import Utils

def getUsersToNotify(setup, kwargs):
  """Get a list of users to notify (helper function for AlarmPolTypeActions)
  Optional keyword arguments:
  - Granularity
  - SiteType
  - ServiceType
  - ResourceType
  """

  notifications = []
  groups = CS.getTypedDict("AssigneeGroups/" + setup)

  for k in groups:
    if Utils.dictMatch(kwargs, groups[k]):
      notifications.append({'Users':groups[k]['Users'],
                            'Notifications':groups[k]['Notifications']})

  return notifications

def AlarmPolTypeActions(name, res, nc, setup, rsDB, **kwargs):
  """ Do actions required to notify users.
  Mandatory keyword arguments:
  - Granularity
  Optional keyword arguments:
  - SiteType
  - ServiceType
  - ResourceType
  """
  # raise alarms, right now makes a simple notification

  if 'Granularity' not in kwargs.keys():
    raise ValueError, "You have to provide a argument Granularity=<desired_granularity>"

  granularity = kwargs['Granularity']

  if res['Action']:

    notif = "%s %s is perceived as" % (granularity, name)
    notif = notif + " %s. Reason: %s." %(res['Status'], res['Reason'])

    NOTIF_D = getUsersToNotify(setup, kwargs)

    for notification in NOTIF_D:
      for user in notification['Users']:
        if 'Web' in notification['Notifications']:
          nc.addNotificationForUser(user, notif)
        if 'Mail' in notification['Notifications']:
          mailMessage = "Granularity = %s \n" % granularity
          mailMessage = mailMessage + "Name = %s\n" % name
          mailMessage = mailMessage + "New perceived status = %s\n" % res['Status']
          mailMessage = mailMessage + "Reason for status change = %s\n" % res['Reason']

          was = rsDB.getMonitoredsHistory(granularity,
                                          ['Status', 'Reason', 'DateEffective'],
                                          name, False, 'DESC', 1)[0]

          mailMessage = mailMessage + "Was in status \"%s\", " % (was[0])
          mailMessage = mailMessage + "with reason \"%s\", since %s\n" % (was[1], was[2])

          mailMessage = mailMessage + "Setup = %s\n" % setup

          nc.sendMail(CS.getMailForUser(user)['Value'][0],
                      '%s: %s' % (name, res['Status']), mailMessage)
