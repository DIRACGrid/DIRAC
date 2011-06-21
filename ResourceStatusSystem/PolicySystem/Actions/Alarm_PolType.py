"""
AlarmPolType Actions
"""
import urllib

from DIRAC.ResourceStatusSystem.Utilities import CS
from DIRAC.ResourceStatusSystem.Utilities import Utils

def getUsersToNotifyShiftDB():
  url = urllib.urlopen("http://lbshiftdb.cern.ch/shiftdb_report.php")
  lines = [l.split('|')[1:-1] for l in  url.readlines()]
  lines = [ {'Date': e[0].strip(), 'Function': e[1].strip(), 'Phone':e[2].strip(), 'Morning':e[3].strip(),
             'Evening':e[4].strip(), 'Night':e[5].strip() } for e in lines[1:]]

  lines = [ e for e in lines if e['Function'] == "Grid Expert" or e['Function'] == "Production" ]
  # Warning: The getMailForName function might not return what you
  # want! There is no guarantee that it's possible to correctly infer
  # mail from username, but it has been reported to work on some cases
  # :DD (Maybe use lxplus command "phonebook" ?)
  return  [ CS.getMailForName(e['Morning']) for e in lines ]

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
          was = rsDB.getMonitoredsHistory(granularity,
                                          ['Status', 'Reason', 'DateEffective'],
                                          name, False, 'DESC', 1)[0]

          mailMessage = """Granularity = %s
Name = %s
New perceived status = %s
Reason for status change = %s
Was in status "%s", with reason "%s", since %s
Setup = %s
""" % granularity, name, res['Status'], res['Reason'], was[0], was[1], was[2], setup

          nc.sendMail(CS.getMailForUser(user)['Value'][0],
                      '%s: %s' % (name, res['Status']), mailMessage)


if __name__ == "__main__":
  print getUsersToNotifyShiftDB()
