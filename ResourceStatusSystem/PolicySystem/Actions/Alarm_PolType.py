################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

"""
  AlarmPolType Actions
"""
from DIRAC.ResourceStatusSystem.Utilities import CS
from DIRAC.ResourceStatusSystem.Utilities import Utils

class AlarmPolType(object):
  def __init__(self, name, res, statusType, nc, rsAPI, rmAPI, **kwargs):
    self.name       = name
    self.res        = res
    self.statusType = statusType
    self.nc         = nc
    self.rsAPI      = rsAPI
    self.rmAPI      = rmAPI
    self.kwargs     = kwargs
    self.setup      = CS.getSetup()
    self.run()

  def getUsersToNotify(self):
    """Get a list of users to notify (helper function for AlarmPolTypeActions)
    Optional keyword arguments:
    - Granularity
    - SiteType
    - ServiceType
    - ResourceType
    """

    notifications = []
    groups = CS.getTypedDictRootedAt("AssigneeGroups/" + self.setup)

    for k in groups:
      if Utils.dictMatch(self.kwargs, groups[k]):
        notifications.append({'Users':groups[k]['Users'],
                              'Notifications':groups[k]['Notifications']})

    return notifications

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
      raise ValueError, "You have to provide a argument Granularity=<desired_granularity>"

    granularity = self.kwargs['Granularity']

    if self.res['Action']:

      notif = "%s %s is perceived as" % (granularity, self.name)
      notif = notif + " %s. Reason: %s." % (self.res['Status'], self.res['Reason'])

      NOTIF_D = self.getUsersToNotify()

      for notification in NOTIF_D:
        for user in notification['Users']:
          if 'Web' in notification['Notifications']:
            self.nc.addNotificationForUser(user, notif)
          if 'Mail' in notification['Notifications']:
            #histGetter = getattr( self.rsAPI, 'get%sHistory' % granularity )

            kwargs = { '%sName'     : self.name,
                       'statusType' : self.statusType,
                       'columns'    : ['Status', 'Reason', 'DateEffective'],
                       'order'      : 'DESC',
                       'limit'      : 1 }

            #was = histGetter( **kwargs )[ 'Value' ][ 0 ]
            was = self.rsAPI.getElementHistory( granularity, **kwargs )[ 'Value' ][ 0 ]

            mailMessage = """Granularity = %s
Name = %s
New perceived status = %s
Reason for status change = %s
Was in status "%s", with reason "%s", since %s
Setup = %s
""" % (granularity, self.name, self.res['Status'], self.res['Reason'], was[0], was[1], was[2], self.setup)

            self.nc.sendMail(self.rmDB.getUserRegistryCache(user)[ 'Value' ][0][0],
                        '[RSS] Status change for site %s: %s -> %s' % (self.name,  self.res['Status'], was[0]), mailMessage)


################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
