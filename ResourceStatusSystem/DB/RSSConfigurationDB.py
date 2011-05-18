"""
Module to access the RSSConfigurationDB
"""

from DIRAC.ResourceStatusSystem.Utilities.Utils import list_split

# Config
t_users          = "Users"
t_statuses       = "Status"
t_policiesparams = "PoliciesParams"
t_checkfreqs     = "CheckFreqs"
t_assigneegroups = "AssigneeGroups"
t_policies       = "Policies"

class RSSConfigurationDB(object):

  def __init__(self, DBin = None, maxQueueSize = 10):
    if DBin: self.db = DBin
    else:
      from DIRAC.Core.Base.DB import DB
      self.db = DB('RSSConfigurationDB',
                   'ResourceStatus/RSSConfigurationDB', maxQueueSize)

  def _delete(self, tableName, inFields, inValues):
    # FIXME: Should go in DIRAC.Core.Utilities.MySQL
    zipped = zip(inFields, inValues)
    where_clause = ''
    for i,(l,v) in enumerate(zipped):
      if i == 0:
        where_clause = where_clause + l + '="' + v + '" '
      else:
        where_clause = where_clause + 'AND ' + l + '="' + v + '" '

    return self.db._update('DELETE FROM ' + tableName + ' WHERE ' + where_clause)

  def _addValue(self, table, vdict):
    keys, values = list_split(vdict.items())
    return self.db._insert(table, keys, values)

  def _delValue(self, table, vdict):
    keys, values = list_split(vdict.items())
    return self._delete(table, keys, values)

  def _addValues(self, table, vdicts):
    res = []
    for d in vdicts:
      res.append(self._addValue(table, d))
    return res

  def _delValues(self, table, vdicts):
    res = []
    for d in vdicts:
      res.append(self._delValue(table, d))
    return res

  def _query(self, table):
    """
    Returns values in a list instead of a tuple, i.e. retval[1] is a
    list of tuples, one per line of result.
    """
    retval = self.db._query("SELECT * from " + table)
    return retval['OK'], list(retval['Value'])

  # Helper functions.

  def addUsers(self, unames):
    """Add users. The argument is a list of strings representing
    usernames."""
    unames = [{'login':u} for u in unames]
    return self._addValues(t_users, unames)

  def addUser(self, uname):
    """Add one user. The argument is a string representing the
    username"""
    return self.addUsers([uname])[0]

  def delUsers(self, unames):
    """Delete users. The argument is a list of strings representing
    usernames."""
    unames = [{'login': u} for u in unames]
    return self._delValues(t_users, unames)

  def delUser(self, uname):
    """Delete a user. The argument is a string representing
    username."""
    return self.delUsers([uname])[0]

  def getUsers(self):
    """Return users registered in database. Return S_OK() with Value:
    a dict representing users with (for now) only a field 'login'.
    TODO: This function is not supposed to fail in normal conditions,
    and result incorrect if it fails. Fix this.
    """
    ret, users = self._query(t_users)
    users = [{'login': u} for u in users]
    return {'OK':ret, 'Value':users}

  def addStatuses(self, statuses):
    """Add statuses. Argument is a list of tuples (label, priority)"""
    statuses = [{'label':l, 'priority':p} for (l,p) in statuses]
    return self._addValues(t_statuses, statuses)

  def addStatus(self, label, priority):
    """Add a status. The argument is respectively a string and an
    integer, representing the name of the status and the priority"""
    return self.addStatuses([(label, priority)])[0]

  def delStatuses(self, statuses):
    """Delete statuses. Argument is a list of strings (labels)"""
    statuses = [{'label':l} for l in statuses]
    return self._delValues(t_statuses, statuses)

  def delStatus(self, status):
    return self.delStatuses([status])[0]

  def getStatuses(self):
    ret, statuses = self._query(t_statuses)
    statuses = [{'label':l, 'priority':p} for (l,p) in statuses]
    return {'OK':ret, 'Value':statuses}

  def addCheckFreq(self, kwargs):
    """
    Add a new check frequency. Arguments must be a dict with fields:
    granularity, site_type, status, freq.
    """
    return self._addValue(t_checkfreqs, kwargs)

  def delCheckFreq(self, kwargs):
    """
    Delete check frequencies. Arguments must be a dict with fields
    part or all of: granularity, site_type, status, freq.
    """
    return self._delValue(t_checkfreqs, kwargs)

  def getCheckFreqs(self):
    ret, freqs = self._query(t_checkfreqs)
    freqs = [{'granularity':g, 'site_type':st,'status':sta, 'freq':f}
             for (g,st,sta,f) in freqs]
    return {'OK':ret, 'Value':freqs}

  def addAssigneeGroup(self, kwargs):
    """
    Add new assignee groups. Argument must be a dict with fields:
    label, login, granularity, site_type, service_type, resource_type,
    notification.
    """
    return self._addValue(t_assigneegroups, kwargs)

  def delAssigneeGroup(self, kwargs):
    """
    Delete assignee groups. Argument must be a dict with fields all or
    part of: label, login, granularity, site_type, service_type,
    resource_type, notification.
    """
    return self._delValue(t_assigneegroups, kwargs)

  def getAssigneeGroups(self):
    ret, groups = self._query(t_assigneegroups)
    groups = [{'label':label, 'login':login, 'granularity':granularity,
               'site_type':site_type, 'service_type':service_type,
               'resource_type':resource_type, 'notification':notification}
              for (label, login, granularity, site_type,
                   service_type, resource_type, notification) in groups
              ]
    return {'OK':ret, 'Value':groups}

  def addPolicy(self, kwargs):
    """
    Add new policies. Argument must be a dict with fiels: label,
    description, status, former_status, site_type, service_type,
    resource_type.
    """
    return self._addValue(t_policies, kwargs)

  def delPolicy(self, kwargs):
    """
    Delete policies. Argument must be a dict with fields all or part
    of: label, description, status, former_status, site_type,
    service_type, resource_type, or a list of labels.
    """
    return self._delValue(t_policies, kwargs)


  def getPolicies(self):
    ret, policies = self._query(t_policies)
    policies = [{'label':label, 'description':description, 'status':status,
                 'former_status':former_status, 'site_type':site_type,
                 'service_type':service_type, 'resource_type':resource_type}
                for (label, description, status, former_status,
                     site_type, service_type, resource_type) in policies
                ]
    return {'OK':ret, 'Value':policies}
