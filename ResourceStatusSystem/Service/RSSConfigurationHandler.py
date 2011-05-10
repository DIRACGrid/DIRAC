"""
RSSConfigurationHandler exposes the RSSConfigurationDB to clients.
"""

from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ResourceStatusSystem.DB.RSSConfigurationDB import RSSConfigurationDB

rssConfDB = None

def initializeRSSConfigurationHandler(serviceInfo):

  # Connect to the RSSConfigurationDB
  global rssConfDB
  rssConfDB = RSSConfigurationDB()

  return S_OK()

class RSSConfigurationHandler(RequestHandler):

  types_addUsers = [list]
  def export_addUsers(self, users):
    gLogger.info("[RSSConfig] Adding users " + str(users))
    return rssConfDB.addUsers(users)

  types_addUser = [str]
  def export_addUser(self, username):
    gLogger.info("[RSSConfig] Adding user " + username)
    return rssConfDB.addUser(username)

  types_delUsers = [list]
  def export_delUsers(self, users):
    gLogger.info("[RSSConfig] Deleting users " + str(users))
    return rssConfDB.delUsers(users)

  types_delUser = [str]
  def export_delUser(self, username):
    gLogger.info("[RSSConfig] Deleting user " + username)
    return rssConfDB.delUser(username)

  types_getUsers = []
  def export_getUsers(self):
    gLogger.info("[RSSConfig] Getting users")
    return rssConfDB.getUsers()

  types_addStatuses = [list]
  def export_addStatuses(self, statuses):
    gLogger.info("[RSSConfig] Adding statuses " + str(statuses))
    return rssConfDB.addStatuses(statuses)

  types_addStatus = [str, int]
  def export_addStatus(self, status, priority):
    gLogger.info("[RSSConfig] Adding status " + status + " with priority " + str(priority))
    return rssConfDB.addStatus(status, priority)

  types_delStatuses = [list]
  def export_delStatuses(self, statuses):
    gLogger.info("[RSSConfig] Deleting statuses " + str(statuses))
    return rssConfDB.delStatuses(statuses)

  types_delUser = [str]
  def export_delStatus(self, status):
    gLogger.info("[RSSConfig] Deleting status " + status)
    return rssConfDB.delStatus(status)

  types_getStatuses = []
  def export_getStatuses(self):
    gLogger.info("[RSSConfig] Getting statuses")
    return rssConfDB.getStatuses()

  types_addCheckFreq = [dict]
  def export_addCheckFreq(self, freq_dict):
    gLogger.info("[RSSConfig] Adding Check Frequency " + str(freq_dict))
    return rssConfDB.addCheckFreq(freq_dict)

  types_delCheckFreq = [dict]
  def export_delCheckFreq(self, freq_dict):
    gLogger.info("[RSSConfig] Deleting Check Frequency " + str(freq_dict))
    return rssConfDB.delCheckFreq(freq_dict)

  types_getCheckFreq = []
  def export_getCheckFreqs(self):
    gLogger.info("[RSSConfig] Getting Check Frequencies")
    return rssConfDB.getCheckFreqs()

  types_addAssigneeGroup = [dict]
  def export_addAssigneeGroup(self, group_dict):
    gLogger.info("[RSSConfig] Adding Assignee Group " + str(group_dict))
    return rssConfDB.addAssigneeGroup(group_dict)

  types_delAssigneeGroup = [dict]
  def export_delAssigneeGroup(self, group_dict):
    gLogger.info("[RSSConfig] Deleting Assignee Group " + str(group_dict))
    return rssConfDB.delAssigneeGroup(group_dict)

  types_getAssigneeGroups = []
  def export_getAssigneeGroups(self):
    gLogger.info("[RSSConfig] Getting Assignee Group")
    return rssConfDB.getAssigneeGroups()

  types_addPolicy = [dict]
  def export_addPolicy(self, pol_dict):
    gLogger.info("[RSSConfig] Adding Policy " + str(pol_dict))
    return rssConfDB.addPolicy(pol_dict)

  types_delPolicy = [dict]
  def export_delPolicy(self, pol_dict):
    gLogger.info("[RSSConfig] Deleting Policy " + str(pol_dict))
    return rssConfDB.delPolicy(pol_dict)

  types_getPolicies = []
  def export_getPolicies(self):
    gLogger.info("[RSSConfig] Getting Policies")
    return rssConfDB.getPolicies()
