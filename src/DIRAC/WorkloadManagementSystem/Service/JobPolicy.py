""" JobPolicy encapsulates authorization rules for different groups
    with respect to job related operations
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Security import Properties
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN, getGroupsForUser, \
    getPropertiesForGroup, getUsersInGroup

RIGHT_GET_JOB = 'GetJob'
RIGHT_GET_INFO = 'GetInfo'
RIGHT_GET_SANDBOX = 'GetSandbox'
RIGHT_PUT_SANDBOX = 'PutSandbox'
RIGHT_CHANGE_STATUS = 'ChangeStatus'
RIGHT_DELETE = 'Delete'
RIGHT_KILL = 'Kill'
RIGHT_SUBMIT = 'Submit'
RIGHT_RESCHEDULE = 'Reschedule'
RIGHT_GET_STATS = 'GetStats'
RIGHT_RESET = 'Reset'

ALL_RIGHTS = [RIGHT_GET_JOB, RIGHT_GET_INFO, RIGHT_GET_SANDBOX, RIGHT_PUT_SANDBOX,
              RIGHT_CHANGE_STATUS, RIGHT_DELETE, RIGHT_KILL, RIGHT_SUBMIT,
              RIGHT_RESCHEDULE, RIGHT_GET_STATS, RIGHT_RESET]

ADMIN_RIGHTS = [RIGHT_GET_JOB, RIGHT_GET_INFO, RIGHT_GET_SANDBOX, RIGHT_PUT_SANDBOX,
                RIGHT_CHANGE_STATUS, RIGHT_DELETE, RIGHT_KILL,
                RIGHT_RESCHEDULE, RIGHT_GET_STATS, RIGHT_RESET]

OWNER_RIGHTS = [RIGHT_GET_INFO, RIGHT_GET_SANDBOX, RIGHT_PUT_SANDBOX,
                RIGHT_CHANGE_STATUS, RIGHT_DELETE, RIGHT_KILL,
                RIGHT_RESCHEDULE]

SHARED_GROUP_RIGHTS = OWNER_RIGHTS

# Rights with respect to non-owner's jobs or to newly created jobs
PROPERTY_RIGHTS = {}
PROPERTY_RIGHTS[Properties.JOB_ADMINISTRATOR] = ADMIN_RIGHTS
PROPERTY_RIGHTS[Properties.NORMAL_USER] = [RIGHT_SUBMIT]
PROPERTY_RIGHTS[Properties.GENERIC_PILOT] = [RIGHT_RESCHEDULE]
PROPERTY_RIGHTS[Properties.JOB_MONITOR] = [RIGHT_GET_INFO]


sLog = gLogger.getSubLogger(__name__)


class JobPolicy(object):

  def __init__(self, userDN, userGroup, allInfo=True):

    self.userDN = userDN
    self.userName = ''
    result = getUsernameForDN(userDN)
    if result['OK']:
      self.userName = result['Value']
    self.userGroup = userGroup
    self.userProperties = getPropertiesForGroup(userGroup, [])
    self.jobDB = None
    self.allInfo = allInfo
    self.__permissions = {}
    self.__getUserJobPolicy()

  def getUserRightsForJob(self, jobID, owner=None, group=None):
    """ Get access rights to job with jobID for the user specified by
        userDN/userGroup
    """
    if owner is None or group is None:
      result = self.jobDB.getJobAttributes(jobID, ['Owner', 'OwnerGroup'])
      if not result['OK']:
        return result
      elif result['Value']:
        owner = result['Value']['OwnerDN']
        group = result['Value']['OwnerGroup']
      else:
        return S_ERROR('Job not found')

    return self.getJobPolicy(owner, group)

  def __getUserJobPolicy(self):
    """ Get the job rights for the primary user for which the JobPolicy object
        is created
    """
    # Can not do anything by default
    for right in ALL_RIGHTS:
      self.__permissions[right] = False

    # Anybody can get info about the jobs
    if self.allInfo:
      self.__permissions[RIGHT_GET_INFO] = True

    # Give JobAdmin permission if needed
    if Properties.JOB_ADMINISTRATOR in self.userProperties:
      for right in PROPERTY_RIGHTS[Properties.JOB_ADMINISTRATOR]:
        self.__permissions[right] = True

    # Give JobMonitor permission if needed
    if Properties.JOB_MONITOR in self.userProperties:
      for right in PROPERTY_RIGHTS[Properties.JOB_MONITOR]:
        self.__permissions[right] = True

    # Give normal user permission if needed
    if Properties.NORMAL_USER in self.userProperties:
      for right in PROPERTY_RIGHTS[Properties.NORMAL_USER]:
        self.__permissions[right] = True

    # Give permissions of the generic pilot
    if Properties.GENERIC_PILOT in self.userProperties:
      for right in PROPERTY_RIGHTS[Properties.GENERIC_PILOT]:
        self.__permissions[right] = True

  def getJobPolicy(self, jobOwner='', jobOwnerGroup=''):
    """ Get the job operations rights for a job owned by jobOwnerDN/jobOwnerGroup
        for a user with userDN/userGroup.
        Returns a dictionary of various operations rights
    """
    permDict = dict(self.__permissions)
    # Job Owner can do everything with his jobs
    if jobOwner and self.userName and jobOwner == self.userName:
      for right in OWNER_RIGHTS:
        permDict[right] = True

    # Members of the same group sharing their jobs can do everything
    if jobOwnerGroup == self.userGroup:
      if Properties.JOB_SHARING in self.userProperties:
        for right in SHARED_GROUP_RIGHTS:
          permDict[right] = True

    return S_OK(permDict)

  def evaluateJobRights(self, jobList, right):
    """ Get access rights to jobID for the user ownerDN/ownerGroup
    """
    validJobList = []
    invalidJobList = []
    nonauthJobList = []
    ownerJobList = []
    userRights = {}

    result = self.jobDB.getJobsAttributes(jobList, ['Owner', 'OwnerGroup'])
    if not result['OK']:
      sLog.error(
          "evaluateJobRights: failure while getJobsAttributes",
          "for %s : %s" % (','.join(str(j) for j in jobList), result['Message']))
      return validJobList, invalidJobList, nonauthJobList, ownerJobList
    jobDict = result['Value']
    for jID in jobList:
      jobID = int(jID)
      if jobID not in jobDict:
        invalidJobList.append(jobID)
        continue
      owner = jobDict[jobID]['Owner']
      group = jobDict[jobID]['OwnerGroup']

      if (owner, group) in userRights:
        rightDict = userRights[(owner, group)]
      else:
        result = self.getUserRightsForJob(jobID, owner=owner, group=group)
        if result['OK']:
          rightDict = result['Value']
          userRights[(owner, group)] = rightDict
        else:
          sLog.error(
              "evaluateJobRights: failure while getUserRightsForJob",
              "for %s : %s" % (str(jobID), result['Message']))
          invalidJobList.append(jobID)

      if rightDict[right]:
        validJobList.append(jobID)
      else:
        nonauthJobList.append(jobID)
      if owner == self.userName:
        ownerJobList.append(jobID)

    return validJobList, invalidJobList, nonauthJobList, ownerJobList

  def getControlledUsers(self, right):
    """ Get users and groups which jobs are subject to the given access right
    """

    userGroupList = 'ALL'
    # If allInfo flag is defined we can see info for any job
    if right == RIGHT_GET_INFO and self.allInfo:
      return S_OK(userGroupList)

    # Administrators can do everything
    if Properties.JOB_ADMINISTRATOR in self.userProperties:
      return S_OK(userGroupList)

    # Inspectors can see info for all the jobs
    if Properties.JOB_MONITOR in self.userProperties and right == RIGHT_GET_INFO:
      return S_OK(userGroupList)

    userGroupList = []
    # User can do many things with his jobs
    if Properties.NORMAL_USER in self.userProperties and right in OWNER_RIGHTS:
      result = getGroupsForUser(self.userName)
      if not result['OK']:
        return result
      groups = result['Value']
      for group in groups:
        if 'NormalUser' in getPropertiesForGroup(group, []):
          userGroupList.append((self.userName, group))

    # User can do many things with the jobs in the shared group
    if Properties.JOB_SHARING in self.userProperties and right in SHARED_GROUP_RIGHTS:
      sharedUsers = getUsersInGroup(self.userGroup)
      for user in sharedUsers:
        userGroupList.append((user, self.userGroup))

    userGroupList = list(set(userGroupList))
    return S_OK(userGroupList)
