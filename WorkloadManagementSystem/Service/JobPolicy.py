""" JobPolicy encapsulates authorization rules for different groups
    with respect to job related operations
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security import Properties
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getGroupsForUser, getPropertiesForGroup, getUsersInGroup
    
RIGHT_KILL = 'Kill'
RIGHT_RESET = 'Reset'
RIGHT_DELETE = 'Delete'
RIGHT_SUBMIT = 'Submit'
RIGHT_GET_JOB = 'GetJob'
RIGHT_GET_INFO = 'GetInfo'
RIGHT_GET_STATS = 'GetStats'
RIGHT_RESCHEDULE = 'Reschedule'
RIGHT_GET_SANDBOX = 'GetSandbox'
RIGHT_PUT_SANDBOX = 'PutSandbox'
RIGHT_CHANGE_STATUS = 'ChangeStatus'

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


class JobPolicy(object):

  def __init__(self, username, userGroup, allInfo=True):
    """ C'tor

        :param str username: user name
        :param str userGroup: group name
        :param bool allInfo: all information
    """
    self.jobDB = None
    self.allInfo = allInfo
    self.userName = username
    self.userGroup = userGroup
    self.userProperties = getPropertiesForGroup(userGroup, [])
    self.__permissions = {}
    self.__getUserJobPolicy()

  def getUserRightsForJob(self, jobID, owner=None, group=None):
    """ Get access rights to job with jobID for the user specified by username/userGroup

        :param str jobID: job ID
        :param str owner: user name
        :param str group: group name

        :return: S_OK()/S_ERROR()
    """
    if owner is None or group is None:
      result = self.jobDB.getJobAttributes(jobID, ['Owner', 'OwnerGroup'])
      if not result['OK']:
        return result
      if not result['Value']:
        return S_ERROR('Job not found')
      owner = result['Value']['OwnerDN']
      group = result['Value']['OwnerGroup']

    result = self.getJobPolicy(owner, group)
    return result

  def __getUserJobPolicy(self):
    """ Get the job rights for the primary user for which the JobPolicy object is created
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

  def getJobPolicy(self, jobOwner=None, jobOwnerGroup=None):
    """ Get the job operations rights for a job owned by jobOwner/jobOwnerGroup
        for a user with username/userGroup.
        Returns a dictionary of various operations rights

        :param str jobOwner: user name
        :param str jobOwnerGroup: group name

        :return: S_OK(dict)/S_ERROR()
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
    """ Get access rights to jobID for the user owner/ownerGroup

        :param list jobList: job list
        :param str right: right

        :return: tuple -- contain valid, invalid, nonauth, owner jobs
    """
    validJobList = []
    invalidJobList = []
    nonauthJobList = []
    ownerJobList = []
    userRights = {}

    result = self.jobDB.getAttributesForJobList(jobList, ['Owner', 'OwnerGroup'])
    if not result['OK']:
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

        :param str right: right

        :return: S_OK()/S_ERROR()
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
