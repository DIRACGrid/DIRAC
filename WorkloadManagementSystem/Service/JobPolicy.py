########################################################################
# $HeadURL$
########################################################################

""" JobPolicy encapsulates authorization rules for different groups
    with respect to job related operations

"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security import Properties
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN

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

ALL_RIGHTS = [ RIGHT_GET_JOB, RIGHT_GET_INFO, RIGHT_GET_SANDBOX, RIGHT_PUT_SANDBOX,
               RIGHT_CHANGE_STATUS, RIGHT_DELETE, RIGHT_KILL, RIGHT_SUBMIT,
               RIGHT_RESCHEDULE, RIGHT_GET_STATS, RIGHT_RESET ]

OWNER_RIGHTS = [ RIGHT_GET_INFO, RIGHT_GET_SANDBOX, RIGHT_PUT_SANDBOX,
                 RIGHT_CHANGE_STATUS, RIGHT_DELETE, RIGHT_KILL,
                 RIGHT_RESCHEDULE ]

GROUP_RIGHTS = OWNER_RIGHTS

PROPERTY_RIGHTS = {}
PROPERTY_RIGHTS[ Properties.JOB_ADMINISTRATOR ] = ALL_RIGHTS
PROPERTY_RIGHTS[ Properties.NORMAL_USER ] = [ RIGHT_SUBMIT, RIGHT_GET_INFO ]
PROPERTY_RIGHTS[ Properties.GENERIC_PILOT ] = [ RIGHT_RESCHEDULE ]

class JobPolicy:

  def __init__( self, userDN, userGroup, userProperties ):

    self.userDN = userDN
    self.userName = ''
    result = getUsernameForDN(userDN)
    if result['OK']:
      self.userName = result['Value']
    self.userGroup = userGroup
    self.userProperties = userProperties
    self.jobDB = None

  def setJobDB( self, jobDB ):
    """ Supply a JobDB client object
    """

    self.jobDB = jobDB

###########################################################################
  def getUserRightsForJob( self, jobID ):
    """ Get access rights to job with jobID for the user specified by
        userDN/userGroup
    """

    result = self.jobDB.getJobAttributes( jobID, [ 'OwnerDN', 'OwnerGroup' ] )

    if not result['OK']:
      return result
    elif result['Value']:
      owner = result['Value']['OwnerDN']
      group = result['Value']['OwnerGroup']
      result = getUsernameForDN(owner)
      ownerName = ''
      if result['OK']:
        ownerName = result['Value']

      result = self.getJobPolicy( owner, group )

      if self.userName and self.userName == ownerName and self.userGroup == group:
        result[ 'UserIsOwner' ] = True
      else:
        result[ 'UserIsOwner' ] = False
      return result
    else:
      return S_ERROR('Job not found')

###########################################################################
  def getJobPolicy( self, jobOwnerDN = '', jobOwnerGroup = '' ):
    """ Get the job operations rights for a job owned by jobOwnerDN/jobOwnerGroup
        for a user with userDN/userGroup.
        Returns a dictionary of various operations rights
    """

    # Can not do anything by default
    permDict = {}
    for r in ALL_RIGHTS:
      permDict[r] = False

    # Anybody can get info about the jobs
    permDict[ RIGHT_GET_INFO ] = True

    # Give JobAdmin permission if needed
    if Properties.JOB_ADMINISTRATOR in self.userProperties:
      for r in PROPERTY_RIGHTS[ Properties.JOB_ADMINISTRATOR ]:
        permDict[ r ] = True

    # Give JobAdmin permission if needed
    if Properties.NORMAL_USER in self.userProperties:
      for r in PROPERTY_RIGHTS[ Properties.NORMAL_USER ]:
        permDict[ r ] = True

    # Give permissions of the generic pilot
    if Properties.GENERIC_PILOT in self.userProperties:
      for r in PROPERTY_RIGHTS[ Properties.GENERIC_PILOT ]:
        permDict[ r ] = True

    # Job Owner can do everything with his jobs
    result = getUsernameForDN(jobOwnerDN)
    jobOwnerName = ''
    if result['OK']:
      jobOwnerName = result['Value']
    if jobOwnerName and self.userName and jobOwnerName == self.userName:
      for r in OWNER_RIGHTS:
        permDict[r] = True

    # Members of the same group sharing their jobs can do everything
    if jobOwnerGroup == self.userGroup:
      if Properties.JOB_SHARING in self.userProperties:
        for right in GROUP_RIGHTS:
          permDict[right] = True

    return S_OK( permDict )

  def evaluateJobRights( self, jobList, right ):
    """ Get access rights to jobID for the user ownerDN/ownerGroup
    """
    validJobList = []
    invalidJobList = []
    nonauthJobList = []
    ownerJobList = []
    for jobID in jobList:
      result = self.getUserRightsForJob( jobID )
      if result['OK']:
        if result['Value'][right]:
          validJobList.append( jobID )
        else:
          nonauthJobList.append( jobID )
        if result[ 'UserIsOwner' ]:
          ownerJobList.append( jobID )
      else:
        invalidJobList.append( jobID )
  
    return validJobList, invalidJobList, nonauthJobList, ownerJobList