########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Service/JobPolicy.py,v 1.9 2008/07/17 13:25:25 acasajus Exp $
########################################################################

""" JobPolicy encapsulates authorization rules for different groups
    with respect to job related operations

"""

__RCSID__ = "$Id: JobPolicy.py,v 1.9 2008/07/17 13:25:25 acasajus Exp $"

from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.Core.Security import Properties

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


class JobPolicy:

  def __init__( self, userDN, userGroup, userProperties ):

    self.userDN = userDN
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
      result = self.getJobPolicy( owner, group )
      if self.userDN == owner and self.userGroup == group:
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

    #Give JobAdmin permission if needed
    if Properties.JOB_ADMINISTRATOR in self.userProperties:
      for r in PROPERTY_RIGHTS[ Properties.JOB_ADMINISTRATOR ]:
        permDict[ r ] = True

    #Give JobAdmin permission if needed
    if Properties.NORMAL_USER in self.userProperties:
      for r in PROPERTY_RIGHTS[ Properties.NORMAL_USER ]:
        permDict[ r ] = True

    # Job Owner can do everything with his jobs
    if jobOwnerDN == self.userDN:
      for r in OWNER_RIGHTS:
        permDict[r] = True

    # Members of the same group sharing their jobs can do everything
    if jobOwnerGroup == self.userGroup:
      if Properties.JOB_SHARING in self.userProperties:
        for right in GROUP_RIGHTS:
          permDict[right] = True

    return S_OK( permDict )
