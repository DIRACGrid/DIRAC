########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Service/JobPolicy.py,v 1.7 2008/07/14 13:15:59 acasajus Exp $
########################################################################

""" JobPolicy encapsulates authorization rules for different groups
    with respect to job related operations

"""

__RCSID__ = "$Id: JobPolicy.py,v 1.7 2008/07/14 13:15:59 acasajus Exp $"

from DIRAC import gConfig, S_OK, S_ERROR

JOB_RIGHTS = ['GetJob','GetInfo','GetSandbox','PutSandbox','ChangeStatus',
              'Delete','Kill','Submit','Reschedule','GetStats']
GROUP_RIGHTS = {}
GROUP_RIGHTS['Visitor'] = ['GetInfo']
GROUP_RIGHTS['NormalUser'] = ['GetInfo','Submit']
GROUP_RIGHTS['JobSharing'] = ['GetInfo','GetSandbox','PutSandbox','ChangeStatus',
                              'Delete','Kill','Reschedule']
GROUP_RIGHTS['JobAgent'] = ['GetJob','GetInfo','GetSandbox','PutSandbox','ChangeStatus',
                            'Reschedule']
GROUP_RIGHTS['JobAdministrator'] = JOB_RIGHTS

JOB_OWNER_RIGHTS = ['GetInfo','GetInput','GetOutput','ChangeStatus',
                    'Delete','Kill','Submit','Reschedule']


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
    for r in JOB_RIGHTS:
      permDict[r] = False

    # Anybody can get info about the jobs
    permDict['GetInfo'] = True

    # Job Owner can do everything with his jobs
    if jobOwnerDN == self.userDN:
      for r in JOB_OWNER_RIGHTS:
        permDict[r] = True

    # Visitors, NormalUsers and JobAdministrators
    for groupProperty in ['Visitor','NormalUser','JobAdministrator','JobAgent']:
      if groupProperty in self.userProperties:
        for right in GROUP_RIGHTS[ groupProperty ]:
          permDict[ right ] = True

    # Members of the same group sharing their jobs can do everything
    if jobOwnerGroup == self.userGroup:
      if 'JobSharing' in self.userProperties:
        for right in GROUP_RIGHTS['JobSharing']:
          permDict[right] = True

    return S_OK( permDict )
