########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Service/JobPolicy.py,v 1.5 2008/01/31 19:03:53 atsareg Exp $
########################################################################

""" JobPolicy encapsulates authorization rules for different groups
    with respect to job related operations

"""

__RCSID__ = "$Id: JobPolicy.py,v 1.5 2008/01/31 19:03:53 atsareg Exp $"

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

  def __init__(self):

    self.jobDB = None

  def setJobDB(self,jobDB):
    """ Supply a JobDB client object
    """

    self.jobDB = jobDB

###########################################################################
  def getUserRightsForJob(self,jobID,userDN,userGroup):
    """ Get access rights to job with jobID for the user specified by
        userDN/userGroup
    """

    result = self.jobDB.getJobAttributes(jobID,['OwnerDN','OwnerGroup'])
        
    if not result['OK']:
      return result
    elif result['Value']:
      owner = result['Value']['OwnerDN']
      group = result['Value']['OwnerGroup']
      result = self.getJobPolicy(userDN,userGroup,owner,group)
      return result
    else:
      return S_ERROR('Job not found')  

###########################################################################
  def getJobPolicy(self,userDN,userGroup,jobOwnerDN='',jobOwnerGroup=''):
    """ Get the job operations rights for a job owned by jobOwnerDN/jobOwnerGroup
        for a user with userDN/userGroup.
        Returns a dictionary of various operations rights
    """

    # Can not do anything by default
    jobDict = {}
    for r in JOB_RIGHTS:
      jobDict[r] = False

    # Anybody can get info about the jobs
    jobDict['GetInfo'] = True

    # Job Owner can do everything with his jobs
    if jobOwnerDN == userDN:
      for r in JOB_OWNER_RIGHTS:
        jobDict[r] = True

    result = gConfig.getOption('/Groups/'+userGroup+'/Properties')

    if result['OK']:
      propertyList = result['Value']
    else:
      return S_ERROR('Failed to get properties for group '+userGroup)

    # Visitors, NormalUsers and JobAdministrators
    for groupProperty in ['Visitor','NormalUser','JobAdministrator','JobAgent']:
      if groupProperty in propertyList:
        for right in GROUP_RIGHTS[groupProperty]:
          jobDict[right] = True

    # Members of the same group sharing their jobs can do everything
    if jobOwnerGroup == userGroup:
      if 'JobSharing' in propertyList:
        for right in GROUP_RIGHTS['JobSharing']:
          jobDict[right] = True

    return S_OK(jobDict)
