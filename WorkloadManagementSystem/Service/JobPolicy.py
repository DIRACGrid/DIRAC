########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Service/JobPolicy.py,v 1.1 2007/12/19 14:53:18 atsareg Exp $
########################################################################

""" JobPolicy encapsulates authorization rules for different groups
    with respect to job related operations

"""

__RCSID__ = "$Id: JobPolicy.py,v 1.1 2007/12/19 14:53:18 atsareg Exp $"

from DIRAC import gConfig, S_OK, S_ERROR

class JobPolicy:

  def __init__(self):

    self.jobDB = None

  def setJobDB(self,jobDB):
    """ Supply a JobDB client object
    """

    self.jobDB = jobDB

  def getJobPolicy(self,userDN,userGroup,jobOwnerDN='',jobOwnerGroup=''):
    """ Get the job operations rights for a job owned by jobOwnerDN/jobOwnerGroup
        for a user with userDN/userGroup.
        Returns a dictionary of various operations rights
    """

    rightsList = ['GetInfo','GetInput','GetOutput','ChangeStatus','Delete','Kill']

    # Can not do anything by default
    jobDict = {}
    jobDict['Run'] = False
    for r in rightsList:
      jobDict[r] = False

    # Anybody can get info about the jobs
    jobDict['GetInfo'] = True

    # Job Owner can do everything
    # If the jobOwner is defined, the job is already existing
    if jobOwnerDN == userDN:
      for r in rightsList:
        jobDict[r] = True
      return S_OK(jobDict)

    result = gConfig.getOption('/Groups/'+userGroup+'/Properties')
    if result['OK']:
      propertyList = result['Value']
    else:
      return S_ERROR('Failed to get properties for group '+userGroup)

    # Visitors can only get information
    if 'Visitor' in propertyList:
      return S_OK(jobDict)

    # Normal users can run jobs
    if 'NormalUser' in propertyList:
      jobDict['Run'] = True

    # Administrators can do everything
    if 'JobAdministration' in propertyList:
      for r in rightsList:
        jobDict[r] = True
      return S_OK(jobDict)

    # Members of the same group sharing their jobs can do everything
    if jobOwnerGroup == userGroup:
      if 'JobSharing' in propertyList:
        for r in rightsList:
          jobDict[r] = True
        return S_OK(jobDict)


