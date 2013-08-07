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

ADMIN_RIGHTS = [ RIGHT_GET_JOB, RIGHT_GET_INFO, RIGHT_GET_SANDBOX, RIGHT_PUT_SANDBOX,
               RIGHT_CHANGE_STATUS, RIGHT_DELETE, RIGHT_KILL, 
               RIGHT_RESCHEDULE, RIGHT_GET_STATS, RIGHT_RESET ]

OWNER_RIGHTS = [ RIGHT_GET_INFO, RIGHT_GET_SANDBOX, RIGHT_PUT_SANDBOX,
                 RIGHT_CHANGE_STATUS, RIGHT_DELETE, RIGHT_KILL,
                 RIGHT_RESCHEDULE ]

SHARED_GROUP_RIGHTS = OWNER_RIGHTS

# Rights with respect to non-owner's jobs or to newly created jobs
PROPERTY_RIGHTS = {}
PROPERTY_RIGHTS[ Properties.JOB_ADMINISTRATOR ] = ADMIN_RIGHTS
PROPERTY_RIGHTS[ Properties.NORMAL_USER ] = [ RIGHT_SUBMIT ]
PROPERTY_RIGHTS[ Properties.GENERIC_PILOT ] = [ RIGHT_RESCHEDULE ]

class JobPolicy:

  def __init__( self, userDN, userGroup, userProperties, allInfo=True ):

    self.userDN = userDN
    self.userName = ''
    result = getUsernameForDN(userDN)
    if result['OK']:
      self.userName = result['Value']
    self.userGroup = userGroup
    self.userProperties = userProperties
    self.jobDB = None
    self.allInfo = allInfo
    self.__permissions = {}
    self.__getUserJobPolicy()

  def setJobDB( self, jobDB ):
    """ Supply a JobDB client object
    """

    self.jobDB = jobDB

###########################################################################
  def getUserRightsForJob( self, jobID, owner=None, group=None ):
    """ Get access rights to job with jobID for the user specified by
        userDN/userGroup
    """
    if owner is None or group is None:
      result = self.jobDB.getJobAttributes( jobID, [ 'Owner', 'OwnerGroup' ] )
      if not result['OK']:
        return result
      elif result['Value']:
        owner = result['Value']['OwnerDN']
        group = result['Value']['OwnerGroup']
      else:
        return S_ERROR('Job not found')
    
    result = self.getJobPolicy( owner, group )
    return result

###########################################################################
  def __getUserJobPolicy( self ):
    """ Get the job rights for the primary user for which the JobPolicy object
        is created 
    """
    # Can not do anything by default
    for r in ALL_RIGHTS:
      self.__permissions[r] = False

    # Anybody can get info about the jobs
    if self.allInfo:
      self.__permissions[ RIGHT_GET_INFO ] = True

    # Give JobAdmin permission if needed
    if Properties.JOB_ADMINISTRATOR in self.userProperties:
      for r in PROPERTY_RIGHTS[ Properties.JOB_ADMINISTRATOR ]:
        self.__permissions[ r ] = True

    # Give normal user permission if needed
    if Properties.NORMAL_USER in self.userProperties:
      for r in PROPERTY_RIGHTS[ Properties.NORMAL_USER ]:
        self.__permissions[ r ] = True

    # Give permissions of the generic pilot
    if Properties.GENERIC_PILOT in self.userProperties:
      for r in PROPERTY_RIGHTS[ Properties.GENERIC_PILOT ]:
        self.__permissions[ r ] = True


###########################################################################
  def getJobPolicy( self, jobOwner = '', jobOwnerGroup = '' ):
    """ Get the job operations rights for a job owned by jobOwnerDN/jobOwnerGroup
        for a user with userDN/userGroup.
        Returns a dictionary of various operations rights
    """
    permDict = dict( self.__permissions )
    # Job Owner can do everything with his jobs
    if jobOwner and self.userName and jobOwner == self.userName:
      for r in OWNER_RIGHTS:
        permDict[r] = True

    # Members of the same group sharing their jobs can do everything
    if jobOwnerGroup == self.userGroup:
      if Properties.JOB_SHARING in self.userProperties:
        for right in SHARED_GROUP_RIGHTS:
          permDict[right] = True

    return S_OK( permDict )

  def evaluateJobRights( self, jobList, right ):
    """ Get access rights to jobID for the user ownerDN/ownerGroup
    """
    validJobList = []
    invalidJobList = []
    nonauthJobList = []
    ownerJobList = []
    userRights = {}
    
    result = self.jobDB.getAttributesForJobList( jobList, [ 'Owner', 'OwnerGroup' ] )        
    if not result['OK']:
      return validJobList, invalidJobList, nonauthJobList, ownerJobList
    jobDict = result['Value']    
    for jID in jobList:
      jobID = int( jID )
      if not jobID in jobDict:
        invalidJobList.append( jobID )
        continue 
      owner = jobDict[jobID]['Owner']
      group = jobDict[jobID]['OwnerGroup']
      
      if (owner,group) in userRights:
        rightDict = userRights[ (owner,group) ]
      else:  
        result = self.getUserRightsForJob( jobID, owner=owner, group=group )
        if result['OK']:
          rightDict = result['Value']
          userRights[ (owner,group) ] = rightDict
        else:
          invalidJobList.append( jobID )
        
      if rightDict[right]:
        validJobList.append( jobID )
      else:
        nonauthJobList.append( jobID )
      if owner == self.userName:
        ownerJobList.append( jobID )
  
    return validJobList, invalidJobList, nonauthJobList, ownerJobList