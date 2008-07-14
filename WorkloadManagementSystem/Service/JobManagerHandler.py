########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Service/JobManagerHandler.py,v 1.17 2008/07/14 13:15:59 acasajus Exp $
########################################################################

""" JobManagerHandler is the implementation of the JobManager service
    in the DISET framework

    The following methods are available in the Service interface

    submitJob()
    rescheduleJob()
    deleteJob()
    killJob()

"""

__RCSID__ = "$Id: JobManagerHandler.py,v 1.17 2008/07/14 13:15:59 acasajus Exp $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.Service.JobPolicy import JobPolicy
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager

# This is a global instance of the JobDB class
gJobDB = False

def initializeJobManagerHandler( serviceInfo ):

  global gJobDB
  gJobDB = JobDB()
  return S_OK()

class JobManagerHandler( RequestHandler ):

  def initialize(self):
    credDict = self.getRemoteCredentials()
    self.userDN = credDict['DN']
    self.userGroup = credDict['group']
    self.userProperties = credDict[ 'properties' ]
    self.userName = credDict[ 'username' ]
    self.peerUsesLimitedProxy = credDict[ 'isLimitedProxy' ]
    self.jobPolicy = JobPolicy( self.userDN, self.userGroup, self.userProperties )

  ###########################################################################
  types_submitJob = [ StringType ]
  def export_submitJob( self, JDL ):
    """ Submit a single job to DIRAC WMS
    """

    if self.peerUsesLimitedProxy:
      return S_ERROR( "Can't submit using a limited proxy! (bad boy!)" )

    # Check job submission permission
    result = self.jobPolicy.getJobPolicy()
    if not result['OK']:
      return S_ERROR( 'Failed to get job policies' )
    policyDict = result['Value']
    if not policyDict['Submit']:
      return S_ERROR('Job submission not authorized')

    # Get the new jobID first
    result_jobID  = gJobDB.getJobID()
    if not result_jobID['OK']:
      return S_ERROR('Failed to acquire a new JobID')

    jobID = int( result_jobID['Value'] )
    gLogger.verbose( "Served jobID %s" % jobID )
    # Now add a new job
    #gActivityClient.addMark( "submitJob" )

    classAdJob = ClassAd( "[%s]" % JDL )
    classAdJob.insertAttributeInt( 'JobID', jobID )
    classAdJob.insertAttributeString( 'DIRACSetup', self.serviceInfoDict['clientSetup'] )

    # Force the owner name to be the nickname defined in the CS
    classAdJob.insertAttributeString( 'Owner', self.userName )
    classAdJob.insertAttributeString( 'OwnerDN', self.userDN )
    classAdJob.insertAttributeString( 'OwnerGroup', self.userGroup )

    newJDL = classAdJob.asJDL()
    result  = gJobDB.insertJobIntoDB( jobID, newJDL )
    if not result['OK']:
      return result
    result  = gJobDB.addJobToDB( jobID,
                                JDL = newJDL,
                                ownerDN = self.userDN,
                                ownerGroup = self.userGroup )
    if not result['OK']:
      return result

    result = gJobDB.setJobJDL( jobID, newJDL )
    if not result['OK']:
      return result

    gLogger.info( 'Job %s added to the JobDB for %s/%s' % ( str(jobID), self.userDN, self.userGroup ) )

    #Set persistency flag
    retVal = gProxyManager.getUserPersistence( self.userDN, self.userGroup )
    if 'Value' not in retVal or not retVal[ 'Value' ]:
      gProxyManager.setPersistency( self.userDN, self.userGroup, True )

    result = S_OK( jobID )
    result[ 'requireProxyUpload' ] = self.__checkIfProxyUploadIsRequired()
    return result

###########################################################################
  def __checkIfProxyUploadIsRequired( self ):
    result = gProxyManager.userHasProxy( self.userDN, self.userGroup, validSeconds = 18000 )
    if not result[ 'OK' ]:
      gLogger.error( "Can't check if the user has proxy uploaded: %s" % result[ 'Message' ] )
      return True
    #Check if an upload is required
    return result[ 'Value' ] == False

###########################################################################
  types_invalidateJob = [ IntType ]
  def invalidateJob(self,jobID):
    """ Make job with jobID invalid, e.g. because of the sandbox submission
        errors.
    """

    pass

###########################################################################
  def __get_job_list( self, jobInput ):
    """ Evaluate the jobInput into a list of ints
    """

    if type(jobInput) == IntType:
      return [jobInput]
    if type(jobInput) == StringType:
      try:
        ijob = int(jobInput)
        return [ijob]
      except:
        return []
    if type(jobInput) == ListType:
      try:
        ljob = [ int(x) for x in jobInput ]
        return ljob
      except:
        return []

    return []

###########################################################################
  def __evaluate_rights( self, jobList, right):
    """ Get access rights to jobID for the user userDN/userGroup
    """
    self.jobPolicy.setJobDB( gJobDB )
    validJobList = []
    invalidJobList = []
    nonauthJobList = []
    ownerJobList = []
    for jobID in jobList:
      result = self.jobPolicy.getUserRightsForJob( jobID )
      if result['OK']:
        if result['Value'][right]:
          validJobList.append(jobID)
        else:
          nonauthJobList.append(jobID)
        if result[ 'UserIsOwner' ]:
          ownerJobList.append(jobID)
      else:
        invalidJobList.append(jobID)

    return validJobList,invalidJobList,nonauthJobList

###########################################################################
  types_rescheduleJob = [ ]
  def export_rescheduleJob(self, jobIDs ):
    """  Reschedule a single job. If the optional proxy parameter is given
         it will be used to refresh the proxy in the Proxy Repository
    """

    jobList = self.__get_job_list(jobIDs)
    if not jobList:
      return S_ERROR('Invalid job specification: '+str(jobIDs))

    validJobList,invalidJobList,nonauthJobList,ownerJobList = self.__evaluate_rights(jobList,
                                                                        'Reschedule' )
    for jobID in validJobList:
      result  = gJobDB.rescheduleJob( jobID )
      gLogger.debug( str( result ) )
      if not result['OK']:
          return result

    if invalidJobList or nonauthJobList:
      result = S_ERROR('Some jobs failed deletion')
      if invalidJobList:
        result['InvalidJobIDs'] = invalidJobList
      if nonauthJobList:
        result['NonauthorizedJobIDs'] = nonauthJobList
      return result

    result = S_OK( validJobList )
    result[ 'requireProxyUpload' ] = len( ownerJobList ) > 0 and self.__checkIfProxyUploadIsRequired()
    return result


###########################################################################
  types_deleteJob = [  ]
  def export_deleteJob(self, jobIDs):
    """  Delete jobs specified in the jobIDs list
    """

    jobList = self.__get_job_list(jobIDs)
    if not jobList:
      return S_ERROR('Invalid job specification: '+str(jobIDs))

    validJobList,invalidJobList,nonauthJobList,ownerJobList = self.__evaluate_rights(jobList,
                                                                        'Delete')

    bad_ids = []
    good_ids = []
    for jobID in validJobList:
      result = gJobDB.setJobStatus(jobID,'Deleted','Checking accounting')
      if not result['OK']:
        bad_ids.append(jobID)
      else:
        good_ids.append(jobID)

    if invalidJobList or nonauthJobList:
      result = S_ERROR('Some jobs failed deletion')
      if invalidJobList:
        result['InvalidJobIDs'] = invalidJobList
      if nonauthJobList:
        result['NonauthorizedJobIDs'] = nonauthJobList
      if bad_ids:
        result['FailedJobIDs'] = bad_ids
      return result

    result = S_OK( validJobList )
    result[ 'requireProxyUpload' ] = len( ownerJobList ) > 0 and self.__checkIfProxyUploadIsRequired()
    return result

###########################################################################
  types_killJob = [  ]
  def export_killJob(self, jobIDs):
    """  Kill jobs specified in the jobIDs list
    """

    jobList = self.__get_job_list(jobIDs)
    if not jobList:
      return S_ERROR('Invalid job specification: '+str(jobIDs))

    validJobList,invalidJobList,nonauthJobList,ownerJobList = self.__evaluate_rights(jobList,
                                                                        'Kill')

    bad_ids = []
    good_ids = []
    for jobID in validJobList:
      # kill jobID
      result = gJobDB.setJobCommand(jobID,'Kill','')
      if not result['OK']:
        bad_ids.append(jobID)
      else:
        gLogger.info('Job %d is marked for termination' % jobID)
        good_ids.append(jobID)
        result = gJobDB.setJobStatus(jobID,'Killed','Marked for termination')
        if not result['OK']:
          gLogger.warn('Failed to set job status')

    if invalidJobList or nonauthJobList or bad_ids:
      result = S_ERROR('Some jobs failed deletion')
      if invalidJobList:
        result['InvalidJobIDs'] = invalidJobList
      if nonauthJobList:
        result['NonauthorizedJobIDs'] = nonauthJobList
      if bad_ids:
        result['FailedJobIDs'] = bad_ids
      return result

    result = S_OK( validJobList )
    result[ 'requireProxyUpload' ] = len( ownerJobList ) > 0 and self.__checkIfProxyUploadIsRequired()
    return result

###########################################################################
  types_resetJob = [  ]
  def export_resetJob(self, jobIDs):
    """  Reset jobs specified in the jobIDs list
    """

    jobList = self.__get_job_list(jobIDs)
    if not jobList:
      return S_ERROR('Invalid job specification: '+str(jobIDs))

    validJobList,invalidJobList,nonauthJobList,ownerJobList = self.__evaluate_rights(jobList,
                                                                        'Reschedule')

    bad_ids = []
    good_ids = []
    for jobID in validJobList:
      result = gJobDB.setJobAttribute(jobID,'RescheduleCounter',1)
      if not result['OK']:
        bad_ids.append(jobID)
      else:
        result  = gJobDB.rescheduleJob( jobID )
        if not result['OK']:
          bad_ids.append(jobID)
        else:
          good_ids.append(jobID)

    if invalidJobList or nonauthJobList or bad_ids:
      result = S_ERROR('Some jobs failed resetting')
      if invalidJobList:
        result['InvalidJobIDs'] = invalidJobList
      if nonauthJobList:
        result['NonauthorizedJobIDs'] = nonauthJobList
      if bad_ids:
        result['FailedJobIDs'] = bad_ids
      return result

    result = S_OK()
    result[ 'requireProxyUpload' ] = len( ownerJobList ) > 0 and self.__checkIfProxyUploadIsRequired()
    return result
