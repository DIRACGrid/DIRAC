""" JobManagerHandler is the implementation of the JobManager service
    in the DISET framework

    The following methods are available in the Service interface

    submitJob()
    rescheduleJob()
    deleteJob()
    killJob()

"""

__RCSID__ = "$Id$"

from types import StringType, IntType, ListType
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB     import TaskQueueDB
from DIRAC.Core.DISET.MessageClient import MessageClient
from DIRAC.WorkloadManagementSystem.Service.JobPolicy import JobPolicy, \
                                                             RIGHT_SUBMIT, RIGHT_RESCHEDULE, \
                                                             RIGHT_DELETE, RIGHT_KILL, RIGHT_RESET
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient

# This is a global instance of the JobDB class
gJobDB = False
gJobLoggingDB = False
gtaskQueueDB = False

MAX_PARAMETRIC_JOBS = 20

def initializeJobManagerHandler( serviceInfo ):

  global gJobDB, gJobLoggingDB, gtaskQueueDB
  gJobDB = JobDB()
  gJobLoggingDB = JobLoggingDB()
  gtaskQueueDB = TaskQueueDB()
  return S_OK()

class JobManagerHandler( RequestHandler ):

  @classmethod
  def initializeHandler( cls, serviceInfoDict ):
    cls.msgClient = MessageClient( "WorkloadManagement/OptimizationMind" )
    cls.__connectToOptMind()
    gThreadScheduler.addPeriodicTask( 60, cls.__connectToOptMind )
    return S_OK()


  @classmethod
  def __connectToOptMind( cls ):
    if not cls.msgClient.connected:
      result = cls.msgClient.connect( JobManager = True )
      if not result[ 'OK' ]:
        cls.log.warn( "Cannot connect to OptimizationMind!", result[ 'Message' ] )

  def initialize( self ):
    credDict = self.getRemoteCredentials()
    self.ownerDN = credDict['DN']
    self.ownerGroup = credDict['group']
    self.userProperties = credDict[ 'properties' ]
    self.owner = credDict[ 'username' ]
    self.peerUsesLimitedProxy = credDict[ 'isLimitedProxy' ]
    self.diracSetup = self.serviceInfoDict['clientSetup']
    self.maxParametricJobs = self.srv_getCSOption( 'MaxParametricJobs', MAX_PARAMETRIC_JOBS )
    self.jobPolicy = JobPolicy( self.ownerDN, self.ownerGroup, self.userProperties )
    self.jobPolicy.setJobDB( gJobDB )
    return S_OK()

  def __sendJobsToOptimizationMind( self, jids ):
    if not self.msgClient.connected:
      return
    result = self.msgClient.createMessage( "OptimizeJobs" )
    if not result[ 'OK' ]:
      self.log.error( "Cannot create Optimize message: %s" % result[ 'Message' ] )
      return
    msgObj = result[ 'Value' ]
    msgObj.jids = list( sorted( jids ) )
    result = self.msgClient.sendMessage( msgObj )
    if not result[ 'OK' ]:
      self.log.error( "Cannot send Optimize message: %s" % result[ 'Message' ] )
      return
    self.log.info( "Optimize msg sent for %s jobs" % len( jids ) )

  ###########################################################################
  types_submitJob = [ StringType ]
  def export_submitJob( self, jobDesc ):
    """ Submit a single job to DIRAC WMS
    """

    if self.peerUsesLimitedProxy:
      return S_ERROR( "Can't submit using a limited proxy! (bad boy!)" )

    # Check job submission permission
    result = self.jobPolicy.getJobPolicy()
    if not result['OK']:
      return S_ERROR( 'Failed to get job policies' )
    policyDict = result['Value']
    if not policyDict[ RIGHT_SUBMIT ]:
      return S_ERROR( 'Job submission not authorized' )

    #jobDesc is JDL for now
    jobDesc = jobDesc.strip()
    if jobDesc[0] != "[":
      jobDesc = "[%s" % jobDesc
    if jobDesc[-1] != "]":
      jobDesc = "%s]" % jobDesc

    result = gJobDB.insertNewJobIntoDB( jobDesc, self.owner, self.ownerDN, self.ownerGroup, self.diracSetup )
    if not result['OK']:
      return result

    jobID = result['JobID']
    gLogger.info( 'Job %s added to the JobDB for %s/%s' % ( jobID, self.ownerDN, self.ownerGroup ) )

    gJobLoggingDB.addLoggingRecord( jobID, result['Status'], result['MinorStatus'], source = 'JobManager' )

    result = S_OK( jobID )

    result['JobID'] = result['Value']
    result[ 'requireProxyUpload' ] = self.__checkIfProxyUploadIsRequired()
    self.__sendJobsToOptimizationMind( [ jobID ] )
    return result

###########################################################################
  def __checkIfProxyUploadIsRequired( self ):
    result = gProxyManager.userHasProxy( self.ownerDN, self.ownerGroup, validSeconds = 18000 )
    if not result[ 'OK' ]:
      gLogger.error( "Can't check if the user has proxy uploaded:", result[ 'Message' ] )
      return True
    #Check if an upload is required
    return result[ 'Value' ] == False

###########################################################################
  types_invalidateJob = [ IntType ]
  def invalidateJob( self, jobID ):
    """ Make job with jobID invalid, e.g. because of the sandbox submission
        errors.
    """

    pass

###########################################################################
  def __get_job_list( self, jobInput ):
    """ Evaluate the jobInput into a list of ints
    """

    if type( jobInput ) == IntType:
      return [jobInput]
    if type( jobInput ) == StringType:
      try:
        ijob = int( jobInput )
        return [ijob]
      except:
        return []
    if type( jobInput ) == ListType:
      try:
        ljob = [ int( x ) for x in jobInput ]
        return ljob
      except:
        return []

    return []

###########################################################################
  types_rescheduleJob = [ ]
  def export_rescheduleJob( self, jobIDs ):
    """  Reschedule a single job. If the optional proxy parameter is given
         it will be used to refresh the proxy in the Proxy Repository
    """

    jobList = self.__get_job_list( jobIDs )
    if not jobList:
      return S_ERROR( 'Invalid job specification: ' + str( jobIDs ) )

    validJobList, invalidJobList, nonauthJobList, ownerJobList = self.jobPolicy.evaluateJobRights( jobList,
                                                                                                   RIGHT_RESCHEDULE )
    for jobID in validJobList:
      gtaskQueueDB.deleteJob( jobID )
      #gJobDB.deleteJobFromQueue(jobID)
      result = gJobDB.rescheduleJob( jobID )
      gLogger.debug( str( result ) )
      if not result['OK']:
        return result
      gJobLoggingDB.addLoggingRecord( result['JobID'], result['Status'], result['MinorStatus'],
                                      application = 'Unknown', source = 'JobManager' )

    if invalidJobList or nonauthJobList:
      result = S_ERROR( 'Some jobs failed reschedule' )
      if invalidJobList:
        result['InvalidJobIDs'] = invalidJobList
      if nonauthJobList:
        result['NonauthorizedJobIDs'] = nonauthJobList
      return result

    result = S_OK( validJobList )
    result[ 'requireProxyUpload' ] = len( ownerJobList ) > 0 and self.__checkIfProxyUploadIsRequired()
    self.__sendJobsToOptimizationMind( validJobList )
    return result

  def __deleteJob( self, jobID ):
    """ Delete one job
    """
    result = gJobDB.setJobStatus( jobID, 'Deleted', 'Checking accounting' )
    if not result['OK']:
      return result

    result = gtaskQueueDB.deleteJob( jobID )
    if not result['OK']:
      gLogger.warn( 'Failed to delete job from the TaskQueue' )

    return S_OK()

  def __killJob( self, jobID ):
    """  Kill one job
    """
    result = gJobDB.setJobCommand( jobID, 'Kill' )
    if not result['OK']:
      return result
    else:
      gLogger.info( 'Job %d is marked for termination' % jobID )
      result = gJobDB.setJobStatus( jobID, 'Killed', 'Marked for termination' )
      if not result['OK']:
        gLogger.warn( 'Failed to set job Killed status' )
      result = gtaskQueueDB.deleteJob( jobID )
      if not result['OK']:
        gLogger.warn( 'Failed to delete job from the TaskQueue' )

    return S_OK()

  def __kill_delete_jobs( self, jobIDList, right ):
    """  Kill or delete jobs as necessary
    """

    jobList = self.__get_job_list( jobIDList )
    if not jobList:
      return S_ERROR( 'Invalid job specification: ' + str( jobIDList ) )

    validJobList, invalidJobList, nonauthJobList, ownerJobList = self.jobPolicy.evaluateJobRights( jobList, right )

    # Get job status to see what is to be killed or deleted
    result = gJobDB.getAttributesForJobList( validJobList, ['Status'] )
    if not result['OK']:
      return result
    killJobList = []
    deleteJobList = []
    stagingJobList = []
    for jobID, sDict in result['Value'].items():
      if sDict['Status'] in ['Running','Matched','Stalled']:
        killJobList.append( jobID )
      elif sDict['Status'] in ['Done','Failed']:
        if not right == RIGHT_KILL:
          deleteJobList.append( jobID )
      else:
        deleteJobList.append( jobID )
      if sDict['Status'] in ['Staging']:
        stagingJobList.append( jobID )
    
    bad_ids = []
    for jobID in killJobList:
      result = self.__killJob( jobID )
      if not result['OK']:
        bad_ids.append( jobID )

    for jobID in deleteJobList:
      result = self.__deleteJob( jobID )
      if not result['OK']:
        bad_ids.append( jobID )

    if stagingJobList:
      stagerClient = StorageManagerClient()
      gLogger.info('Going to send killing signal to stager as well!')
      result = stagerClient.killTasksBySourceTaskID(stagingJobList)
      if not result['OK']:
        gLogger.warn( 'Failed to kill some Stager tasks: %s' % result['Message'] )

    if nonauthJobList or bad_ids:
      result = S_ERROR( 'Some jobs failed deletion' )
      if nonauthJobList:
        result['NonauthorizedJobIDs'] = nonauthJobList
      if bad_ids:
        result['FailedJobIDs'] = bad_ids
      return result

    result = S_OK( validJobList )
    result[ 'requireProxyUpload' ] = len( ownerJobList ) > 0 and self.__checkIfProxyUploadIsRequired()

    if invalidJobList:
      result['InvalidJobIDs'] = invalidJobList

    return result

###########################################################################
  types_deleteJob = [  ]
  def export_deleteJob( self, jobIDs ):
    """  Delete jobs specified in the jobIDs list
    """

    return self.__kill_delete_jobs( jobIDs, RIGHT_DELETE )

###########################################################################
  types_killJob = [  ]
  def export_killJob( self, jobIDs ):
    """  Kill jobs specified in the jobIDs list
    """

    return self.__kill_delete_jobs( jobIDs, RIGHT_KILL )

###########################################################################
  types_resetJob = [  ]
  def export_resetJob( self, jobIDs ):
    """  Reset jobs specified in the jobIDs list
    """

    jobList = self.__get_job_list( jobIDs )
    if not jobList:
      return S_ERROR( 'Invalid job specification: ' + str( jobIDs ) )

    validJobList, invalidJobList, nonauthJobList, ownerJobList = self.jobPolicy.evaluateJobRights( jobList,
                                                                                                   RIGHT_RESET )

    bad_ids = []
    good_ids = []
    for jobID in validJobList:
      gtaskQueueDB.deleteJob( jobID )
      result = gJobDB.resetJob( jobID )
      if not result['OK']:
        self.log.warn( "Could not reset job %d: %s" % ( jobID, result[ 'Message' ] ) )
        bad_ids.append( jobID )
      else:
        good_ids.append( jobID )
      if 'Status' in result:
        gJobLoggingDB.addLoggingRecord( jobID, result['Status'], result['MinorStatus'],
                                        application = 'Unknown', source = 'JobManager' )

    self.__sendJobsToOptimizationMind( good_ids )
    if invalidJobList or nonauthJobList or bad_ids:
      result = S_ERROR( 'Some jobs failed resetting' )
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
