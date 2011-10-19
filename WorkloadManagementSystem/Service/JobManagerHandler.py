########################################################################
# $HeadURL$
########################################################################

""" JobManagerHandler is the implementation of the JobManager service
    in the DISET framework

    The following methods are available in the Service interface

    submitJob()
    rescheduleJob()
    deleteJob()
    killJob()

"""

__RCSID__ = "$Id$"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB     import TaskQueueDB
from DIRAC.WorkloadManagementSystem.Service.JobPolicy import JobPolicy, RIGHT_SUBMIT, RIGHT_RESCHEDULE, \
                                                                        RIGHT_DELETE, RIGHT_KILL, RIGHT_RESET
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager

# This is a global instance of the JobDB class
gJobDB = False
gJobLoggingDB = False
gtaskQueueDB = False

MAX_PARAMETRIC_JOBS = 20

def initializeJobManagerHandler( serviceInfo ):

  global gJobDB, gJobLoggingDB, gtaskQueueDB
  gJobDB = JobDB()
  gJobLoggingDB = JobLoggingDB()
  gtaskQueueDB  = TaskQueueDB()
  return S_OK()

class JobManagerHandler( RequestHandler ):

  def initialize(self):
    credDict = self.getRemoteCredentials()
    self.ownerDN = credDict['DN']
    self.ownerGroup = credDict['group']
    self.userProperties = credDict[ 'properties' ]
    self.owner = credDict[ 'username' ]
    self.peerUsesLimitedProxy = credDict[ 'isLimitedProxy' ]
    self.diracSetup = self.serviceInfoDict['clientSetup']
    serviceSectionPath = self.serviceInfoDict['serviceSectionPath']
    self.maxParametricJobs = gConfig.getValue('%s/MaxParametricJobs'%serviceSectionPath,MAX_PARAMETRIC_JOBS)
    self.jobPolicy = JobPolicy( self.ownerDN, self.ownerGroup, self.userProperties )

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
      return S_ERROR('Job submission not authorized')

    #jobDesc is JDL for now
    jobDesc = jobDesc.strip()
    if jobDesc[0] != "[":
      jobDesc = "[%s" % jobDesc
    if jobDesc[-1] != "]":
      jobDesc = "%s]" % jobDesc

    # Check if the job is a parameteric one
    jobClassAd = ClassAd(jobDesc)
    parametricJob = False
    if jobClassAd.lookupAttribute('Parameters'):
      parametricJob = True
      if jobClassAd.isAttributeList('Parameters'):
        parameterList = jobClassAd.getListFromExpression('Parameters')
      else:
        pStep = 0
        pFactor = 1
        nParameters = jobClassAd.getAttributeInt('Parameters')
        if not nParameters:
          value = jobClassAd.get_expression('Parameters')
          return S_ERROR('Illegal value for Parameters JDL field: %s' % value)
        
        if jobClassAd.lookupAttribute('ParameterStart'):
          value = jobClassAd.get_expression('ParameterStart').replace('"','')
          try:
            pStart = int(value)
          except:
            try:
              pStart = float(value)
            except:   
              return S_ERROR('Illegal value for ParameterStart JDL field: %s' % value)
            
        if jobClassAd.lookupAttribute('ParameterStep'):  
          pStep = jobClassAd.getAttributeInt('ParameterStep')
          if not pStep:
            pStep = jobClassAd.getAttributeFloat('ParameterStep')
            if not pStep:
              value = jobClassAd.get_expression('ParameterStep')
              return S_ERROR('Illegal value for ParameterStep JDL field: %s' % value)
        if jobClassAd.lookupAttribute('ParameterFactor'):  
          pFactor = jobClassAd.getAttributeInt('ParameterFactor')
          if not pFactor:
            pFactor = jobClassAd.getAttributeFloat('ParameterFactor')
            if not pFactor:
              value = jobClassAd.get_expression('ParameterFactor')
              return S_ERROR('Illegal value for ParameterFactor JDL field: %s' % value) 
        
        parameterList = list()
        parameterList.append(pStart)
        for i in range(nParameters-1):
          parameterList.append(parameterList[i]*pFactor+pStep)
        

      if len(parameterList) > self.maxParametricJobs:
        return S_ERROR('The number of parametric jobs exceeded the limit of %d' % self.maxParametricJobs  )  
        
      jobDescList = []
      for n,p in enumerate(parameterList):
        jobDescList.append( jobDesc.replace('%s',str(p)).replace('%n',str(n)) )
    else:
      jobDescList = [ jobDesc ]     

    jobIDList = []
    for jobDescription in jobDescList:
      result = gJobDB.insertNewJobIntoDB( jobDescription, self.owner, self.ownerDN, self.ownerGroup, self.diracSetup )
      if not result['OK']:
        return result

      jobID = result['JobID']
      gLogger.info( 'Job %s added to the JobDB for %s/%s' % ( jobID, self.ownerDN, self.ownerGroup ) )

      gJobLoggingDB.addLoggingRecord( jobID, result['Status'], result['MinorStatus'], source = 'JobManager' )

      jobIDList.append(jobID)

    #Set persistency flag
    retVal = gProxyManager.getUserPersistence( self.ownerDN, self.ownerGroup )
    if 'Value' not in retVal or not retVal[ 'Value' ]:
      gProxyManager.setPersistency( self.ownerDN, self.ownerGroup, True )

    if parametricJob:
      result = S_OK(jobIDList)
    else:
      result = S_OK(jobIDList[0])

    result['JobID'] = result['Value']
    result[ 'requireProxyUpload' ] = self.__checkIfProxyUploadIsRequired()
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
    """ Get access rights to jobID for the user ownerDN/ownerGroup
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

    return validJobList,invalidJobList,nonauthJobList,ownerJobList

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
                                                                        RIGHT_RESCHEDULE )
    for jobID in validJobList:
      gtaskQueueDB.deleteJob(jobID)
      #gJobDB.deleteJobFromQueue(jobID)
      result  = gJobDB.rescheduleJob( jobID )
      gLogger.debug( str( result ) )
      if not result['OK']:
        return result
      gJobLoggingDB.addLoggingRecord( result['JobID'], result['Status'], result['MinorStatus'], 
                                      application = 'Unknown', source = 'JobManager' )

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
                                                                        RIGHT_DELETE)

    bad_ids = []
    good_ids = []
    for jobID in validJobList:
      result = gJobDB.setJobStatus(jobID,'Deleted','Checking accounting')
      if not result['OK']:
        bad_ids.append(jobID)
      else:
        good_ids.append(jobID)
      #result = gJobDB.deleteJobFromQueue(jobID)
      #if not result['OK']:
      #  gLogger.warn('Failed to delete job from the TaskQueue (old)')
      result = gtaskQueueDB.deleteJob(jobID)
      if not result['OK']:
        gLogger.warn('Failed to delete job from the TaskQueue')

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
                                                                             RIGHT_KILL )

    bad_ids = []
    good_ids = []
    for jobID in validJobList:
      # kill jobID
      result = gJobDB.setJobCommand(jobID,'Kill')
      if not result['OK']:
        bad_ids.append(jobID)
      else:
        gLogger.info('Job %d is marked for termination' % jobID)
        good_ids.append(jobID)
        result = gJobDB.setJobStatus(jobID,'Killed','Marked for termination')
        if not result['OK']:
          gLogger.warn('Failed to set job status')
        #result = gJobDB.deleteJobFromQueue(jobID)
        #if not result['OK']:
        #  gLogger.warn('Failed to delete job from the TaskQueue (old)')
        result = gtaskQueueDB.deleteJob(jobID)
        if not result['OK']:
          gLogger.warn('Failed to delete job from the TaskQueue')

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
                                                                        RIGHT_RESET)

    bad_ids = []
    good_ids = []
    for jobID in validJobList:
      result = gJobDB.setJobAttribute(jobID,'RescheduleCounter',0)
      if not result['OK']:
        bad_ids.append(jobID)
      else:
        gtaskQueueDB.deleteJob(jobID)
        #gJobDB.deleteJobFromQueue(jobID)
        result  = gJobDB.rescheduleJob( jobID )
        if not result['OK']:
          bad_ids.append(jobID)
        else:
          good_ids.append(jobID)
        gJobLoggingDB.addLoggingRecord( result['JobID'], result['Status'], result['MinorStatus'], 
                                        application = 'Unknown', source = 'JobManager' )  

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
