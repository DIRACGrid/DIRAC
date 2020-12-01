""" JobManagerHandler is the implementation of the JobManager service
    in the DISET framework

    The following methods are available in the Service interface

    submitJob()
    rescheduleJob()
    deleteJob()
    killJob()

"""

__RCSID__ = "$Id$"

import six
from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.DISET.MessageClient import MessageClient
from DIRAC.Core.Utilities.DErrno import EWMSJDL, EWMSSUBM
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.JEncode import strToIntDict
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.WorkloadManagementSystem.DB.PilotsLoggingDB import PilotsLoggingDB
from DIRAC.WorkloadManagementSystem.Utilities.ParametricJob import generateParametricJobs, getParameterVectorLength
from DIRAC.WorkloadManagementSystem.Service.JobPolicy import JobPolicy, \
    RIGHT_SUBMIT, RIGHT_RESCHEDULE, \
    RIGHT_DELETE, RIGHT_KILL, RIGHT_RESET

# This is a global instance of the JobDB class
gJobDB = None
gJobLoggingDB = None
gtaskQueueDB = None
gPilotAgentsDB = None
gPilotsLoggingDB = None
enablePilotsLogging = None

MAX_PARAMETRIC_JOBS = 20


def initializeJobManagerHandler(serviceInfo):
  """ Initialize

      :param dict serviceInfo: service information dictionary

      :return: S_OK()
  """
  global gJobDB, gJobLoggingDB, gtaskQueueDB, enablePilotsLogging, gPilotAgentsDB, gPilotsLoggingDB
  gJobDB = JobDB()
  gJobLoggingDB = JobLoggingDB()
  gtaskQueueDB = TaskQueueDB()
  gPilotAgentsDB = PilotAgentsDB()

  # there is a problem with accessing CS with shorter paths, so full path is extracted from serviceInfo dict
  enablePilotsLogging = gConfig.getValue(
      serviceInfo['serviceSectionPath'].replace(
          'JobManager',
          'PilotsLogging') + '/Enable',
      'False').lower() in ('yes', 'true')

  if enablePilotsLogging:
    gPilotsLoggingDB = PilotsLoggingDB()
  return S_OK()


class JobManagerHandler(RequestHandler):
  """ RequestHandler implementation of the JobManager
  """

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    cls.msgClient = MessageClient("WorkloadManagement/OptimizationMind")
    cls.__connectToOptMind()
    gThreadScheduler.addPeriodicTask(60, cls.__connectToOptMind)
    return S_OK()

  @classmethod
  def __connectToOptMind(cls):
    if not cls.msgClient.connected:
      result = cls.msgClient.connect(JobManager=True)
      if not result['OK']:
        cls.log.warn("Cannot connect to OptimizationMind!", result['Message'])

  def initialize(self):
    credDict = self.getRemoteCredentials()
    self.ownerDN = credDict['DN']
    self.ownerGroup = credDict['group']
    self.userProperties = credDict['properties']
    self.owner = credDict['username']
    self.peerUsesLimitedProxy = credDict['isLimitedProxy']
    self.diracSetup = self.serviceInfoDict['clientSetup']
    self.maxParametricJobs = self.srv_getCSOption('MaxParametricJobs', MAX_PARAMETRIC_JOBS)
    self.jobPolicy = JobPolicy(self.ownerDN, self.ownerGroup, self.userProperties)
    self.jobPolicy.jobDB = gJobDB
    return S_OK()

  def __sendJobsToOptimizationMind(self, jids):
    if not self.msgClient.connected:
      return
    result = self.msgClient.createMessage("OptimizeJobs")
    if not result['OK']:
      self.log.error("Cannot create Optimize message", result['Message'])
      return
    msgObj = result['Value']
    msgObj.jids = list(sorted(jids))
    result = self.msgClient.sendMessage(msgObj)
    if not result['OK']:
      self.log.error("Cannot send Optimize message", result['Message'])
      return
    self.log.info("Optimize msg sent", "for %s jobs" % len(jids))

  ###########################################################################
  types_getMaxParametricJobs = []

  def export_getMaxParametricJobs(self):
    """ Get the maximum number of parametric jobs

        :return: S_OK()/S_ERROR()
    """
    return S_OK(self.maxParametricJobs)

  types_submitJob = [basestring]

  def export_submitJob(self, jobDesc):
    """ Submit a job to DIRAC WMS.
        The job can be a single job, or a parametric job.
        If it is a parametric job, then the parameters will need to be unpacked.

        :param str jobDesc: job description JDL (of a single or parametric job)
        :return: S_OK/S_ERROR, a list of newly created job IDs in case of S_OK.
    """

    if self.peerUsesLimitedProxy:
      return S_ERROR(EWMSSUBM, "Can't submit using a limited proxy")

    # Check job submission permission
    result = self.jobPolicy.getJobPolicy()
    if not result['OK']:
      return S_ERROR(EWMSSUBM, 'Failed to get job policies')
    policyDict = result['Value']
    if not policyDict[RIGHT_SUBMIT]:
      return S_ERROR(EWMSSUBM, 'Job submission not authorized')

    # jobDesc is JDL for now
    jobDesc = jobDesc.strip()
    if jobDesc[0] != "[":
      jobDesc = "[%s" % jobDesc
    if jobDesc[-1] != "]":
      jobDesc = "%s]" % jobDesc

    # Check if the job is a parametric one
    jobClassAd = ClassAd(jobDesc)
    result = getParameterVectorLength(jobClassAd)
    if not result['OK']:
      self.log.error("Issue with getParameterVectorLength", result['Message'])
      return result
    nJobs = result['Value']
    parametricJob = False
    if nJobs > 0:
      # if we are here, then jobDesc was the description of a parametric job. So we start unpacking
      parametricJob = True
      if nJobs > self.maxParametricJobs:
        self.log.error("Maximum of parametric jobs exceeded:",
                       "limit %d smaller than number of jobs %d" % (self.maxParametricJobs, nJobs))
        return S_ERROR(EWMSJDL, "Number of parametric jobs exceeds the limit of %d" % self.maxParametricJobs)
      result = generateParametricJobs(jobClassAd)
      if not result['OK']:
        return result
      jobDescList = result['Value']
    else:
      # if we are here, then jobDesc was the description of a single job.
      jobDescList = [jobDesc]

    jobIDList = []

    if parametricJob:
      initialStatus = JobStatus.SUBMITTING
      initialMinorStatus = 'Bulk transaction confirmation'
    else:
      initialStatus = JobStatus.RECEIVED
      initialMinorStatus = 'Job accepted'

    for jobDescription in jobDescList:  # jobDescList because there might be a list generated by a parametric job
      result = gJobDB.insertNewJobIntoDB(jobDescription,
                                         self.owner,
                                         self.ownerDN,
                                         self.ownerGroup,
                                         self.diracSetup,
                                         initialStatus=initialStatus,
                                         initialMinorStatus=initialMinorStatus)
      if not result['OK']:
        return result

      jobID = result['JobID']
      self.log.info('Job added to the JobDB", "%s for %s/%s' % (jobID, self.ownerDN, self.ownerGroup))

      gJobLoggingDB.addLoggingRecord(jobID, result['Status'], result['MinorStatus'], source='JobManager')

      jobIDList.append(jobID)

    # Set persistency flag
    retVal = gProxyManager.getUserPersistence(self.ownerDN, self.ownerGroup)
    if 'Value' not in retVal or not retVal['Value']:
      gProxyManager.setPersistency(self.ownerDN, self.ownerGroup, True)

    if parametricJob:
      result = S_OK(jobIDList)
    else:
      result = S_OK(jobIDList[0])

    result['JobID'] = result['Value']
    result['requireProxyUpload'] = self.__checkIfProxyUploadIsRequired()
    # Ensure non-parametric jobs (i.e. non-bulk) get sent to optimizer immediately
    if not parametricJob:
      self.__sendJobsToOptimizationMind(jobIDList)
    return result

###########################################################################
  types_confirmBulkSubmission = [list]

  def export_confirmBulkSubmission(self, jobIDs):
    """ Confirm the possibility to proceed with processing of the jobs specified
        by the jobIDList

        :param list jobIDs: list of job IDs

        :return: S_OK(list)/S_ERROR() -- confirmed job IDs
    """
    jobList = self.__getJobList(jobIDs)
    if not jobList:
      self.log.error("Issue with __getJobList", ": invalid job specification %s" % str(jobIDs))
      return S_ERROR(EWMSSUBM, 'Invalid job specification: ' + str(jobIDs))

    validJobList, _invalidJobList, _nonauthJobList, _ownerJobList = self.jobPolicy.evaluateJobRights(jobList,
                                                                                                     RIGHT_SUBMIT)

    # Check that all the requested jobs are eligible
    if set(jobList) != set(validJobList):
      return S_ERROR(EWMSSUBM, 'Requested jobs for bulk transaction are not valid')

    result = gJobDB.getAttributesForJobList(jobList, ['Status', 'MinorStatus'])
    if not result['OK']:
      return S_ERROR(EWMSSUBM, 'Requested jobs for bulk transaction are not valid')
    js_dict = strToIntDict(result['Value'])

    # Check if the jobs are already activated
    jobEnabledList = [jobID for jobID in jobList
                      if js_dict[jobID]['Status'] in [JobStatus.RECEIVED,
                                                      JobStatus.CHECKING,
                                                      JobStatus.WAITING,
                                                      JobStatus.MATCHED,
                                                      JobStatus.RUNNING]]
    if set(jobEnabledList) == set(jobList):
      return S_OK(jobList)

    # Check that requested job are in Submitting status
    jobUpdateStatusList = list(jobID for jobID in jobList if js_dict[jobID]['Status'] == JobStatus.SUBMITTING)
    if set(jobUpdateStatusList) != set(jobList):
      return S_ERROR(EWMSSUBM, 'Requested jobs for bulk transaction are not valid')

    # Update status of all the requested jobs in one transaction
    result = gJobDB.setJobAttributes(jobUpdateStatusList,
                                     ['Status', 'MinorStatus'],
                                     [JobStatus.RECEIVED, 'Job accepted'])

    if not result['OK']:
      return result

    self.__sendJobsToOptimizationMind(jobUpdateStatusList)
    return S_OK(jobUpdateStatusList)

###########################################################################
  def __checkIfProxyUploadIsRequired(self):
    """ Check if an upload is required

        :return: bool
    """
    result = gProxyManager.userHasProxy(self.ownerDN, self.ownerGroup, validSeconds=18000)
    if not result['OK']:
      self.log.error("Can't check if the user has proxy uploaded", result['Message'])
      return True
    # Check if an upload is required
    return not result['Value']

###########################################################################

  @staticmethod
  def __getJobList(jobInput):
    """ Evaluate the jobInput into a list of ints

        :param jobInput: one or more job IDs in int or str form
        :type jobInput: str or int or list
        :return : a list of int job IDs
    """

    if isinstance(jobInput, int):
      return [jobInput]
    if isinstance(jobInput, six.string_types):
      try:
        ijob = int(jobInput)
        return [ijob]
      except BaseException:
        return []
    if isinstance(jobInput, list):
      try:
        ljob = [int(x) for x in jobInput]
        return ljob
      except BaseException:
        return []

    return []

###########################################################################
  types_rescheduleJob = []

  def export_rescheduleJob(self, jobIDs):
    """  Reschedule a single job. If the optional proxy parameter is given
         it will be used to refresh the proxy in the Proxy Repository

         :param list jobIDs: list of job IDs

         :return: S_OK()/S_ERROR() -- confirmed job IDs
    """

    jobList = self.__getJobList(jobIDs)
    if not jobList:
      return S_ERROR('Invalid job specification: ' + str(jobIDs))

    validJobList, invalidJobList, nonauthJobList, ownerJobList = self.jobPolicy.evaluateJobRights(jobList,
                                                                                                  RIGHT_RESCHEDULE)
    for jobID in validJobList:
      gtaskQueueDB.deleteJob(jobID)
      # gJobDB.deleteJobFromQueue(jobID)
      result = gJobDB.rescheduleJob(jobID)
      self.log.debug(str(result))
      if not result['OK']:
        return result
      gJobLoggingDB.addLoggingRecord(result['JobID'], status=result['Status'], minorStatus=result['MinorStatus'],
                                     applicationStatus='Unknown', source='JobManager')

    if invalidJobList or nonauthJobList:
      result = S_ERROR('Some jobs failed reschedule')
      if invalidJobList:
        result['InvalidJobIDs'] = invalidJobList
      if nonauthJobList:
        result['NonauthorizedJobIDs'] = nonauthJobList
      return result

    result = S_OK(validJobList)
    result['requireProxyUpload'] = len(ownerJobList) > 0 and self.__checkIfProxyUploadIsRequired()
    self.__sendJobsToOptimizationMind(validJobList)
    return result

  def __deleteJob(self, jobID):
    """ Delete one job

        :param int jobID: job ID

        :return: S_OK()/S_ERROR()
    """
    result = gJobDB.setJobStatus(jobID, JobStatus.DELETED, 'Checking accounting')
    if not result['OK']:
      return result

    result = gtaskQueueDB.deleteJob(jobID)
    if not result['OK']:
      self.log.warn('Failed to delete job from the TaskQueue')

    # if it was the last job for the pilot, clear PilotsLogging about it
    result = gPilotAgentsDB.getPilotsForJobID(jobID)
    if not result['OK']:
      self.log.error("Failed to get Pilots for JobID", result['Message'])
      return result
    for pilot in result['Value']:
      res = gPilotAgentsDB.getJobsForPilot(pilot)
      if not res['OK']:
        self.log.error("Failed to get jobs for pilot", res['Message'])
        return res
      if not res['Value']:  # if list of jobs for pilot is empty, delete pilot and pilotslogging
        result = gPilotAgentsDB.getPilotInfo(pilotID=pilot)
        if not result['OK']:
          self.log.error("Failed to get pilot info", result['Message'])
          return result
        pilotRef = result[0]['PilotJobReference']
        ret = gPilotAgentsDB.deletePilot(pilot)
        if not ret['OK']:
          self.log.error("Failed to delete pilot from PilotAgentsDB", ret['Message'])
          return ret
        if enablePilotsLogging:
          ret = gPilotsLoggingDB.deletePilotsLogging(pilotRef)
          if not ret['OK']:
            self.log.error("Failed to delete pilot logging from PilotAgentsDB", ret['Message'])
            return ret

    return S_OK()

  def __killJob(self, jobID, sendKillCommand=True):
    """  Kill one job

        :param int jobID: job ID
        :param bool sendKillCommand: send kill command

        :return: S_OK()/S_ERROR()
    """
    if sendKillCommand:
      result = gJobDB.setJobCommand(jobID, 'Kill')
      if not result['OK']:
        return result

    self.log.info('Job marked for termination', jobID)
    result = gJobDB.setJobStatus(jobID, JobStatus.KILLED, 'Marked for termination')
    if not result['OK']:
      self.log.warn('Failed to set job Killed status', result['Message'])
    result = gtaskQueueDB.deleteJob(jobID)
    if not result['OK']:
      self.log.warn('Failed to delete job from the TaskQueue', result['Message'])

    return S_OK()

  def __kill_delete_jobs(self, jobIDList, right):
    """ Kill or delete jobs as necessary

        :param list jobIDList: job IDs
        :param str right: right

        :return: S_OK()/S_ERROR()
    """
    jobList = self.__getJobList(jobIDList)
    if not jobList:
      return S_ERROR('Invalid job specification: ' + str(jobIDList))

    validJobList, invalidJobList, nonauthJobList, ownerJobList = self.jobPolicy.evaluateJobRights(jobList, right)

    # Get job status to see what is to be killed or deleted
    result = gJobDB.getAttributesForJobList(validJobList, ['Status'])
    if not result['OK']:
      return result
    killJobList = []
    deleteJobList = []
    markKilledJobList = []
    stagingJobList = []
    for jobID, sDict in result['Value'].items():  # can be an iterator
      if sDict['Status'] in (JobStatus.RUNNING, JobStatus.MATCHED, JobStatus.STALLED):
        killJobList.append(jobID)
      elif sDict['Status'] in (JobStatus.DONE, JobStatus.FAILED, JobStatus.KILLED):
        if not right == RIGHT_KILL:
          deleteJobList.append(jobID)
      else:
        markKilledJobList.append(jobID)
      if sDict['Status'] in ['Staging']:
        stagingJobList.append(jobID)

    badIDs = []
    for jobID in markKilledJobList:
      result = self.__killJob(jobID, sendKillCommand=False)
      if not result['OK']:
        badIDs.append(jobID)

    for jobID in killJobList:
      result = self.__killJob(jobID)
      if not result['OK']:
        badIDs.append(jobID)

    for jobID in deleteJobList:
      result = self.__deleteJob(jobID)
      if not result['OK']:
        badIDs.append(jobID)

    if stagingJobList:
      stagerClient = StorageManagerClient()
      self.log.info('Going to send killing signal to stager as well!')
      result = stagerClient.killTasksBySourceTaskID(stagingJobList)
      if not result['OK']:
        self.log.warn('Failed to kill some Stager tasks', result['Message'])

    if nonauthJobList or badIDs:
      result = S_ERROR('Some jobs failed deletion')
      if nonauthJobList:
        self.log.warn("Non-authorized JobIDs won't be deleted", str(nonauthJobList))
        result['NonauthorizedJobIDs'] = nonauthJobList
      if badIDs:
        self.log.warn("JobIDs failed to be deleted", str(badIDs))
        result['FailedJobIDs'] = badIDs
      return result

    result = S_OK(validJobList)
    result['requireProxyUpload'] = len(ownerJobList) > 0 and self.__checkIfProxyUploadIsRequired()

    if invalidJobList:
      result['InvalidJobIDs'] = invalidJobList

    return result

###########################################################################
  types_deleteJob = []

  def export_deleteJob(self, jobIDs):
    """ Delete jobs specified in the jobIDs list

        :param list jobIDs: list of job IDs

        :return: S_OK/S_ERROR
    """

    return self.__kill_delete_jobs(jobIDs, RIGHT_DELETE)

###########################################################################
  types_killJob = []

  def export_killJob(self, jobIDs):
    """ Kill jobs specified in the jobIDs list

        :param list jobIDs: list of job IDs

        :return: S_OK/S_ERROR
    """

    return self.__kill_delete_jobs(jobIDs, RIGHT_KILL)

###########################################################################
  types_resetJob = []

  def export_resetJob(self, jobIDs):
    """ Reset jobs specified in the jobIDs list

        :param list jobIDs: list of job IDs

        :return: S_OK/S_ERROR
    """

    jobList = self.__getJobList(jobIDs)
    if not jobList:
      return S_ERROR('Invalid job specification: ' + str(jobIDs))

    validJobList, invalidJobList, nonauthJobList, ownerJobList = self.jobPolicy.evaluateJobRights(jobList,
                                                                                                  RIGHT_RESET)

    badIDs = []
    good_ids = []
    for jobID in validJobList:
      result = gJobDB.setJobAttribute(jobID, 'RescheduleCounter', -1)
      if not result['OK']:
        badIDs.append(jobID)
      else:
        gtaskQueueDB.deleteJob(jobID)
        # gJobDB.deleteJobFromQueue(jobID)
        result = gJobDB.rescheduleJob(jobID)
        if not result['OK']:
          badIDs.append(jobID)
        else:
          good_ids.append(jobID)
        gJobLoggingDB.addLoggingRecord(result['JobID'], status=result['Status'], minorStatus=result['MinorStatus'],
                                       applicationStatus='Unknown', source='JobManager')

    self.__sendJobsToOptimizationMind(good_ids)
    if invalidJobList or nonauthJobList or badIDs:
      result = S_ERROR('Some jobs failed resetting')
      if invalidJobList:
        result['InvalidJobIDs'] = invalidJobList
      if nonauthJobList:
        result['NonauthorizedJobIDs'] = nonauthJobList
      if badIDs:
        result['FailedJobIDs'] = badIDs
      return result

    result = S_OK()
    result['requireProxyUpload'] = len(ownerJobList) > 0 and self.__checkIfProxyUploadIsRequired()
    return result
