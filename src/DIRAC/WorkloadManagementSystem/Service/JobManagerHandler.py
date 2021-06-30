""" JobManagerHandler is the implementation of the JobManager service
    in the DISET framework

    The following methods are available in the Service interface

    submitJob()
    rescheduleJob()
    deleteJob()
    killJob()

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.DISET.MessageClient import MessageClient
from DIRAC.Core.Utilities.DErrno import EWMSJDL, EWMSSUBM
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.JEncode import strToIntDict
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.StorageManagementSystem.Client.StorageManagerClient import StorageManagerClient
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Utilities.ParametricJob import generateParametricJobs, getParameterVectorLength
from DIRAC.WorkloadManagementSystem.Service.JobPolicy import JobPolicy, \
    RIGHT_SUBMIT, RIGHT_RESCHEDULE, \
    RIGHT_DELETE, RIGHT_KILL, RIGHT_RESET

MAX_PARAMETRIC_JOBS = 20


class JobManagerHandler(RequestHandler):
  """ RequestHandler implementation of the JobManager
  """

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    """ Initialization of DB objects and OptimizationMind
    """
    try:
      result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobDB", "JobDB")
      if not result['OK']:
        return result
      cls.jobDB = result['Value']()

      result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.JobLoggingDB", "JobLoggingDB")
      if not result['OK']:
        return result
      cls.jobLoggingDB = result['Value']()

      result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.TaskQueueDB", "TaskQueueDB")
      if not result['OK']:
        return result
      cls.taskQueueDB = result['Value']()

      result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.PilotAgentsDB", "PilotAgentsDB")
      if not result['OK']:
        return result
      cls.pilotAgentsDB = result['Value']()

    except RuntimeError as excp:
      return S_ERROR("Can't connect to DB: %s" % excp)

    cls.pilotsLoggingDB = None
    enablePilotsLogging = Operations().getValue(
        '/Services/JobMonitoring/usePilotsLoggingFlag', False)
    if enablePilotsLogging:
      try:
        result = ObjectLoader().loadObject("WorkloadManagementSystem.DB.PilotsLoggingDB", "PilotsLoggingDB")
        if not result['OK']:
          return result
        cls.pilotsLoggingDB = result['Value']()
      except RuntimeError as excp:
        return S_ERROR("Can't connect to DB: %s" % excp)

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
    self.jobPolicy.jobDB = self.jobDB
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

  types_submitJob = [six.string_types]

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
      result = self.jobDB.insertNewJobIntoDB(jobDescription,
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

      self.jobLoggingDB.addLoggingRecord(
          jobID, result['Status'], result['MinorStatus'], source='JobManager')

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
      return S_ERROR(
          EWMSSUBM, 'Requested jobs for bulk transaction are not valid')

    result = self.jobDB.getAttributesForJobList(
        jobList, ['Status', 'MinorStatus'])
    if not result['OK']:
      return S_ERROR(
          EWMSSUBM, 'Requested jobs for bulk transaction are not valid')
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
    result = self.jobDB.setJobAttributes(jobUpdateStatusList,
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
      except ValueError:
        return []
    if isinstance(jobInput, list):
      try:
        ljob = [int(x) for x in jobInput]
        return ljob
      except ValueError:
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
      self.taskQueueDB.deleteJob(jobID)
      # gJobDB.deleteJobFromQueue(jobID)
      result = self.jobDB.rescheduleJob(jobID)
      self.log.debug(str(result))
      if not result['OK']:
        return result
      self.jobLoggingDB.addLoggingRecord(
          result['JobID'], status=result['Status'], minorStatus=result['MinorStatus'],
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

  types_removeJob = []

  def export_removeJob(self, jobIDs):
    """
    Completely remove a list of jobs, also from TaskQueueDB,
    and including its JobLogging info.
    Only authorized users are allowed to remove jobs.

    :param list jobIDs: list of job IDs
    :return: S_OK()/S_ERROR() -- confirmed job IDs
    """

    jobList = self.__getJobList(jobIDs)
    if not jobList:
      return S_ERROR('Invalid job specification: ' + str(jobIDs))

    validJobList, invalidJobList, nonauthJobList, _ = self.jobPolicy.evaluateJobRights(jobList,
                                                                                       RIGHT_DELETE)
    count = 0
    error_count = 0

    if validJobList:
      self.log.verbose("Removing jobs", "(n=%d)" % len(validJobList))
      result = self.jobDB.removeJobFromDB(validJobList)
      if not result['OK']:
        self.log.error("Failed to remove jobs from JobDB", "(n=%d)" % len(validJobList))
      else:
        self.log.info("Removed jobs from JobDB", "(n=%d)" % len(validJobList))

      for jobID in validJobList:
        resultTQ = self.taskQueueDB.deleteJob(jobID)
        if not resultTQ['OK']:
          self.log.warn("Failed to remove job from TaskQueueDB",
                        "(%d): %s" % (jobID, resultTQ['Message']))
          error_count += 1
        else:
          count += 1

      result = self.jobLoggingDB.deleteJob(validJobList)
      if not result['OK']:
        self.log.error("Failed to remove jobs from JobLoggingDB", "(n=%d)" % len(validJobList))
      else:
        self.log.info("Removed jobs from JobLoggingDB", "(n=%d)" % len(validJobList))

      if count > 0 or error_count > 0:
        self.log.info("Removed jobs from DB",
                      "(%d jobs with %d errors)" % (count, error_count))

    if invalidJobList or nonauthJobList:
      self.log.error(
          "Jobs can not be removed",
          ": %d invalid and %d in nonauthJobList" % (len(invalidJobList), len(nonauthJobList)))
      errMsg = "Some jobs failed removal"
      res = S_ERROR()
      if invalidJobList:
        self.log.debug("Invalid jobs: %s" % ','.join(str(ij) for ij in invalidJobList))
        res['InvalidJobIDs'] = invalidJobList
        errMsg += ": invalid jobs"
      if nonauthJobList:
        self.log.debug("nonauthJobList jobs: %s" % ','.join(str(nj) for nj in nonauthJobList))
        res['NonauthorizedJobIDs'] = nonauthJobList
        errMsg += ": non-authorized jobs"
      res['Message'] = errMsg
      return res

    return S_OK(validJobList)

  def __deleteJob(self, jobID):
    """ Set the job status to "Deleted"
    and remove the pilot that ran and its logging info if the pilot is finished.

    :param int jobID: job ID
    :return: S_OK()/S_ERROR()
    """
    result = self.jobDB.setJobStatus(
        jobID, JobStatus.DELETED, 'Checking accounting')
    if not result['OK']:
      return result

    result = self.taskQueueDB.deleteJob(jobID)
    if not result['OK']:
      self.log.warn('Failed to delete job from the TaskQueue')

    # if it was the last job for the pilot, clear PilotsLogging about it
    result = self.pilotAgentsDB.getPilotsForJobID(jobID)
    if not result['OK']:
      self.log.error("Failed to get Pilots for JobID", result['Message'])
      return result
    for pilot in result['Value']:
      res = self.pilotAgentsDB.getJobsForPilot(pilot)
      if not res['OK']:
        self.log.error("Failed to get jobs for pilot", res['Message'])
        return res
      if not res['Value']:  # if list of jobs for pilot is empty, delete pilot and pilotslogging
        result = self.pilotAgentsDB.getPilotInfo(pilotID=pilot)
        if not result['OK']:
          self.log.error("Failed to get pilot info", result['Message'])
          return result
        pilotRef = result[0]['PilotJobReference']
        ret = self.pilotAgentsDB.deletePilot(pilot)
        if not ret['OK']:
          self.log.error("Failed to delete pilot from PilotAgentsDB", ret['Message'])
          return ret
        if self.pilotsLoggingDB:
          ret = self.pilotsLoggingDB.deletePilotsLogging(pilotRef)
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
      result = self.jobDB.setJobCommand(jobID, 'Kill')
      if not result['OK']:
        return result

    self.log.info('Job marked for termination', jobID)
    result = self.jobDB.setJobStatus(jobID, JobStatus.KILLED, 'Marked for termination')
    if not result['OK']:
      self.log.warn('Failed to set job Killed status', result['Message'])
    result = self.taskQueueDB.deleteJob(jobID)
    if not result['OK']:
      self.log.warn('Failed to delete job from the TaskQueue', result['Message'])

    return S_OK()

  def __kill_delete_jobs(self, jobIDList, right):
    """ Kill (== set the status to "KILLED") or delete (== set the status to "DELETED") jobs as necessary

        :param list jobIDList: job IDs
        :param str right: right

        :return: S_OK()/S_ERROR()
    """
    jobList = self.__getJobList(jobIDList)
    if not jobList:
      return S_ERROR('Invalid job specification: ' + str(jobIDList))

    validJobList, invalidJobList, nonauthJobList, ownerJobList = self.jobPolicy.evaluateJobRights(jobList, right)

    badIDs = []

    if validJobList:
      # Get job status to see what is to be killed or deleted
      result = self.jobDB.getAttributesForJobList(validJobList, ['Status'])
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
        if sDict['Status'] in [JobStatus.STAGING]:
          stagingJobList.append(jobID)

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
      result = self.jobDB.setJobAttribute(jobID, 'RescheduleCounter', -1)
      if not result['OK']:
        badIDs.append(jobID)
      else:
        self.taskQueueDB.deleteJob(jobID)
        # gJobDB.deleteJobFromQueue(jobID)
        result = self.jobDB.rescheduleJob(jobID)
        if not result['OK']:
          badIDs.append(jobID)
        else:
          good_ids.append(jobID)
        self.jobLoggingDB.addLoggingRecord(
            result['JobID'], status=result['Status'], minorStatus=result['MinorStatus'],
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
