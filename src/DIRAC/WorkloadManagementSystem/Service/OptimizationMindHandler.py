from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
__RCSID__ = "$Id$"

from past.builtins import long
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import ThreadScheduler
from DIRAC.Core.Base.ExecutorMindHandler import ExecutorMindHandler
from DIRAC.WorkloadManagementSystem.Client.JobState.JobState import JobState
from DIRAC.WorkloadManagementSystem.Client.JobState.CachedJobState import CachedJobState
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB


def cleanTaskQueues():
  tqDB = TaskQueueDB()
  jobDB = JobDB()
  logDB = JobLoggingDB()

  result = tqDB.enableAllTaskQueues()
  if not result['OK']:
    return result
  result = tqDB.findOrphanJobs()
  if not result['OK']:
    return result
  for jid in result['Value']:
    result = tqDB.deleteJob(jid)
    if not result['OK']:
      gLogger.error("Cannot delete from TQ job %s" % jid, result['Message'])
      continue
    result = jobDB.rescheduleJob(jid)
    if not result['OK']:
      gLogger.error("Cannot reschedule in JobDB job %s" % jid, result['Message'])
      continue
    result = logDB.addLoggingRecord(jid, JobStatus.RECEIVED, "", "", source="JobState")
    if not result['OK']:
      gLogger.error("Cannot add logging record in JobLoggingDB %s" % jid, result['Message'])
      continue
  return S_OK()


class OptimizationMindHandler(ExecutorMindHandler):

  __optimizationStates = ['Received', 'Checking']
  __loadTaskId = False

  MSG_DEFINITIONS = {'OptimizeJobs': {'jids': (list, tuple)}}

  auth_msg_OptimizeJobs = ['all']

  def msg_OptimizeJobs(self, msgObj):
    jids = msgObj.jids
    for jid in jids:
      try:
        jid = int(jid)
      except ValueError:
        self.log.error("Job ID %s has to be an integer" % jid)
        continue
      # Forget and add task to ensure state is reset
      self.forgetTask(jid)
      result = self.executeTask(jid, CachedJobState(jid))
      if not result['OK']:
        self.log.error("Could not add job %s to optimization: %s" % (jid, result['Value']))
      else:
        self.log.info("Received new job %s" % jid)
    return S_OK()

  @classmethod
  def __loadJobs(cls, eTypes=None):
    log = cls.log
    if cls.__loadTaskId:
      period = cls.srv_getCSOption("LoadJobPeriod", 300)
      ThreadScheduler.gThreadScheduler.setTaskPeriod(cls.__loadTaskId, period)
    if not eTypes:
      eConn = cls.getExecutorsConnected()
      eTypes = [eType for eType in eConn if eConn[eType] > 0]
    if not eTypes:
      log.info("No optimizer connected. Skipping load")
      return S_OK()
    log.info("Getting jobs for %s" % ",".join(eTypes))
    checkingMinors = [eType.split("/")[1] for eType in eTypes if eType != "WorkloadManagement/JobPath"]
    for opState in cls.__optimizationStates:
      # For Received states
      if opState == JobStatus.RECEIVED:
        if 'WorkloadManagement/JobPath' not in eTypes:
          continue
        jobCond = {'Status': opState}
      # For checking states
      if opState == JobStatus.CHECKING:
        if not checkingMinors:
          continue
        jobCond = {'Status': opState, 'MinorStatus': checkingMinors}
      # Do the magic
      jobTypeCondition = cls.srv_getCSOption("JobTypeRestriction", [])
      if jobTypeCondition:
        jobCond['JobType'] = jobTypeCondition
      result = cls.__jobDB.selectJobs(jobCond, limit=cls.srv_getCSOption("JobQueryLimit", 10000))
      if not result['OK']:
        return result
      jidList = result['Value']
      knownJids = cls.getTaskIds()
      added = 0
      for jid in jidList:
        jid = long(jid)
        if jid not in knownJids:
          # Same as before. Check that the state is ok.
          cls.executeTask(jid, CachedJobState(jid))
          added += 1
      log.info("Added %s/%s jobs for %s state" % (added, len(jidList), opState))
    return S_OK()

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    try:
      from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
      cls.__jobDB = JobDB()
    except Exception as excp:
      return S_ERROR("Could not connect to JobDB: %s" % str(excp))
    cls.setFailedOnTooFrozen(False)
    cls.setFreezeOnFailedDispatch(False)
    cls.setFreezeOnUnknownExecutor(False)
    cls.setAllowedClients("JobManager")
    cleanTaskQueues()
    period = cls.srv_getCSOption("LoadJobPeriod", 60)
    result = ThreadScheduler.gThreadScheduler.addPeriodicTask(period, cls.__loadJobs)
    if not result['OK']:
      return result
    cls.__loadTaskId = result['Value']
    return cls.__loadJobs()

  @classmethod
  def exec_executorConnected(cls, trid, eTypes):
    return cls.__loadJobs(eTypes)

  @classmethod
  def exec_taskProcessed(cls, jid, jobState, eType):
    cls.log.info("Saving changes for job %s after %s" % (jid, eType))
    result = jobState.commitChanges()
    if not result['OK']:
      cls.log.error("Could not save changes for job", "%s: %s" % (jid, result['Message']))
    return result

  @classmethod
  def exec_taskFreeze(cls, jid, jobState, eType):
    cls.log.info("Saving changes for job %s before freezing from %s" % (jid, eType))
    result = jobState.commitChanges()
    if not result['OK']:
      cls.log.error("Could not save changes for job", "%s: %s" % (jid, result['Message']))
    return result

  @classmethod
  def exec_dispatch(cls, jid, jobState, pathExecuted):
    result = jobState.getStatus()
    if not result['OK']:
      cls.log.error("Could not get status for job", "%s: %s" % (jid, result['Message']))
      return S_ERROR("Could not retrieve status: %s" % result['Message'])
    status, minorStatus = result['Value']
    # If not in proper state then end chain
    if status not in cls.__optimizationStates:
      cls.log.info("Dispatching job %s out of optimization" % jid)
      return S_OK()
    # If received send to JobPath
    if status == JobStatus.RECEIVED:
      cls.log.info("Dispatching job %s to JobPath" % jid)
      return S_OK("WorkloadManagement/JobPath")
    result = jobState.getOptParameter('OptimizerChain')
    if not result['OK']:
      cls.log.error("Could not get optimizer chain for job, auto resetting job", "%s: %s" % (jid, result['Message']))
      result = jobState.resetJob()
      if not result['OK']:
        cls.log.error("Could not reset job", "%s: %s" % (jid, result['Message']))
        return S_ERROR("Cound not get OptimizationChain or reset job %s" % jid)
      return S_OK("WorkloadManagement/JobPath")
    optChain = result['Value']
    if minorStatus not in optChain:
      cls.log.error("Next optimizer is not in the chain for job", "%s: %s not in %s" % (jid, minorStatus, optChain))
      return S_ERROR("Next optimizer %s not in chain %s" % (minorStatus, optChain))
    cls.log.info("Dispatching job %s to %s" % (jid, minorStatus))
    return S_OK("WorkloadManagement/%s" % minorStatus)

  @classmethod
  def exec_prepareToSend(cls, jid, jobState, eId):
    return jobState.recheckValidity()

  @classmethod
  def exec_serializeTask(cls, jobState):
    return S_OK(jobState.serialize())

  @classmethod
  def exec_deserializeTask(cls, taskStub):
    return CachedJobState.deserialize(taskStub)

  @classmethod
  def exec_taskError(cls, jid, cachedJobState, errorMsg):
    result = cachedJobState.commitChanges()
    if not result['OK']:
      cls.log.error("Cannot write changes to job %s: %s" % (jid, result['Message']))
    jobState = JobState(jid)
    result = jobState.getStatus()
    if result['OK']:
      if result['Value'][0].lower() == "failed":
        return S_OK()
    else:
      cls.log.error("Could not get status of job %s: %s" % (jid, result['Message ']))
    cls.log.notice("Job %s: Setting to Failed|%s" % (jid, errorMsg))
    return jobState.setStatus("Failed", errorMsg, source='OptimizationMindHandler')
