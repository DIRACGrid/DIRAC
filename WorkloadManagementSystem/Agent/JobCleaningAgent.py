########################################################################
# File :    JobCleaningAgent.py
# Author :  A.T.
########################################################################
""" The Job Cleaning Agent controls removing jobs from the WMS in the end of their life cycle.

    This agent will take care of removing user jobs, while production jobs should be removed through the
    :mod:`~DIRAC.TransformationSystem.Agent.TransformationCleaningAgent`.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN JobCleaningAgent
  :end-before: ##END
  :dedent: 2
  :caption: JobCleaningAgent options


Cleaning HeartBeatLoggingInfo
-----------------------------

If the HeartBeatLoggingInfo table of the JobDB is too large, the information for finished jobs can be removed (including
for transformation related jobs). In vanilla DIRAC the HeartBeatLoggingInfo is only used by the StalledJobAgent. For
this purpose the options MaxHBJobsAtOnce and RemoveStatusDelayHB/[Done|Killed|Failed] should be set to values larger
than 0.

"""

from __future__ import absolute_import
__RCSID__ = "$Id$"

import time
import os

from DIRAC import S_OK, gLogger
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient

import DIRAC.Core.Utilities.Time as Time


class JobCleaningAgent(AgentModule):
  """
      The specific agents must provide the following methods:

         *  initialize() for initial settings
         *  beginExecution()
         *  execute() - the main method called in the agent cycle
         *  endExecution()
         *  finalize() - the graceful exit of the method, this one is usually used for the agent restart
  """

  def __init__(self, *args, **kwargs):
    """ c'tor
    """
    AgentModule.__init__(self, *args, **kwargs)

    # clients
    self.jobDB = None
    self.taskQueueDB = None
    self.jobLoggingDB = None

    self.maxJobsAtOnce = 100
    self.jobByJob = False
    self.throttlingPeriod = 0.

    self.prodTypes = []

    self.removeStatusDelay = {}
    self.removeStatusDelayHB = {}

  #############################################################################
  def initialize(self):
    """ Sets defaults
    """

    self.am_setOption("PollingTime", 120)
    self.jobDB = JobDB()
    self.taskQueueDB = TaskQueueDB()
    self.jobLoggingDB = JobLoggingDB()
    # self.sandboxDB = SandboxDB( 'SandboxDB' )
    agentTSTypes = self.am_getOption('ProductionTypes', [])
    if agentTSTypes:
      self.prodTypes = agentTSTypes
    else:
      self.prodTypes = Operations().getValue(
          'Transformations/DataProcessing', ['MCSimulation', 'Merge'])
    gLogger.info("Will exclude the following Production types from cleaning %s" % (
        ', '.join(self.prodTypes)))
    self.maxJobsAtOnce = self.am_getOption('MaxJobsAtOnce', 500)
    self.jobByJob = self.am_getOption('JobByJob', False)
    self.throttlingPeriod = self.am_getOption('ThrottlingPeriod', 0.)

    self.removeStatusDelay['Done'] = self.am_getOption('RemoveStatusDelay/Done', 7)
    self.removeStatusDelay['Killed'] = self.am_getOption('RemoveStatusDelay/Killed', 7)
    self.removeStatusDelay['Failed'] = self.am_getOption('RemoveStatusDelay/Failed', 7)
    self.removeStatusDelay['Any'] = self.am_getOption('RemoveStatusDelay/Any', -1)

    self.removeStatusDelayHB['Done'] = self.am_getOption('RemoveStatusDelayHB/Done', -1)
    self.removeStatusDelayHB['Killed'] = self.am_getOption('RemoveStatusDelayHB/Killed', -1)
    self.removeStatusDelayHB['Failed'] = self.am_getOption('RemoveStatusDelayHB/Failed', -1)
    self.maxHBJobsAtOnce = self.am_getOption('MaxHBJobsAtOnce', 0)

    return S_OK()

  def _getAllowedJobTypes(self):
    """ Get valid jobTypes
    """
    result = self.jobDB.getDistinctJobAttributes('JobType')
    if not result['OK']:
      return result
    cleanJobTypes = []
    for jobType in result['Value']:
      if jobType not in self.prodTypes:
        cleanJobTypes.append(jobType)
    self.log.notice("JobTypes to clean %s" % cleanJobTypes)
    return S_OK(cleanJobTypes)

  #############################################################################
  def execute(self):
    """ Remove jobs in various status
    """
    # Delete jobs in "Deleted" state
    result = self.removeJobsByStatus({'Status': 'Deleted'})
    if not result['OK']:
      return result

    # Get all the Job types that can be cleaned
    result = self._getAllowedJobTypes()
    if not result['OK']:
      return result

    # No jobs in the system subject to removal
    if not result['Value']:
      return S_OK()

    baseCond = {'JobType': result['Value']}
    # Remove jobs with final status
    for status in self.removeStatusDelay:
      delay = self.removeStatusDelay[status]
      if delay < 0:
        # Negative delay means don't delete anything...
        continue
      condDict = dict(baseCond)
      if status != 'Any':
        condDict['Status'] = status
      delTime = str(Time.dateTime() - delay * Time.day)
      result = self.removeJobsByStatus(condDict, delTime)
      if not result['OK']:
        gLogger.warn('Failed to remove jobs in status %s' % status)

    if self.maxHBJobsAtOnce > 0:
      for status, delay in self.removeStatusDelayHB.items():
        if delay > 0:
          self.removeHeartBeatLoggingInfo(status, delay)

    return S_OK()

  def removeJobsByStatus(self, condDict, delay=False):
    """ Remove deleted jobs
    """
    if delay:
      gLogger.verbose("Removing jobs with %s and older than %s day(s)" % (condDict, delay))
      result = self.jobDB.selectJobs(condDict, older=delay, limit=self.maxJobsAtOnce)
    else:
      gLogger.verbose("Removing jobs with %s " % condDict)
      result = self.jobDB.selectJobs(condDict, limit=self.maxJobsAtOnce)

    if not result['OK']:
      return result

    jobList = result['Value']
    if len(jobList) > self.maxJobsAtOnce:
      jobList = jobList[:self.maxJobsAtOnce]
    if not jobList:
      return S_OK()

    self.log.notice("Deleting %s jobs for %s" % (len(jobList), condDict))

    count = 0
    error_count = 0
    result = SandboxStoreClient(useCertificates=True).unassignJobs(jobList)
    if not result['OK']:
      gLogger.error("Cannot unassign jobs to sandboxes", result['Message'])
      return result

    result = self.deleteJobOversizedSandbox(jobList)
    if not result['OK']:
      gLogger.error(
          "Cannot schedule removal of oversized sandboxes", result['Message'])
      return result

    failedJobs = result['Value']['Failed']
    for job in failedJobs:
      jobList.pop(jobList.index(job))

    # TODO: we should not remove a job if it still has requests in the RequestManager.
    # But this logic should go in the client or in the service, and right now no service expose jobDB.removeJobFromDB

    if self.jobByJob:
      for jobID in jobList:
        resultJobDB = self.jobDB.removeJobFromDB(jobID)
        resultTQ = self.taskQueueDB.deleteJob(jobID)
        resultLogDB = self.jobLoggingDB.deleteJob(jobID)
        errorFlag = False
        if not resultJobDB['OK']:
          gLogger.warn('Failed to remove job %d from JobDB' % jobID, result['Message'])
          errorFlag = True
        if not resultTQ['OK']:
          gLogger.warn('Failed to remove job %d from TaskQueueDB' % jobID, result['Message'])
          errorFlag = True
        if not resultLogDB['OK']:
          gLogger.warn('Failed to remove job %d from JobLoggingDB' % jobID, result['Message'])
          errorFlag = True
        if errorFlag:
          error_count += 1
        else:
          count += 1
        if self.throttlingPeriod:
          time.sleep(self.throttlingPeriod)
    else:
      result = self.jobDB.removeJobFromDB(jobList)
      if not result['OK']:
        gLogger.error('Failed to delete %d jobs from JobDB' % len(jobList))
      else:
        gLogger.info('Deleted %d jobs from JobDB' % len(jobList))

      for jobID in jobList:
        resultTQ = self.taskQueueDB.deleteJob(jobID)
        if not resultTQ['OK']:
          gLogger.warn('Failed to remove job %d from TaskQueueDB' % jobID, resultTQ['Message'])
          error_count += 1
        else:
          count += 1

      result = self.jobLoggingDB.deleteJob(jobList)
      if not result['OK']:
        gLogger.error('Failed to delete %d jobs from JobLoggingDB' % len(jobList))
      else:
        gLogger.info('Deleted %d jobs from JobLoggingDB' % len(jobList))

    if count > 0 or error_count > 0:
      gLogger.info('Deleted %d jobs from JobDB, %d errors' % (count, error_count))
    return S_OK()

  def deleteJobOversizedSandbox(self, jobIDList):
    """ Delete the job oversized sandbox files from storage elements
    """

    failed = {}
    successful = {}

    result = JobMonitoringClient().getJobParameters(jobIDList, 'OutputSandboxLFN')
    if not result['OK']:
      return result
    osLFNList = result['Value']
    if not osLFNList:
      return S_OK({'Successful': successful, 'Failed': failed})

    # Schedule removal of the LFNs now
    for jobID, outputSandboxLFNdict in osLFNList.iteritems():
      lfn = outputSandboxLFNdict['OutputSandboxLFN']
      result = self.jobDB.getJobAttributes(jobID, ['OwnerDN', 'OwnerGroup'])
      if not result['OK']:
        failed[jobID] = lfn
        continue
      if not result['Value']:
        failed[jobID] = lfn
        continue

      ownerDN = result['Value']['OwnerDN']
      ownerGroup = result['Value']['OwnerGroup']
      result = self.__setRemovalRequest(lfn, ownerDN, ownerGroup)
      if not result['OK']:
        failed[jobID] = lfn
      else:
        successful[jobID] = lfn

    result = {'Successful': successful, 'Failed': failed}
    return S_OK(result)

  def __setRemovalRequest(self, lfn, ownerDN, ownerGroup):
    """ Set removal request with the given credentials
    """
    oRequest = Request()
    oRequest.OwnerDN = ownerDN
    oRequest.OwnerGroup = ownerGroup
    oRequest.RequestName = os.path.basename(lfn).strip() + '_removal_request.xml'
    oRequest.SourceComponent = 'JobCleaningAgent'

    removeFile = Operation()
    removeFile.Type = 'RemoveFile'

    removedFile = File()
    removedFile.LFN = lfn

    removeFile.addFile(removedFile)
    oRequest.addOperation(removeFile)

    return ReqClient().putRequest(oRequest)

  def removeHeartBeatLoggingInfo(self, status, delayDays):
    """Remove HeartBeatLoggingInfo for jobs with given status after given number of days.

    :param str status: Job Status
    :param int delayDays: number of days after which information is removed
    :returns: None
    """
    gLogger.info("Removing HeartBeatLoggingInfo for Jobs with %s and older than %s day(s)" % (status, delayDays))
    delTime = str(Time.dateTime() - delayDays * Time.day)
    result = self.jobDB.removeInfoFromHeartBeatLogging(status, delTime, self.maxHBJobsAtOnce)
    if not result['OK']:
      gLogger.error('Failed to delete from HeartBeatLoggingInfo', result['Message'])
    else:
      gLogger.info('Deleted HeartBeatLogging info')
    return
