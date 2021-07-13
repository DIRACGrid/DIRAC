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

If the HeartBeatLoggingInfo table of the JobDB is too large, the information for finished jobs can be removed
(including for transformation related jobs).
In vanilla DIRAC the HeartBeatLoggingInfo is only used by the StalledJobAgent. For
this purpose the options MaxHBJobsAtOnce and RemoveStatusDelayHB/[Done|Killed|Failed] should be set to values larger
than 0.

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient import SandboxStoreClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.Client.WMSClient import WMSClient

import DIRAC.Core.Utilities.Time as Time


class JobCleaningAgent(AgentModule):
  """
  Agent for removing jobs in status "Deleted", and not only
  """

  def __init__(self, *args, **kwargs):
    """ c'tor
    """
    AgentModule.__init__(self, *args, **kwargs)

    # clients
    self.jobDB = None

    self.maxJobsAtOnce = 500
    self.prodTypes = []
    self.removeStatusDelay = {}
    self.removeStatusDelayHB = {}

  #############################################################################
  def initialize(self):
    """ Sets defaults
    """

    self.jobDB = JobDB()

    agentTSTypes = self.am_getOption('ProductionTypes', [])
    if agentTSTypes:
      self.prodTypes = agentTSTypes
    else:
      self.prodTypes = Operations().getValue(
          'Transformations/DataProcessing', ['MCSimulation', 'Merge'])
    self.log.info("Will exclude the following Production types from cleaning %s" % (
        ', '.join(self.prodTypes)))
    self.maxJobsAtOnce = self.am_getOption('MaxJobsAtOnce', self.maxJobsAtOnce)

    self.removeStatusDelay[JobStatus.DONE] = self.am_getOption('RemoveStatusDelay/Done', 7)
    self.removeStatusDelay[JobStatus.KILLED] = self.am_getOption('RemoveStatusDelay/Killed', 7)
    self.removeStatusDelay[JobStatus.FAILED] = self.am_getOption('RemoveStatusDelay/Failed', 7)
    self.removeStatusDelay['Any'] = self.am_getOption('RemoveStatusDelay/Any', -1)

    self.removeStatusDelayHB[JobStatus.DONE] = self.am_getOption('RemoveStatusDelayHB/Done', -1)
    self.removeStatusDelayHB[JobStatus.KILLED] = self.am_getOption('RemoveStatusDelayHB/Killed', -1)
    self.removeStatusDelayHB[JobStatus.FAILED] = self.am_getOption('RemoveStatusDelayHB/Failed', -1)
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

  def execute(self):
    """ Remove or delete jobs in various status
    """

    # TODO: check the WMS SM before calling the functions below (v7r3)

    # First, fully remove jobs in JobStatus.DELETED state
    result = self.removeDeletedJobs()
    if not result['OK']:
      self.log.error('Failed to remove jobs with status %s' % JobStatus.DELETED)

    # Second: set the status to JobStatus.DELETED for certain jobs

    # Get all the Job types for which we can set the status to JobStatus.DELETED
    result = self._getAllowedJobTypes()
    if not result['OK']:
      return result

    # No jobs in the system subject to deletion
    if not result['Value']:
      return S_OK()

    baseCond = {'JobType': result['Value']}
    # Delete jobs with final status
    for status in self.removeStatusDelay:
      delay = self.removeStatusDelay[status]
      if delay < 0:
        # Negative delay means don't delete anything...
        continue
      condDict = dict(baseCond)
      if status != 'Any':
        condDict['Status'] = status
      delTime = str(Time.dateTime() - delay * Time.day)
      result = self.deleteJobsByStatus(condDict, delTime)
      if not result['OK']:
        self.log.error('Failed to delete jobs', 'with condDict %s' % condDict)

    if self.maxHBJobsAtOnce > 0:
      for status, delay in self.removeStatusDelayHB.items():
        if delay > 0:
          self.removeHeartBeatLoggingInfo(status, delay)

    return S_OK()

  def removeDeletedJobs(self, delay=False):
    """ Fully remove jobs that are already in status "DELETED", unless there are still requests.

    :param int delay: days of delay
    :returns: S_OK/S_ERROR
    """

    res = self._getJobsList({'Status': JobStatus.DELETED}, delay)
    if not res['OK']:
      return res
    jobList = res['Value']
    if not jobList:
      self.log.info("No jobs to remove")
      return S_OK()

    self.log.info("Attempting to remove deleted jobs", "(%d)" % len(jobList))

    # remove from jobList those that have still Operations to do in RMS
    reqClient = ReqClient()
    res = reqClient.getRequestIDsForJobs(jobList)
    if not res['OK']:
      return res
    if res['Value']['Successful']:
      notFinal = set()
      # Check whether these requests are in a final status
      for job, reqID in res['Value']['Successful'].items():
        # If not, remove job from list to remove
        if reqClient.getRequestStatus(reqID).get('Value') not in Request.FINAL_STATES:
          # Keep that job
          notFinal.add(job)
        else:
          # Remove the request, if failed, keep the job
          res1 = reqClient.deleteRequest(reqID)
          if not res1['OK']:
            notFinal.add(job)
      if notFinal:
        self.log.info("Some jobs won't be removed, as still having Requests not in final status",
                      "(n=%d)" % len(notFinal))
        jobList = list(set(jobList) - notFinal)
    if not jobList:
      return S_OK()

    ownerJobsDict = self._getOwnerJobsDict(jobList)

    fail = False
    for owner, jobsList in ownerJobsDict.items():
      ownerDN = owner.split(';')[0]
      ownerGroup = owner.split(';')[1]
      self.log.verbose(
          "Attempting to remove jobs",
          "(n=%d) for %s : %s" % (len(jobsList), ownerDN, ownerGroup))
      wmsClient = WMSClient(useCertificates=True, delegatedDN=ownerDN, delegatedGroup=ownerGroup)
      result = wmsClient.removeJob(jobsList)
      if not result['OK']:
        self.log.error(
            "Could not remove jobs",
            "for %s : %s (n=%d) : %s" % (ownerDN, ownerGroup, len(jobsList), result['Message']))
        fail = True

    if fail:
      return S_ERROR()

    return S_OK()

  def deleteJobsByStatus(self, condDict, delay=False):
    """ Sets the job status to "DELETED" for jobs in condDict.

    :param dict condDict: a dict like {'JobType': 'User', 'Status': 'Killed'}
    :param int delay: days of delay
    :returns: S_OK/S_ERROR
    """

    res = self._getJobsList(condDict, delay)
    if not res['OK']:
      return res
    jobList = res['Value']
    if not jobList:
      return S_OK()

    self.log.notice("Attempting to delete jobs", "(%d for %s)" % (len(jobList), condDict))

    result = SandboxStoreClient(useCertificates=True).unassignJobs(jobList)
    if not result['OK']:
      self.log.error("Cannot unassign jobs to sandboxes", result['Message'])
      return result

    result = self.deleteJobOversizedSandbox(jobList)  # This might set a request
    if not result['OK']:
      self.log.error(
          "Cannot schedule removal of oversized sandboxes", result['Message'])
      return result

    failedJobs = result['Value'][JobStatus.FAILED]
    for job in failedJobs:
      jobList.pop(jobList.index(job))
    if not jobList:
      return S_OK()

    ownerJobsDict = self._getOwnerJobsDict(jobList)

    fail = False
    for owner, jobsList in ownerJobsDict.items():
      ownerDN = owner.split(';')[0]
      ownerGroup = owner.split(';')[1]
      self.log.verbose(
          "Attempting to delete jobs",
          "(n=%d) for %s : %s" % (len(jobsList), ownerDN, ownerGroup))
      wmsClient = WMSClient(useCertificates=True, delegatedDN=ownerDN, delegatedGroup=ownerGroup)
      result = wmsClient.deleteJob(jobsList)
      if not result['OK']:
        self.log.error(
            "Could not delete jobs",
            "for %s : %s (n=%d) : %s" % (ownerDN, ownerGroup, len(jobsList), result['Message']))
        fail = True

    if fail:
      return S_ERROR()

    return S_OK()

  def _getJobsList(self, condDict, delay=None):
    """ Get jobs list according to conditions

    :param dict condDict: a dict like {'JobType': 'User', 'Status': 'Killed'}
    :param int delay: days of delay
    :returns: S_OK with jobsList
    """
    jobIDsS = set()
    delayStr = "and older than %s day(s)" % delay if delay else ""
    self.log.info("Get jobs with %s %s" % (str(condDict), delayStr))
    for order in ['JobID:ASC', 'JobID:DESC']:
      result = self.jobDB.selectJobs(condDict, older=delay, orderAttribute=order, limit=self.maxJobsAtOnce)
      if not result['OK']:
        return result
      jobIDsS = jobIDsS.union({int(jID) for jID in result['Value']})

    return S_OK(list(jobIDsS))

  def _getOwnerJobsDict(self, jobList):
    """
    gets in input a list of int(JobID) and return a dict with a grouping of them by owner, e.g.
    {'dn;group': [1, 3, 4], 'dn;group_1': [5], 'dn_1;group': [2]}
    """
    res = self.jobDB.getJobsAttributes(jobList, ['OwnerDN', 'OwnerGroup'])
    if not res['OK']:
      self.log.error("Could not get the jobs attributes", res['Message'])
      return res
    jobsDictAttribs = res['Value']

    ownerJobsDict = {}
    for jobID, jobDict in jobsDictAttribs.items():
      ownerJobsDict.setdefault(';'.join(jobDict.values()), []).append(jobID)
    return ownerJobsDict

  def deleteJobOversizedSandbox(self, jobIDList):
    """
    Deletes the job oversized sandbox files from storage elements.
    Creates a request in RMS if not immediately possible.

    :param list jobIDList: list of job IDs
    :returns: S_OK/S_ERROR
    """

    failed = {}
    successful = {}

    result = JobMonitoringClient().getJobParameters(jobIDList, ['OutputSandboxLFN'])
    if not result['OK']:
      return result
    osLFNDict = result['Value']
    if not osLFNDict:
      return S_OK({'Successful': successful, 'Failed': failed})
    osLFNDict = dict(osLFN for osLFN in osLFNDict.items() if osLFN[1])

    self.log.verbose("Deleting oversized sandboxes", osLFNDict)
    # Schedule removal of the LFNs now
    for jobID, outputSandboxLFNdict in osLFNDict.items():  # can be an iterator
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
    self.log.info("Removing HeartBeatLoggingInfo for Jobs with %s and older than %s day(s)" % (status, delayDays))
    delTime = str(Time.dateTime() - delayDays * Time.day)
    result = self.jobDB.removeInfoFromHeartBeatLogging(status, delTime, self.maxHBJobsAtOnce)
    if not result['OK']:
      self.log.error('Failed to delete from HeartBeatLoggingInfo', result['Message'])
    else:
      self.log.info('Deleted HeartBeatLogging info')
    return
