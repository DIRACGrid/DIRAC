"""An agent to ensure consistency for transformation jobs, tasks and files.

Depending on what is the status of a job and its input and output files different actions are performed.

.. warning:: Before fully enabling this agent make sure that your transformation jobs fulfill the assumptions of the
  agent.  Otherwise it might delete some of your data! Do not set ``EnableFlag`` to True before letting the agent run
  through a few times and read the messages it produces.

.. note::  Use the :ref:`admin_dirac-transformation-recover-data` script for checking individual transformations

The agent takes the following steps

- obtain list of transformation
- get a list of all 'Failed' and 'Done' jobs, jobs with pending requests are ignored.
- get input files for all jobs, get the transformation file status
  associated for the file (Unused, Assigned, MaxReset, Processed),
  check if the input file exists
- get the output files for each job, check if the output files exist
- perform changes for Jobs, Files and Tasks: cleanup incomplete output
  files to obtain consistent state for jobs, tasks, input and output
  files

- Send email about performed actions

Requirements/Assumptions:

- JobParameters:

  - ProductionOutputData: with the semi-colon separated list of expected output files, stored as a Job Parameter
      This parameter needs to be set by the production UploadOutputData tool _before_ uploading files
  - JobName of the form: TransformationID_TaskID obtained as a  JobAttribute
  - InputData from the JobMonitor.getInputData

  - Or Extract that information from the JDL for the job, which must also contain the ProductionOutputData fields

- JobGroup equal to "%08d" % transformationID

.. note::

  Transformations are only treated, if during the last pass changes
  were performed, or the number of Failed and Done jobs has changed.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN DataRecoveryAgent
  :end-before: ##END
  :dedent: 2
  :caption: DataRecoveryAgent options

.. note::

  For the ``TransformationsNoInput`` or ``TransformationsWithInput``
  to take their default value, the options need to be
  removed from the configuration, otherwise no transformations of this type will be treated.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict
import time
import itertools

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.TransformationSystem.Utilities.JobInfo import TaskInfoException
from DIRAC.TransformationSystem.Utilities.TransformationInfo import TransformationInfo
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient

__RCSID__ = "$Id$"

AGENT_NAME = 'Transformation/DataRecoveryAgent'

ASSIGNEDSTATES = ['Assigned', 'Processed']


class DataRecoveryAgent(AgentModule):
  """Data Recovery Agent"""

  def __init__(self, *args, **kwargs):
    AgentModule.__init__(self, *args, **kwargs)
    self.name = 'DataRecoveryAgent'
    self.enabled = False
    self.getJobInfoFromJDLOnly = False

    self.__getCSOptions()

    # This needs to be both otherwise we cannot account for all cases
    self.jobStatus = [JobStatus.DONE,
                      JobStatus.FAILED]

    self.jobMon = JobMonitoringClient()
    self.fcClient = FileCatalogClient()
    self.tClient = TransformationClient()
    self.reqClient = ReqClient()
    self.diracAPI = Dirac()
    self.inputFilesProcessed = set()
    self.todo = {'NoInputFiles':
                 [dict(Message="NoInputFiles: OutputExists: Job 'Done'",
                       ShortMessage="NoInputFiles: job 'Done' ",
                       Counter=0,
                       Check=lambda job: job.allFilesExist() and job.status == JobStatus.FAILED,
                       Actions=lambda job, tInfo: [job.setJobDone(tInfo)],
                       ),
                  dict(Message="NoInputFiles: OutputMissing: Job 'Failed'",
                       ShortMessage="NoInputFiles: job 'Failed' ",
                       Counter=0,
                       Check=lambda job: job.allFilesMissing() and job.status == JobStatus.DONE,
                       Actions=lambda job, tInfo: [job.setJobFailed(tInfo)],
                       ),
                  ],
                 'InputFiles':
                 [ \
                     # must always be first!
                     dict(Message="One of many Successful: clean others",
                          ShortMessage="Other Tasks --> Keep",
                          Counter=0,
                          Check=lambda job: job.allFilesExist() and job.otherTasks and \
                          not set(job.inputFiles).issubset(self.inputFilesProcessed),
                          Actions=lambda job, tInfo: [self.inputFilesProcessed.update(job.inputFiles),
                                                      job.setJobDone(tInfo),
                                                      job.setInputProcessed(tInfo)]
                          ),
                     dict(Message="Other Task processed Input, no Output: Fail",
                          ShortMessage="Other Tasks --> Fail",
                          Counter=0,
                          Check=lambda job: set(job.inputFiles).issubset(self.inputFilesProcessed) and \
                          job.allFilesMissing() and job.status != JobStatus.FAILED,
                          Actions=lambda job, tInfo: [job.setJobFailed(tInfo)]
                          ),
                     dict(Message="Other Task processed Input: Fail and clean",
                          ShortMessage="Other Tasks --> Cleanup",
                          Counter=0,
                          Check=lambda job: set(job.inputFiles).issubset(
                              self.inputFilesProcessed) and not job.allFilesMissing(),
                          Actions=lambda job, tInfo: [job.setJobFailed(tInfo), job.cleanOutputs(tInfo)]
                          ),
                     dict(Message="InputFile(s) missing: mark job 'Failed', mark input 'Deleted', clean",
                          ShortMessage="Input Missing --> Job 'Failed, Input 'Deleted', Cleanup",
                          Counter=0,
                          Check=lambda job: job.inputFiles and job.allInputFilesMissing() and \
                          not job.allTransFilesDeleted(),
                          Actions=lambda job, tInfo: [job.cleanOutputs(tInfo), job.setJobFailed(tInfo),
                                                      job.setInputDeleted(tInfo)],
                          ),
                     dict(Message="InputFile(s) Deleted, output Exists: mark job 'Failed', clean",
                          ShortMessage="Input Deleted --> Job 'Failed, Cleanup",
                          Counter=0,
                          Check=lambda job: job.inputFiles and job.allInputFilesMissing() and \
                          job.allTransFilesDeleted() and not job.allFilesMissing(),
                          Actions=lambda job, tInfo: [job.cleanOutputs(tInfo), job.setJobFailed(tInfo)],
                          ),
                     # All Output Exists
                     dict(Message="Output Exists, job Failed, input not Processed --> Job Done, Input Processed",
                          ShortMessage="Output Exists --> Job Done, Input Processed",
                          Counter=0,
                          Check=lambda job: job.allFilesExist() and \
                          not job.otherTasks and \
                          job.status == JobStatus.FAILED and \
                          not job.allFilesProcessed() and \
                          job.allInputFilesExist(),
                          Actions=lambda job, tInfo: [job.setJobDone(tInfo), job.setInputProcessed(tInfo)]
                          ),
                     dict(Message="Output Exists, job Failed, input Processed --> Job Done",
                          ShortMessage="Output Exists --> Job Done",
                          Counter=0,
                          Check=lambda job: job.allFilesExist() and \
                          not job.otherTasks and \
                          job.status == JobStatus.FAILED and \
                          job.allFilesProcessed() and \
                          job.allInputFilesExist(),
                          Actions=lambda job, tInfo: [job.setJobDone(tInfo)]
                          ),
                     dict(Message="Output Exists, job Done, input not Processed --> Input Processed",
                          ShortMessage="Output Exists --> Input Processed",
                          Counter=0,
                          Check=lambda job: job.allFilesExist() and \
                          not job.otherTasks and \
                          job.status == JobStatus.DONE and \
                          not job.allFilesProcessed() and \
                          job.allInputFilesExist(),
                          Actions=lambda job, tInfo: [job.setInputProcessed(tInfo)]
                          ),
                     # outputmissing
                     dict(Message="Output Missing, job Failed, input Assigned, MaxError --> Input MaxReset",
                          ShortMessage="Max ErrorCount --> Input MaxReset",
                          Counter=0,
                          Check=lambda job: job.allFilesMissing() and \
                          not job.otherTasks and \
                          job.status == JobStatus.FAILED and \
                          job.allFilesAssigned() and \
                          not set(job.inputFiles).issubset(self.inputFilesProcessed) and \
                          job.allInputFilesExist() and \
                          job.checkErrorCount(),
                          Actions=lambda job, tInfo: [job.setInputMaxReset(tInfo)]
                          ),
                     dict(Message="Output Missing, job Failed, input Assigned --> Input Unused",
                          ShortMessage="Output Missing --> Input Unused",
                          Counter=0,
                          Check=lambda job: job.allFilesMissing() and \
                          not job.otherTasks and \
                          job.status == JobStatus.FAILED and \
                          job.allFilesAssigned() and \
                          not set(job.inputFiles).issubset(self.inputFilesProcessed) and \
                          job.allInputFilesExist(),
                          Actions=lambda job, tInfo: [job.setInputUnused(tInfo)]
                          ),
                     dict(Message="Output Missing, job Done, input Assigned --> Job Failed, Input Unused",
                          ShortMessage="Output Missing --> Job Failed, Input Unused",
                          Counter=0,
                          Check=lambda job: job.allFilesMissing() and \
                          not job.otherTasks and \
                          job.status == JobStatus.DONE and \
                          job.allFilesAssigned() and \
                          not set(job.inputFiles).issubset(self.inputFilesProcessed) and \
                          job.allInputFilesExist(),
                          Actions=lambda job, tInfo: [job.setInputUnused(tInfo), job.setJobFailed(tInfo)]
                          ),
                     # some files missing, needing cleanup. Only checking for
                     # assigned, because processed could mean an earlier job was
                     # succesful and this one is just the duplicate that needed
                     # to be removed! But we check for other tasks earlier, so
                     # this should not happen
                     dict(Message="Some missing, job Failed, input Assigned --> cleanup, Input 'Unused'",
                          ShortMessage="Output Missing --> Cleanup, Input Unused",
                          Counter=0,
                          Check=lambda job: job.someFilesMissing() and \
                          not job.otherTasks and \
                          job.status == JobStatus.FAILED and \
                          job.allFilesAssigned() and \
                          job.allInputFilesExist(),
                          Actions=lambda job, tInfo: [job.cleanOutputs(tInfo), job.setInputUnused(tInfo)]
                          ),
                     dict(Message="Some missing, job Done, input Assigned --> cleanup, job Failed, Input 'Unused'",
                          ShortMessage="Output Missing --> Cleanup, Job Failed, Input Unused",
                          Counter=0,
                          Check=lambda job: job.someFilesMissing() and \
                          not job.otherTasks and \
                          job.status == JobStatus.DONE and \
                          job.allFilesAssigned() and \
                          job.allInputFilesExist(),
                          Actions=lambda job, tInfo: [
                              job.cleanOutputs(tInfo), job.setInputUnused(tInfo), job.setJobFailed(tInfo)]
                          ),
                     dict(Message="Some missing, job Done --> job Failed",
                          ShortMessage="Output Missing, Done --> Job Failed",
                          Counter=0,
                          Check=lambda job: not job.allFilesExist() and job.status == 'Done',
                          Actions=lambda job, tInfo: [job.setJobFailed(tInfo)]
                          ),
                     dict(Message="Something Strange",
                          ShortMessage="Strange",
                          Counter=0,
                          Check=lambda job: job.status not in (JobStatus.FAILED, JobStatus.DONE),
                          Actions=lambda job, tInfo: []
                          ),
                     # should always be the last one!
                     dict(Message="Failed Hard",
                          ShortMessage="Failed Hard",
                          Counter=0,
                          Check=lambda job: False,  # never
                          Actions=lambda job, tInfo: []
                          ),
                 ]
                 }
    self.jobCache = defaultdict(lambda: (0, 0))
    # Notification options
    self.notesToSend = ""
    self.subject = "DataRecoveryAgent"
    self.startTime = time.time()

    #############################################################################

  def beginExecution(self):
    """Resets defaults after one cycle."""
    self.__getCSOptions()
    return S_OK()

  def __getCSOptions(self):
    """Get agent options from the CS."""
    self.enabled = self.am_getOption('EnableFlag', False)
    self.transformationsToIgnore = self.am_getOption('TransformationsToIgnore', [])
    self.getJobInfoFromJDLOnly = self.am_getOption('JobInfoFromJDLOnly', False)
    self.transformationStatus = self.am_getOption('TransformationStatus', ['Active', 'Completing'])
    ops = Operations()
    extendableTTypes = set(ops.getValue('Transformations/ExtendableTransfTypes', ['MCSimulation']))
    dataProcessing = set(ops.getValue('Transformations/DataProcessing', []))
    self.transNoInput = self.am_getOption('TransformationsNoInput', list(extendableTTypes))
    self.transWithInput = self.am_getOption('TransformationsWithInput', list(dataProcessing - extendableTTypes))
    self.transformationTypes = self.transWithInput + self.transNoInput
    self.log.notice('Will treat transformations without input files', self.transNoInput)
    self.log.notice('Will treat transformations with input files', self.transWithInput)
    self.addressTo = self.am_getOption('MailTo', [])
    self.addressFrom = self.am_getOption('MailFrom', '')
    self.printEveryNJobs = self.am_getOption('PrintEvery', 200)

  def execute(self):
    """ The main execution method.
    """
    self.log.notice("Will ignore the following transformations: %s" % self.transformationsToIgnore)
    self.log.notice(" Job Cache: %s " % self.jobCache)
    transformations = self.getEligibleTransformations(self.transformationStatus, self.transformationTypes)
    if not transformations['OK']:
      self.log.error("Failure to get transformations", transformations['Message'])
      return S_ERROR("Failure to get transformations")
    for transID, transInfoDict in transformations['Value'].items():
      if transID in self.transformationsToIgnore:
        self.log.notice('Ignoring Transformation: %s' % transID)
        continue
      self.__resetCounters()
      self.inputFilesProcessed = set()
      self.log.notice('Running over Transformation: %s' % transID)
      self.treatTransformation(int(transID), transInfoDict)
      self.sendNotification(transID, transInfoDict)

    return S_OK()

  def getEligibleTransformations(self, status, typeList):
    """ Select transformations of given status and type.
    """
    res = self.tClient.getTransformations(condDict={'Status': status, 'Type': typeList})
    if not res['OK']:
      return res
    transformations = {}
    for prod in res['Value']:
      transID = prod['TransformationID']
      transformations[str(transID)] = prod
    return S_OK(transformations)

  def treatTransformation(self, transID, transInfoDict):
    """Run this thing for given transformation."""
    tInfo = TransformationInfo(transID, transInfoDict, self.enabled,
                               self.tClient, self.fcClient, self.jobMon)
    jobs, nDone, nFailed = tInfo.getJobs(statusList=self.jobStatus)

    if not jobs:
      self.log.notice('Skipping. No jobs for transformation', str(transID))
      return

    if self.jobCache[transID][0] == nDone and self.jobCache[transID][1] == nFailed:
      self.log.notice('Skipping transformation %s because nothing changed' % transID)
      return

    self.jobCache[transID] = (nDone, nFailed)

    tasksDict = None
    lfnTaskDict = None

    self.startTime = time.time()
    if transInfoDict['Type'] in self.transWithInput:
      self.log.notice('Getting tasks...')
      tasksDict = tInfo.checkTasksStatus()
      lfnTaskDict = dict([(taskDict['LFN'], taskID)
                          for taskID, taskDicts in tasksDict.items()
                          for taskDict in taskDicts
                          ])

    self.checkAllJobs(jobs, tInfo, tasksDict, lfnTaskDict)
    self.printSummary()

  def checkJob(self, job, tInfo):
    """Deal with the job."""
    checks = self.todo['NoInputFiles'] if job.tType in self.transNoInput else self.todo['InputFiles']
    for do in checks:
      self.log.verbose('Testing: ', do['Message'])
      if do['Check'](job):
        do['Counter'] += 1
        self.log.notice(do['Message'])
        self.log.notice(job)
        self.notesToSend += do['Message'] + '\n'
        self.notesToSend += str(job) + '\n'
        do['Actions'](job, tInfo)
        return

  def getLFNStatus(self, jobs):
    """Get all the LFNs for the jobs and get their status."""
    self.log.notice('Collecting LFNs...')
    lfnExistence = {}
    lfnCache = []
    counter = 0
    jobInfoStart = time.time()
    for counter, job in enumerate(jobs.values()):
      if counter % self.printEveryNJobs == 0:
        self.log.notice('Getting JobInfo: %d/%d: %3.1fs' %
                        (counter, len(jobs), float(time.time() - jobInfoStart)))
      while True:
        try:
          job.getJobInformation(self.diracAPI, self.jobMon, jdlOnly=self.getJobInfoFromJDLOnly)
          lfnCache.extend(job.inputFiles)
          lfnCache.extend(job.outputFiles)
          break
        except RuntimeError as e:  # try again
          self.log.error('+++++ Failure for job:', job.jobID)
          self.log.error('+++++ Exception: ', str(e))

    timeSpent = float(time.time() - jobInfoStart)
    self.log.notice('Getting JobInfo Done: %3.1fs (%3.3fs per job)' % (timeSpent, timeSpent / counter))

    counter = 0
    fileInfoStart = time.time()
    for lfnChunk in breakListIntoChunks(list(lfnCache), 200):
      counter += 200
      if counter % 1000 == 0:
        self.log.notice('Getting FileInfo: %d/%d: %3.1fs' %
                        (counter, len(lfnCache), float(time.time() - fileInfoStart)))
      while True:
        try:
          reps = self.fcClient.exists(lfnChunk)
          if not reps['OK']:
            self.log.error('Failed to check file existence, try again...', reps['Message'])
            raise RuntimeError('Try again')
          statuses = reps['Value']
          lfnExistence.update(statuses['Successful'])
          break
        except RuntimeError:  # try again
          pass
    self.log.notice('Getting FileInfo Done: %3.1fs' % (float(time.time() - fileInfoStart)))

    return lfnExistence

  def setPendingRequests(self, jobs):
    """Loop over all the jobs and get requests, if any."""
    for jobChunk in breakListIntoChunks(jobs.values(), 1000):
      jobIDs = [job.jobID for job in jobChunk]
      while True:
        result = self.reqClient.readRequestsForJobs(jobIDs)
        if result['OK']:
          break
        self.log.error('Failed to read requests', result['Message'])
        # repeat
      for jobID in result['Value']['Successful']:
        request = result['Value']['Successful'][jobID]
        requestID = request.RequestID
        dbStatus = self.reqClient.getRequestStatus(requestID).get('Value', 'Unknown')
        for job in jobChunk:
          if job.jobID == jobID:
            job.pendingRequest = dbStatus not in ('Done', 'Canceled')
            self.log.notice('Found %s request for job %d' % ('pending' if job.pendingRequest else 'finished', jobID))
            break

  def checkAllJobs(self, jobs, tInfo, tasksDict=None, lfnTaskDict=None):
    """run over all jobs and do checks"""
    fileJobDict = defaultdict(list)
    counter = 0
    nJobs = len(jobs)
    self.setPendingRequests(jobs)
    lfnExistence = self.getLFNStatus(jobs)
    self.log.notice('Running over all the jobs')
    jobCheckStart = time.time()
    for counter, job in enumerate(jobs.values()):
      if counter % self.printEveryNJobs == 0:
        self.log.notice('Checking Jobs %d/%d: %3.1fs' % (counter, nJobs, float(time.time() - jobCheckStart)))
      while True:
        try:
          if job.pendingRequest:
            self.log.warn('Job has Pending requests:\n%s' % job)
            break
          job.checkFileExistence(lfnExistence)
          if tasksDict and lfnTaskDict:
            try:
              job.getTaskInfo(tasksDict, lfnTaskDict, self.transWithInput)
            except TaskInfoException as e:
              self.log.error(" Skip Task, due to TaskInfoException: %s" % e)
              if not job.inputFiles and job.tType in self.transWithInput:
                self.__failJobHard(job, tInfo)
              break
            for inputFile in job.inputFiles:
              fileJobDict[inputFile].append(job.jobID)
          self.checkJob(job, tInfo)
          break  # get out of the while loop
        except RuntimeError as e:
          self.log.error("+++++ Failure for job: %d " % job.jobID)
          self.log.error("+++++ Exception: ", str(e))
          # run these again because of RuntimeError
    self.log.notice('Checking Jobs Done: %d/%d: %3.1fs' % (counter, nJobs, float(time.time() - jobCheckStart)))

  def printSummary(self):
    """print summary of changes"""
    self.log.notice("Summary:")
    for do in itertools.chain.from_iterable(self.todo.values()):
      message = "%s: %s" % (do['ShortMessage'].ljust(56), str(do['Counter']).rjust(5))
      self.log.notice(message)
      if self.notesToSend:
        self.notesToSend = str(message) + '\n' + self.notesToSend

  def __resetCounters(self):
    """ reset counters for modified jobs """
    for _name, checks in self.todo.items():
      for do in checks:
        do['Counter'] = 0

  def __failJobHard(self, job, tInfo):
    """ set job to failed and remove output files if there are any """
    if job.inputFiles:
      return
    if job.status in ("Failed",) \
       and job.allFilesMissing():
      return
    self.log.notice("Failing job hard %s" % job)
    self.notesToSend += "Failing job %s: no input file?\n" % job.jobID
    self.notesToSend += str(job) + '\n'
    self.todo['InputFiles'][-1]['Counter'] += 1
    job.cleanOutputs(tInfo)
    job.setJobFailed(tInfo)
    # if job.inputFile is not None:
    #   job.setInputDeleted(tInfo)

  def __notOnlyKeepers(self, transType):
    """check of we only have 'Keep' messages

    in this case we do not have to send report email or run again next time

    """
    if transType in self.transNoInput:
      return True

    checks = self.todo['InputFiles']
    totalCount = 0
    for check in checks[1:]:
      totalCount += check['Counter']

    return totalCount > 0

  def sendNotification(self, transID, transInfoDict):
    """Send notification email if something was modified for a transformation.

    :param int transID: ID of given transformation
    :param transInfoDict:
    """
    if not self.addressTo or not self.addressFrom or not self.notesToSend:
      return
    if not self.__notOnlyKeepers(transInfoDict['Type']):
      # purge notes
      self.notesToSend = ""
      return

    # remove from the jobCache because something happened
    self.jobCache.pop(int(transID), None)
    # send the email to recipients
    for address in self.addressTo:
      result = NotificationClient().sendMail(address, "%s: %s" %
                                             (self.subject, transID),
                                             self.notesToSend,
                                             self.addressFrom,
                                             localAttempt=False)
      if not result['OK']:
        self.log.error('Cannot send notification mail', result['Message'])
    # purge notes
    self.notesToSend = ""
