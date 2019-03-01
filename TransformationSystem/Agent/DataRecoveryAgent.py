"""An agent so ensure consistency for transformation jobs, tasks and files.

Depending on what is the status of a job and its input and outputfiles different actions are performed

- obtain list of transformation
- get a list of all 'Failed' and 'Done' jobs, make sure no job has pending requests
- get input files for all jobs, get the transformation file status associacted for the file (Unused, Assigned,
  MaxReset, Processed), check if the input file exists
- get the output files for each job, check if the output files exist
- perform changes for Jobs, files and tasks, cleanup incomplete output files to obtain consistent state for jobs,
  tasks, input and output files

  - MCGeneration: output file missing --> Job 'Failed'
  - MCGeneration: output file exists --> Job 'Done'
  - Output Missing --> File Cleanup, Job 'Failed', Input 'Unused'
  - Max ErrorCount --> Input 'MaxReset'
  - Output Exists --> Job Done, Input 'Processed'
  - Input Deleted --> Job 'Failed, File Cleanup
  - Input Missing --> Job 'Failed, Input 'Deleted', Cleanup
  - Other Task processed the File --> File Cleanup, Job Failed
  - Other Task, but this is the latest --> Keep File

- Send email about performed actions

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN DataRecoveryAgent
  :end-before: ##END
  :dedent: 2
  :caption: DataRecoveryAgent options

"""

from collections import defaultdict
import time
import itertools

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.Time import timeThis
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

from DIRAC.TransformationSystem.Utilities.TransformationInfo import TransformationInfo
from DIRAC.TransformationSystem.Utilities.JobInfo import TaskInfoException
from ILCDIRAC.Interfaces.API.DiracILC import DiracILC

__RCSID__ = "$Id$"

AGENT_NAME = 'ILCTransformation/DataRecoveryAgent'
MAXRESET = 10

ASSIGNEDSTATES = ['Assigned', 'Processed']


class DataRecoveryAgent(AgentModule):
  """Data Recovery Agent"""
  def __init__(self, *args, **kwargs):
    AgentModule.__init__(self, *args, **kwargs)
    self.name = 'DataRecoveryAgent'
    self.enabled = False

    self.productionsToIgnore = self.am_getOption("TransformationsToIgnore", [])
    self.transformationTypes = self.am_getOption("TransformationTypes",
                                                 ['MCReconstruction',
                                                  'MCSimulation',
                                                  'MCReconstruction_Overlay',
                                                  'MCGeneration'])
    self.transformationStatus = self.am_getOption("TransformationStatus", ['Active', 'Completing'])

    self.jobStatus = ['Failed', 'Done']  # This needs to be both otherwise we cannot account for all cases

    self.jobMon = JobMonitoringClient()
    self.fcClient = FileCatalogClient()
    self.tClient = TransformationClient()
    self.reqClient = ReqClient()
    self.diracILC = DiracILC()
    self.inputFilesProcessed = set()
    self.todo = {'MCGeneration':
                 [dict(Message="MCGeneration: OutputExists: Job 'Done'",
                       ShortMessage="MCGeneration: job 'Done' ",
                       Counter=0,
                       Check=lambda job: job.allFilesExist() and job.status == 'Failed',
                       Actions=lambda job, tInfo: [job.setJobDone(tInfo)]
                       ),
                  dict(Message="MCGeneration: OutputMissing: Job 'Failed'",
                       ShortMessage="MCGeneration: job 'Failed' ",
                       Counter=0,
                       Check=lambda job: job.allFilesMissing() and job.status == 'Done',
                       Actions=lambda job, tInfo: [job.setJobFailed(tInfo)]
                       ),
                  # dict( Message="MCGeneration, job 'Done': OutputExists: Task 'Done'",
                  #       ShortMessage="MCGeneration: job already 'Done' ",
                  #       Counter=0,
                  #       Check=lambda job: job.allFilesExist() and job.status=='Done',
                  #       Actions=lambda job,tInfo: [ tInfo._TransformationInfo__setTaskStatus(job, 'Done') ]
                  #     ),
                  ],
                 'OtherProductions':
                 [ \
                     ## should always be first!
                     dict(Message="One of many Successful: clean others",
                          ShortMessage="Other Tasks --> Keep",
                          Counter=0,
                          Check=lambda job: job.allFilesExist() and job.otherTasks and job.inputFile not in self.inputFilesProcessed,
                          Actions=lambda job, tInfo: [self.inputFilesProcessed.add(
                              job.inputFile), job.setJobDone(tInfo), job.setInputProcessed(tInfo)]
                          ),
                     dict(Message="Other Task processed Input, no Output: Fail",
                          ShortMessage="Other Tasks --> Fail",
                          Counter=0,
                          Check=lambda job: job.inputFile in self.inputFilesProcessed and job.allFilesMissing() and job.status != 'Failed',
                          Actions=lambda job, tInfo: [job.setJobFailed(tInfo)]
                          ),
                     dict(Message="Other Task processed Input: Fail and clean",
                          ShortMessage="Other Tasks --> Cleanup",
                          Counter=0,
                          Check=lambda job: job.inputFile in self.inputFilesProcessed and not job.allFilesMissing(),
                          Actions=lambda job, tInfo: [job.setJobFailed(tInfo), job.cleanOutputs(tInfo)]
                          ),
                     dict(Message="InputFile missing: mark job 'Failed', mark input 'Deleted', clean",
                          ShortMessage="Input Missing --> Job 'Failed, Input 'Deleted', Cleanup",
                          Counter=0,
                          Check=lambda job: job.inputFile and not job.inputFileExists and job.fileStatus != "Deleted",
                          Actions=lambda job, tInfo: [
                              job.cleanOutputs(tInfo), job.setJobFailed(tInfo), job.setInputDeleted(tInfo)]
                          ),
                     dict(Message="InputFile Deleted, output Exists: mark job 'Failed', clean",
                          ShortMessage="Input Deleted --> Job 'Failed, Cleanup",
                          Counter=0,
                          Check=lambda job: job.inputFile and not job.inputFileExists and job.fileStatus == "Deleted" and not job.allFilesMissing(),
                          Actions=lambda job, tInfo: [job.cleanOutputs(tInfo), job.setJobFailed(tInfo)]
                          ),
                     ## All Output Exists
                     dict(Message="Output Exists, job Failed, input not Processed --> Job Done, Input Processed",
                          ShortMessage="Output Exists --> Job Done, Input Processed",
                          Counter=0,
                          Check=lambda job: job.allFilesExist() and \
                          not job.otherTasks and \
                          job.status == 'Failed' and \
                          job.fileStatus != "Processed" and \
                          job.inputFileExists,
                          Actions=lambda job, tInfo: [job.setJobDone(tInfo), job.setInputProcessed(tInfo)]
                          ),
                     dict(Message="Output Exists, job Failed, input Processed --> Job Done",
                          ShortMessage="Output Exists --> Job Done",
                          Counter=0,
                          Check=lambda job: job.allFilesExist() and \
                          not job.otherTasks and \
                          job.status == 'Failed' and \
                          job.fileStatus == "Processed" and \
                          job.inputFileExists,
                          Actions=lambda job, tInfo: [job.setJobDone(tInfo)]
                          ),
                     dict(Message="Output Exists, job Done, input not Processed --> Input Processed",
                          ShortMessage="Output Exists --> Input Processed",
                          Counter=0,
                          Check=lambda job: job.allFilesExist() and \
                          not job.otherTasks and \
                          job.status == 'Done' and \
                          job.fileStatus != "Processed" and \
                          job.inputFileExists,
                          Actions=lambda job, tInfo: [job.setInputProcessed(tInfo)]
                          ),
                     ## outputmissing
                     dict(Message="Output Missing, job Failed, input Assigned, MaxError --> Input MaxReset",
                          ShortMessage="Max ErrorCount --> Input MaxReset",
                          Counter=0,
                          Check=lambda job: job.allFilesMissing() and \
                          not job.otherTasks and \
                          job.status == 'Failed' and \
                          job.fileStatus in ASSIGNEDSTATES and \
                          job.inputFileExists and \
                          job.errorCount > MAXRESET,
                          Actions=lambda job, tInfo: [job.setInputMaxReset(tInfo)]
                          ),
                     dict(Message="Output Missing, job Failed, input Assigned --> Input Unused",
                          ShortMessage="Output Missing --> Input Unused",
                          Counter=0,
                          Check=lambda job: job.allFilesMissing() and \
                          not job.otherTasks and \
                          job.status == 'Failed' and \
                          job.fileStatus in ASSIGNEDSTATES and \
                          job.inputFileExists,
                          Actions=lambda job, tInfo: [job.setInputUnused(tInfo)]
                          ),
                     dict(Message="Output Missing, job Done, input Assigned --> Job Failed, Input Unused",
                          ShortMessage="Output Missing --> Job Failed, Input Unused",
                          Counter=0,
                          Check=lambda job: job.allFilesMissing() and \
                          not job.otherTasks and \
                          job.status == 'Done' and \
                          job.fileStatus in ASSIGNEDSTATES and \
                          job.inputFileExists,
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
                          job.status == 'Failed' and \
                          job.fileStatus in ASSIGNEDSTATES and \
                          job.inputFileExists,
                          Actions=lambda job, tInfo: [job.cleanOutputs(tInfo), job.setInputUnused(tInfo)]
                          ),
                     dict(Message="Some missing, job Done, input Assigned --> cleanup, job Failed, Input 'Unused'",
                          ShortMessage="Output Missing --> Cleanup, Job Failed, Input Unused",
                          Counter=0,
                          Check=lambda job: job.someFilesMissing() and \
                          not job.otherTasks and \
                          job.status == 'Done' and \
                          job.fileStatus in ASSIGNEDSTATES and \
                          job.inputFileExists,
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
                          Check=lambda job: job.status not in ("Failed", "Done"),
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
    self.printEveryNJobs = self.am_getOption('PrintEvery', 200)
    ##Notification
    self.notesToSend = ""
    self.addressTo = self.am_getOption('MailTo', ["ilcdirac-admin@cern.ch"])
    self.addressFrom = self.am_getOption('MailFrom', "ilcdirac-admin@cern.ch")
    self.subject = "DataRecoveryAgent"
    self.startTime = time.time()

    #############################################################################

  def beginExecution(self):
    """Resets defaults after one cycle
    """
    self.enabled = self.am_getOption('EnableFlag', False)
    self.productionsToIgnore = self.am_getOption("TransformationsToIgnore", [])
    self.transformationTypes = self.am_getOption("TransformationTypes",
                                                 ['MCReconstruction',
                                                  'MCSimulation',
                                                  'MCReconstruction_Overlay',
                                                  'MCGeneration'])
    self.transformationStatus = self.am_getOption("TransformationStatus", ['Active', 'Completing'])
    self.addressTo = self.am_getOption('MailTo', self.addressTo)
    self.addressFrom = self.am_getOption('MailFrom', "ilcdirac-admin@cern.ch")
    self.printEveryNJobs = self.am_getOption('PrintEvery', 200)

    return S_OK()
  #############################################################################

  def execute(self):
    """ The main execution method.
    """
    self.log.notice("Will ignore the following productions: %s" % self.productionsToIgnore)
    self.log.notice(" Job Cache: %s " % self.jobCache)
    transformations = self.getEligibleTransformations(self.transformationStatus, self.transformationTypes)
    if not transformations['OK']:
      self.log.error("Failure to get transformations", transformations['Message'])
      return S_ERROR("Failure to get transformations")
    for prodID, transInfoDict in transformations['Value'].iteritems():
      if prodID in self.productionsToIgnore:
        self.log.notice("Ignoring Production: %s " % prodID)
        continue
      self.__resetCounters()
      self.inputFilesProcessed = set()
      self.log.notice("Running over Production: %s " % prodID)
      self.treatProduction(int(prodID), transInfoDict)

      if self.notesToSend and self.__notOnlyKeepers(transInfoDict['Type']):
        # remove from the jobCache because something happened
        self.jobCache.pop(int(prodID), None)
        notification = NotificationClient()
        for address in self.addressTo:
          result = notification.sendMail(address, "%s: %s" %
                                         (self.subject, prodID), self.notesToSend, self.addressFrom, localAttempt=False)
          if not result['OK']:
            self.log.error('Cannot send notification mail', result['Message'])
      self.notesToSend = ""

    return S_OK()

  def getEligibleTransformations(self, status, typeList):
    """ Select transformations of given status and type.
    """
    res = self.tClient.getTransformations(condDict={'Status': status, 'Type': typeList})
    if not res['OK']:
      return res
    transformations = {}
    for prod in res['Value']:
      prodID = prod['TransformationID']
      transformations[str(prodID)] = prod
    return S_OK(transformations)

  def treatProduction(self, prodID, transInfoDict):
    """Run this thing for given production."""
    tInfo = TransformationInfo(prodID, transInfoDict, self.enabled,
                               self.tClient, self.fcClient, self.jobMon)
    jobs, nDone, nFailed = tInfo.getJobs(statusList=self.jobStatus)

    if self.jobCache[prodID][0] == nDone and self.jobCache[prodID][1] == nFailed:
      self.log.notice("Skipping production %s because nothing changed" % prodID)
      return

    self.jobCache[prodID] = (nDone, nFailed)

    tasksDict = None
    lfnTaskDict = None

    self.startTime = time.time()
    if not transInfoDict['Type'].startswith("MCGeneration"):
      self.log.notice('Getting tasks...')
      tasksDict = tInfo.checkTasksStatus()
      lfnTaskDict = dict([(tasksDict[taskID]['LFN'], taskID) for taskID in tasksDict])

    self.checkAllJobs(jobs, tInfo, tasksDict, lfnTaskDict)
    self.printSummary()

  def checkJob(self, job, tInfo):
    """ deal with the job """
    checks = self.todo['MCGeneration'] if job.tType.startswith('MCGeneration') else self.todo['OtherProductions']
    for do in checks:
      if do['Check'](job):
        do['Counter'] += 1
        self.log.notice(do['Message'])
        self.log.notice(job)
        self.notesToSend += do['Message'] + '\n'
        self.notesToSend += str(job) + '\n'
        do['Actions'](job, tInfo)
        return

  @timeThis
  def getLFNStatus(self, jobs):
    """Get all the LFNs for the jobs and get their status."""
    self.log.notice('Collecting LFNs...')
    lfnExistence = {}
    lfnCache = []
    for counter, job in enumerate(jobs.values()):
      if counter % self.printEveryNJobs == 0:
        self.log.notice('Getting JobInfo: %d/%d: %3.1fs' % (counter, len(jobs), float(time.time() - self.startTime)))
      while True:
        try:
          job.getJobInformation(self.diracILC)
          if job.inputFile:
            lfnCache.append(job.inputFile)
          if job.outputFiles:
            lfnCache.extend(job.outputFiles)
          break
        except RuntimeError as e:  # try again
          self.log.error('+++++ Failure for job:', job.jobID)
          self.log.error('+++++ Exception: ', str(e))

    counter = 0
    for lfnChunk in breakListIntoChunks(list(lfnCache), 200):
      counter += 200
      if counter % 1000 == 0:
        self.log.notice('Getting FileInfo: %d/%d: %3.1fs' % (counter, len(jobs), float(time.time() - self.startTime)))
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

    return lfnExistence

  @timeThis
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
    for counter, job in enumerate(jobs.values()):
      if counter % self.printEveryNJobs == 0:
        self.log.notice('%d/%d: %3.1fs' % (counter, nJobs, float(time.time() - self.startTime)))
      while True:
        try:
          if job.pendingRequest:
            self.log.warn('Job has Pending requests:\n%s' % job)
            break
          job.checkFileExistence(lfnExistence)
          if tasksDict and lfnTaskDict:
            try:
              job.getTaskInfo(tasksDict, lfnTaskDict)
            except TaskInfoException as e:
              self.log.error(" Skip Task, due to TaskInfoException: %s" % e)
              if job.inputFile is None and not job.tType.startswith("MCGeneration"):
                self.__failJobHard(job, tInfo)
              break
            fileJobDict[job.inputFile].append(job.jobID)
          self.checkJob(job, tInfo)
          break  # get out of the while loop
        except RuntimeError as e:
          self.log.error("+++++ Failure for job: %d " % job.jobID)
          self.log.error("+++++ Exception: ", str(e))
          # runs these again because of RuntimeError
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
    for _name, checks in self.todo.iteritems():
      for do in checks:
        do['Counter'] = 0

  def __failJobHard(self, job, tInfo):
    """ set job to failed and remove output files if there are any """
    if job.inputFile is not None:
      return
    if job.status in ("Failed",) \
       and job.allFilesMissing():
      return
    self.log.notice("Failing job hard %s" % job)
    self.notesToSend += "Failing job %s: no input file?\n" % job.jobID
    self.notesToSend += str(job) + '\n'
    self.todo['OtherProductions'][-1]['Counter'] += 1
    job.cleanOutputs(tInfo)
    job.setJobFailed(tInfo)
    # if job.inputFile is not None:
    #   job.setInputDeleted(tInfo)

  def __notOnlyKeepers(self, transType):
    """check of we only have 'Keep' messages

    in this case we do not have to send report email or run again next time

    """
    if transType.startswith('MCGeneration'):
      return True

    checks = self.todo['OtherProductions']
    totalCount = 0
    for check in checks[1:]:
      totalCount += check['Counter']

    return totalCount > 0
