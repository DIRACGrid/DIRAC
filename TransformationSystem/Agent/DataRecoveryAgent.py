"""
Data recovery agent: sets as unused files that are really undone.

    In general for data processing productions we need to completely abandon the 'by hand'
    reschedule operation such that accidental reschedulings don't result in data being processed twice.

    For all above cases the following procedure should be used to achieve 100%:

    - Starting from the data in the Production DB for each transformation
      look for files in the following status:
         Assigned
         MaxReset
      some of these will correspond to the final WMS status 'Failed'.

    For files in MaxReset and Assigned:
    - Discover corresponding job WMS ID
    - Check that there are no outstanding requests for the job
      o wait until all are treated before proceeding
    - Check that none of the job input data has BK descendants for the current production
      o if the data has a replica flag it means all was uploaded successfully - should be investigated by hand
      o if there is no replica flag can proceed with file removal from LFC / storage (can be disabled by flag)
    - Mark the recovered input file status as 'Unused' in the ProductionDB


New Plan:

getTransformations
getFailedJobsOfTheTransformation
- makeSureNoPendingRequests
getInputFilesForthejobs
- checkIfInputFile Assigned or MaxReset
getOutputFilesForTheJobs
- Make Sure no Descendents of the outputfiles?
- Check if _all_ or _no_ outputfiles exist
Send notification about changes

??Cache the jobs for 24hours, will depend on performance

"""

__RCSID__ = "$Id$"

from collections import defaultdict
import time
import itertools

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

from DIRAC.TransformationSystem.Utilities import TransformationInfo

AGENT_NAME = 'ILCTransformation/DataRecoveryAgent'


class DataRecoveryAgent(AgentModule):
  """Data Recovery Agent"""
  def __init__(self, *args, **kwargs):
    AgentModule.__init__(self, *args, **kwargs)
    self.name = 'DataRecoveryAgent'
    self.log = gLogger.getSubLogger("DataRecoveryAgent")
    self.enabled = False

    #only worry about files > 12hrs since last update
    self.transformationTypes = self.am_getOption("TransformationTypes",
                                                 ['MCReconstruction',
                                                  'MCSimulation',
                                                  'MCReconstruction_Overlay',
                                                  'MCGenerations'])
    self.transformationStatus = self.am_getOption("TransformationStatus", ['Active', 'Completing'])
    self.jobStatus = self.am_getOption("JobStatus", ['Failed', 'Done'])

    self.dMan = DataManager()
    self.jobMon = JobMonitoringClient()
    self.fcClient = FileCatalogClient()
    self.tClient = TransformationClient()
    self.reqClient = ReqClient()
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
                     dict(Message="One of many Successful: clean others",
                          ShortMessage="Other Tasks --> Keep",
                          Counter=0,
                          Check=lambda job: job.allFilesExist() and job.otherTasks,
                          Actions=lambda job, tInfo: [self.inputFilesProcessed.add(
                              job.inputFile), job.setJobDone(tInfo), job.setInputProcessed(tInfo)]
                          ),
                     dict(Message="Other Task processed Input, no Output: Fail",
                          ShortMessage="Other Tasks --> Fail",
                          Counter=0,
                          Check=lambda job: job.inputFile in self.inputFilesProcessed and job.allFilesMissing(),
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
                          Check=lambda job: job.allFilesExist(
                          ) and not job.otherTasks and job.status == 'Failed' and job.fileStatus != "Processed" and job.inputFileExists,
                          Actions=lambda job, tInfo: [job.setJobDone(tInfo), job.setInputProcessed(tInfo)]
                          ),
                     dict(Message="Output Exists, job Failed, input Processed --> Job Done",
                          ShortMessage="Output Exists --> Job Done",
                          Counter=0,
                          Check=lambda job: job.allFilesExist(
                          ) and not job.otherTasks and job.status == 'Failed' and job.fileStatus == "Processed" and job.inputFileExists,
                          Actions=lambda job, tInfo: [job.setJobDone(tInfo)]
                          ),
                     dict(Message="Output Exists, job Done, input not Processed --> Input Processed",
                          ShortMessage="Output Exists --> Input Processed",
                          Counter=0,
                          Check=lambda job: job.allFilesExist(
                          ) and not job.otherTasks and job.status == 'Done' and job.fileStatus != "Processed" and job.inputFileExists,
                          Actions=lambda job, tInfo: [job.setInputProcessed(tInfo)]
                          ),
                     ## outputmissing
                     dict(Message="Output Missing, job Failed, input Assigned --> Input Unused",
                          ShortMessage="Output Missing --> Input Unused",
                          Counter=0,
                          Check=lambda job: job.allFilesMissing(
                          ) and not job.otherTasks and job.status == 'Failed' and job.fileStatus == "Assigned" and job.inputFileExists,
                          Actions=lambda job, tInfo: [job.setInputUnused(tInfo)]
                          ),
                     dict(Message="Output Missing, job Done, input Assigned --> Job Failed, Input Unused",
                          ShortMessage="Output Missing --> Job Failed, Input Unused",
                          Counter=0,
                          Check=lambda job: job.allFilesMissing(
                          ) and not job.otherTasks and job.status == 'Done' and job.fileStatus == "Assigned" and job.inputFileExists,
                          Actions=lambda job, tInfo: [job.setInputUnused(tInfo), job.setJobFailed(tInfo)]
                          ),
                     # some files missing, needing cleanup. Only checking for assigned, because
                     # processed could mean an earlier job was succesful and this one is just
                     # the duplucate that needed to be removed!
                     dict(Message="Some missing, job Failed, input Assigned --> cleanup, Input 'Unused'",
                          ShortMessage="Output Missing --> Cleanup, Input Unused",
                          Counter=0,
                          Check=lambda job: job.someFilesMissing(
                          ) and not job.otherTasks and job.status == 'Failed' and job.fileStatus == "Assigned" and job.inputFileExists,
                          Actions=lambda job, tInfo: [job.cleanOutputs(tInfo), job.setInputUnused(tInfo)]
                          ),
                     dict(Message="Some missing, job Done, input Assigned --> cleanup, job Failed, Input 'Unused'",
                          ShortMessage="Output Missing --> Cleanup, Job Failed, Input Unused",
                          Counter=0,
                          Check=lambda job: job.someFilesMissing(
                          ) and not job.otherTasks and job.status == 'Done' and job.fileStatus == "Assigned" and job.inputFileExists,
                          Actions=lambda job, tInfo: [
                              job.cleanOutputs(tInfo), job.setInputUnused(tInfo), job.setJobFailed(tInfo)]
                          ),
                     dict(Message="Something Strange",
                          ShortMessage="Strange",
                          Counter=0,
                          Check=lambda job: job.status not in ("Failed", "Done"),
                          Actions=lambda job, tInfo: []
                          ),
                 ]
                 }

    ##Notification
    self.notesToSend = ""
    self.addressTo = self.am_getOption('MailTo', "andre.philippe.sailer@cern.ch")
    self.addressFrom = self.am_getOption('MailFrom', self.addressTo)
    self.subject = "DataRecoveryAgent"


    #############################################################################
  def initialize(self):
    """Sets defaults
    """
    self.enabled = self.am_getOption('EnableFlag', False)
    self.am_setModuleParam("shifterProxy", "ProductionManager")

    return S_OK()
  #############################################################################

  def execute(self):
    """ The main execution method.
    """
    transformations = self.getEligibleTransformations(self.transformationStatus, self.transformationTypes)
    if not transformations['OK']:
      self.log.error("Failure to get transformations", transformations['Message'])
      return S_ERROR("Failure to get transformations")
    for prodID, values in transformations['Value'].iteritems():
      transType, transName = values
      if transType == "MCGeneration":
        self.treatMCGeneration(int(prodID), transName, transType)
      else:
        self.treatProduction(int(prodID), transName, transType)

  def getEligibleTransformations(self, status, typeList):
    """ Select transformations of given status and type.
    """
    res = self.tClient.getTransformations(condDict={'Status': status, 'Type': typeList})
    if not res['OK']:
      return res
    transformations = {}
    for prod in res['Value']:
      prodID = prod['TransformationID']
      prodName = prod['TransformationName']
      transformations[str(prodID)] = (prod['Type'], prodName)
    return S_OK(transformations)

  def treatMCGeneration(self, prodID, transName, transType):
    """deal with MCGeneration jobs, where there is no inputFile"""
    tInfo = TransformationInfo(prodID, transName, transType, self.enabled,
                               self.tClient, self.dMan, self.fcClient, self.jobMon)
    jobs = tInfo.getJobs(statusList=self.jobStatus)
    #jobs = tInfo.getJobs(statusList=['Failed'])
    ## try until all jobs have been treated
    self.checkAllJobs(jobs, tInfo)
    self.printSummary()

  def treatProduction( self, prodID, transName, transType ):
    """run this thing for given production"""

    tInfo = TransformationInfo( prodID, transName, transType, self.enabled,
                                self.tClient, self.dMan, self.fcClient, self.jobMon )
    jobs = tInfo.getJobs(statusList=self.jobStatus)

    self.log.notice( "Getting tasks...")
    tasksDict = tInfo.checkTasksStatus()
    lfnTaskDict = dict( [ ( tasksDict[taskID]['LFN'],taskID ) for taskID in tasksDict ] )

    self.checkAllJobs( jobs, tInfo, tasksDict, lfnTaskDict )
    if self.notesToSend:
      notification = NotificationClient()
      result = notification.sendMail( self.addressTo, self.subject, self.notesToSend, self.addressFrom, localAttempt = False )
      if not result['OK']:
        self.log.error('Cannot send notification mail', result['Message'])
      self.notesToSend = ""
    self.printSummary()

  def checkJob(self, job, tInfo):
    """ deal with the job """
    checks = self.todo['MCGeneration'] if job.tType == 'MCGeneration' else self.todo['OtherProductions']
    for do in checks:
      if do['Check'](job):
        do['Counter'] += 1
        self.log.notice(do['Message'])
        self.log.notice(job)
        self.notesToSend += do['Message'] + '\n'
        self.notesToSend += str(job) + '\n'
        do['Actions'](job, tInfo)
        return

  def checkAllJobs(self, jobs, tInfo, tasksDict=None, lfnTaskDict=None):
    """run over all jobs and do checks"""
    fileJobDict = defaultdict(list)
    counter = 0
    startTime = time.time()
    nJobs = len(jobs)
    self.log.notice("Running over all the jobs")
    for job in jobs.values():
      counter += 1
      if counter % 200 == 0:
        self.log.notice("%d/%d: %3.1fs " % (counter, nJobs, float(time.time() - startTime)))
      # if job.jobID < self.firstJob:
      #   continue
      while True:
        try:
          job.checkRequests(self.reqClient)
          if job.pendingRequest:
            self.log.warn("Job has Pending requests:\n%s" % job)
            break
          job.getJobInformation(self.jobMon)
          job.checkFileExistance(self.fcClient)
          if tasksDict and lfnTaskDict:
            job.getTaskInfo(tasksDict, lfnTaskDict)
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
      self.log.notice("%s: %s" % (do['ShortMessage'].ljust(52), str(do['Counter']).rjust(5)))

# if __name__ == "__main__":
#   PARAMS = Params()
#   PARAMS.registerSwitches()
#   Script.parseCommandLine( ignoreErrors = True )
#   DRA = DRA()
#   DRA.enabled = PARAMS.enabled
#   DRA.prodID = PARAMS.prodID
#   DRA.jobStatus = PARAMS.jobStatus
#   DRA.firstJob = PARAMS.firstJob
#   DRA.execute()
