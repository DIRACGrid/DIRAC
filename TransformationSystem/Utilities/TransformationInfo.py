"""TransformationInfo class to be used by ILCTransformation System"""

from collections import OrderedDict
from itertools import izip_longest

from DIRAC import gLogger, S_OK
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Core.Utilities.List import breakListIntoChunks

from DIRAC.TransformationSystem.Utilities.JobInfo import JobInfo

__RCSID__ = "$Id$"

class TransformationInfo(object):
  """ hold information about transformations """

  def __init__(self, transformationID, transName, transType, enabled,
               tClient, fcClient, jobMon):
    self.log = gLogger.getSubLogger("TInfo")
    self.enabled = enabled
    self.tID = transformationID
    self.transName = transName
    self.tClient = tClient
    self.jobMon = jobMon
    self.fcClient = fcClient
    self.transType = transType

  def checkTasksStatus(self):
    """Check the status for the task of given transformation and taskID"""

    res = self.tClient.getTransformationFiles(condDict={'TransformationID': self.tID})
    if not res['OK']:
      raise RuntimeError("Failed to get transformation tasks: %s" % res['Message'])

    tasksDict = {}
    for task in res['Value']:
      taskID = task['TaskID']
      lfn = task['LFN']
      status = task['Status']
      fileID = task['FileID']
      tasksDict[taskID] = dict(FileID=fileID, LFN=lfn, Status=status)

    return tasksDict

  def setJobDone(self, job):
    """ set the taskID to Done"""
    if not self.enabled:
      return
    self.__setTaskStatus(job, 'Done')
    if job.status != 'Done':
      self.__updateJobStatus(job.jobID, 'Done', "Job forced to Done")

  def setJobFailed(self, job):
    """ set the taskID to Done"""
    if not self.enabled:
      return
    self.__setTaskStatus(job, 'Failed')
    if job.status != 'Failed':
      self.__updateJobStatus(job.jobID, "Failed", "Job forced to Failed")

  def setInputUnused(self, job):
    """set the inputfile to unused"""
    self.__setInputStatus(job, "Unused")

  def setInputProcessed(self, job):
    """set the inputfile to processed"""
    self.__setInputStatus(job, "Processed")

  def setInputDeleted(self, job):
    """set the inputfile to processed"""
    self.__setInputStatus(job, "Deleted")

  def __setInputStatus(self, job, status):
    """set the input file to status"""
    if self.enabled:
      result = self.tClient.setFileStatusForTransformation(self.tID, status, [job.inputFile], force=True)
      if not result['OK']:
        gLogger.error("Failed updating status", result['Message'])
        raise RuntimeError("Failed updating file status")

  def __setTaskStatus(self, job, status):
    """update the task in the TransformationDB"""
    taskID = job.taskID
    res = self.tClient.setTaskStatus(self.transName, taskID, status)
    if not res['OK']:
      raise RuntimeError("Failed updating task status: %s" % res['Message'])

  def __updateJobStatus(self, jobID, status, minorstatus=None):
    """ This method updates the job status in the JobDB

    FIXME: Use the JobStateUpdate service instead of the JobDB
    """
    self.log.verbose("self.jobDB.setJobAttribute(%s,'Status','%s',update=True)" % (jobID, status))
    from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
    jobDB = JobDB()
    if self.enabled:
      result = jobDB.setJobAttribute(jobID, 'Status', status, update=True)
    else:
      return S_OK('DisabledMode')

    if not result['OK']:
      self.log.error("Failed to update job status", result['Message'])
      raise RuntimeError("Failed to update job status")

    if minorstatus is None:  # Retain last minor status for stalled jobs
      result = jobDB.getJobAttributes(jobID, ['MinorStatus'])
      if result['OK']:
        minorstatus = result['Value']['MinorStatus']
      else:
        self.log.error("Failed to get Minor Status", result['Message'])
        raise RuntimeError("Failed to get Minorstatus")
    else:
      self.log.verbose("self.jobDB.setJobAttribute(%s,'MinorStatus','%s',update=True)" % (jobID, minorstatus))
      result = jobDB.setJobAttribute(jobID, 'MinorStatus', minorstatus, update=True)

    logStatus = status
    from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB

    result = JobLoggingDB().addLoggingRecord(jobID, status=logStatus, minor=minorstatus, source='DataRecoveryAgent')
    if not result['OK']:
      ## just the logging entry, no big loss so no exception
      self.log.warn(result)

    return result

  def __findAllDescendants(self, lfnList):
    """finds all descendants of a list of LFNs"""
    allDescendants = []
    result = self.fcClient.getFileDescendents(lfnList, range(1, 8))
    if not result['OK']:
      return allDescendants
    for dummy_lfn, descendants in result['Value']['Successful'].items():
      allDescendants.extend(descendants)
    return allDescendants

  def cleanOutputs(self, jobInfo):
    """remove all job outputs"""
    if len(jobInfo.outputFiles) == 0:
      return
    descendants = self.__findAllDescendants(jobInfo.outputFiles)
    existingOutputFiles = [
        lfn for lfn,
        status in izip_longest(
            jobInfo.outputFiles,
            jobInfo.outputFileStatus) if status == "Exists"]
    filesToDelete = existingOutputFiles + descendants

    if not filesToDelete:
      return

    if not self.enabled:
      self.log.notice("Would have removed these files: \n +++ %s " % "\n +++ ".join(filesToDelete))
      return
    self.log.notice("Remove these files: \n +++ %s " % "\n +++ ".join(filesToDelete))

    errorReasons = {}
    successfullyRemoved = 0

    for lfnList in breakListIntoChunks(filesToDelete, 200):
      ## this is needed to remove the file with the Shifter credentials and not with the server credentials
      gConfigurationData.setOptionInCFG('/DIRAC/Security/UseServerCertificate', 'false')
      result = DataManager().removeFile(lfnList)
      gConfigurationData.setOptionInCFG('/DIRAC/Security/UseServerCertificate', 'true')
      if not result['OK']:
        self.log.error("Failed to remove LFNs", result['Message'])
        raise RuntimeError("Failed to remove LFNs: %s" % result['Message'])
      for lfn, err in result['Value']['Failed'].items():
        reason = str(err)
        if reason not in errorReasons.keys():
          errorReasons[reason] = []
        errorReasons[reason].append(lfn)
      successfullyRemoved += len(result['Value']['Successful'].keys())

    for reason, lfns in errorReasons.items():
      self.log.error("Failed to remove %d files with error: %s" % (len(lfns), reason))
    self.log.notice("Successfully removed %d files" % successfullyRemoved)

  def getJobs(self, statusList=None):
    """get done and failed jobs"""
    done = S_OK([])
    failed = S_OK([])
    if statusList is None:
      statusList = ['Done', 'Failed']
    if 'Done' in statusList:
      self.log.notice("Getting 'Done' Jobs...")
      done = self.__getJobs(["Done"])
    if 'Failed' in statusList:
      self.log.notice("Getting 'Failed' Jobs...")
      failed = self.__getJobs(["Failed"])
    if not done['OK']:
      raise RuntimeError("Failed to get Done Jobs")
    if not failed['OK']:
      raise RuntimeError("Failed to get Failed Jobs")
    done = done['Value']
    failed = failed['Value']

    jobsUnsorted = {}
    for job in done:
      jobsUnsorted[int(job)] = JobInfo(job, "Done", self.tID, self.transType)
    for job in failed:
      jobsUnsorted[int(job)] = JobInfo(job, "Failed", self.tID, self.transType)
    jobs = OrderedDict(sorted(jobsUnsorted.items(), key=lambda t: t[0]))

    self.log.notice("Found %d Done Jobs " % len(done))
    self.log.notice("Found %d Failed Jobs " % len(failed))
    return jobs, len(done), len(failed)

  def __getJobs(self, status):
    """returns list of done jobs"""
    attrDict = dict(Status=status, JobGroup="%08d" % int(self.tID))
    # if 'Done' in status:
    #   resAppStates = self.jobMon.getApplicationStates()
    #   if not resAppStates['OK']:
    #     raise RuntimeError( "Failed to get application states" )
    #   appStates = resAppStates['Value']
    #   appStates.remove( "Job Finished Successfully" )
    #   attrDict['ApplicationStatus'] = appStates
    res = self.jobMon.getJobs(attrDict)

    if res['OK']:
      self.log.debug("Found Prod jobs: %s" % res['Value'])
    else:
      self.log.error("Error finding jobs: ", res['Message'])
      raise RuntimeError("Failed to get jobs")
    return res
